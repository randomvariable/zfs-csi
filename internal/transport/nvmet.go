// Copyright 2026 Naadir Jeewa
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package transport

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	defaultNVMETBase    = "/sys/kernel/config/nvmet"
	nvmeFabricsDev      = "/dev/nvme-fabrics"
	nvmeSysfsClass      = "/sys/class/nvme"
	defaultNVMETPort    = "4420"
	defaultNVMETTLSPort = "4421"

	// defaultDeviceWaitTimeout bounds the in-line poll for the zvol /dev node
	// before configfs opens device_path. This poll holds the reconciler's
	// per-dataset lock and a worker slot, so it is kept MODEST rather than long:
	// a long in-lock block under a 128-volume burst would pin the worker pool
	// (the poll holds the reconcile worker for its whole duration). The
	// convergence mechanism for a genuinely slow udev is NOT this wait but the
	// reconciler's ErrDeviceNotReady requeue, which re-drives the volume WITHOUT
	// marking Error and WITHOUT re-minting the DEK (reconcileCreate skips the
	// crypto fetch/stage once the dataset exists). So the effective retry is
	// unbounded across passes, cheap per pass, and never fails the PVC. This
	// short wait just lets the common (sub-second udev) case finish in one pass;
	// anything slower falls through to the requeue rather than holding a worker.
	defaultDeviceWaitTimeout = 5 * time.Second
	deviceWaitInterval       = 200 * time.Millisecond
)

// NVMET is an NVMe-TCP target backed by configfs.
type NVMET struct {
	w         Writer
	base      string
	portID    int
	tlsPortID int

	// deviceWaitTimeout gives udev/device-manager enough time to surface a zvol
	// during provisioning bursts before configfs opens device_path.
	deviceWaitTimeout time.Duration

	// portMu serialises the shared-port critical section (configurePort +
	// port->subsystem symlink). Plaintext and TLS ports are shared objects; one
	// mutex protects both. Per-volume subsystem/namespace ops stay lock-free.
	portMu sync.Mutex
}

const defaultTLSPortID = 2

// NewNVMET returns an NVMe-TCP transport server using configfs writer w.
func NewNVMET(w Writer) *NVMET { return NewNVMETAt(w, defaultNVMETBase, 1) }

// NewNVMETAt returns an NVMe-TCP server rooted at base, useful for tests.
func NewNVMETAt(w Writer, base string, portID int) *NVMET {
	if portID == 0 {
		portID = 1
	}

	tlsPortID := defaultTLSPortID
	if tlsPortID == portID {
		tlsPortID++
	}
	return &NVMET{w: w, base: strings.TrimRight(base, "/"), portID: portID, tlsPortID: tlsPortID, deviceWaitTimeout: defaultDeviceWaitTimeout}
}

// waitForDevice blocks until the block-device node at path exists (udev has
// created the zvol symlink) or the context/timeout elapses. It is a bounded
// poll — nvmet's enable write opens device_path synchronously, so the node must
// be present first.
func (n *NVMET) waitForDevice(ctx context.Context, devPath string) error {
	timeout := n.deviceWaitTimeout
	if timeout <= 0 {
		timeout = defaultDeviceWaitTimeout
	}
	deadline := time.Now().Add(timeout)
	for {
		ok, err := n.w.Lexists(devPath)
		if err != nil {
			return fmt.Errorf("stat zvol device %s: %w", devPath, err)
		}
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%w: zvol device %s did not appear within %s", ErrDeviceNotReady, devPath, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(deviceWaitInterval):
		}
	}
}

func (n *NVMET) Export(ctx context.Context, opts ExportOptions) (TargetRef, error) {
	host, svc := exportPortal(opts.Portal, opts.TLS)
	ref := TargetRef{
		Kind:        KindNVMeTCP,
		TargetNQN:   opts.TargetNQN,
		Portal:      net.JoinHostPort(host, svc),
		NamespaceID: 1,
		DeviceGUID:  opts.DeviceGUID,
		TLS:         opts.TLS,
	}
	if ref.TargetNQN == "" {
		return ref, errNVMETTargetNQNRequired
	}

	if opts.ZvolPath == "" {
		return ref, errNVMETZvolPathRequired
	}

	// Wait for the zvol device node to appear. `zfs create` returns before udev
	// has created the /dev/zvol/<pool>/... symlink, but writing "1" to the nvmet
	// namespace's enable attribute makes the kernel open device_path immediately
	// — so without this wait the enable write races udev and fails with ENOENT
	// ("no such file or directory") on a freshly-created volume. Poll (bounded)
	// until the node exists before touching configfs.
	if err := n.waitForDevice(ctx, opts.ZvolPath); err != nil {
		return ref, err
	}

	// Validate or create the shared listener before creating the subsystem. If a
	// caller supplies a portal inconsistent with an immutable in-use port, avoid
	// leaving behind an enabled but unreachable partial export.
	n.portMu.Lock()
	err := n.configurePort(ref)
	n.portMu.Unlock()
	if err != nil {
		return ref, err
	}

	subsys := n.subsys(ref.TargetNQN)

	ns := path.Join(subsys, "namespaces", strconv.Itoa(ref.NamespaceID))

	// Idempotency: reconcileEnsure re-calls Export on every pass (reboot
	// reconciliation). If the namespace already exists and is enabled, the
	// configfs device_path attribute is IMMUTABLE while enabled — re-writing it
	// fails EBUSY ("device or resource busy"). Detect the already-exported state
	// and skip the (re)write of those immutable namespace attributes. A namespace
	// that exists but is disabled (partial/reboot state) falls through to the full
	// (re)configuration below.
	//
	// CRITICAL (Bug SS): even on the already-enabled path we MUST NOT early-return
	// — the port->subsystem link is a SEPARATE configfs object from the namespace.
	// A subsystem can be fully namespaced+enabled yet UNLINKED from the NVMe-TCP
	// port (a prior Export enabled the ns but the port-link step failed, or the
	// link was torn down out of band). Without the link the target is unreachable
	// at the portal and the initiator connect fails NVME_SC_CONNECT_INVALID_PARAM
	// ("Connect Invalid Data Parameter"). So fall through to the level-triggered
	// port + link ensure below on every pass (memory 2246: Ready reconcile
	// idempotently reapplies the full export state).
	alreadyEnabled := false
	if ok, _ := n.w.Lexists(ns); ok {
		if enabled, err := n.w.ReadFile(path.Join(ns, "enable")); err == nil && strings.TrimSpace(string(enabled)) == "1" {
			alreadyEnabled = true
		}
	}

	if !alreadyEnabled {
		if err := n.w.MkdirAll(ns); err != nil {
			return ref, err
		}

		if err := n.w.WriteFile(path.Join(subsys, "attr_allow_any_host"), []byte("0")); err != nil {
			return ref, err
		}

		if err := n.w.WriteFile(path.Join(ns, "device_path"), []byte(opts.ZvolPath)); err != nil {
			return ref, err
		}

		if ref.DeviceGUID != "" {
			if err := n.w.WriteFile(path.Join(ns, "device_nguid"), []byte(ref.DeviceGUID)); err != nil {
				return ref, err
			}
		}

		if err := n.w.WriteFile(path.Join(ns, "enable"), []byte("1")); err != nil {
			return ref, err
		}
	}

	// Port + link are ensured on EVERY pass (idempotent), including the
	// already-enabled path — this is the Bug SS fix. The shared port is mutable
	// state touched by every volume, so serialise this section under portMu to be
	// safe with concurrent reconciles (MaxConcurrentReconciles>1).
	n.portMu.Lock()
	defer n.portMu.Unlock()

	link := path.Join(n.portDir(ref.TLS), "subsystems", ref.TargetNQN)
	if ok, err := n.w.Lexists(link); err != nil {
		return ref, err
	} else if !ok {
		if err := n.w.Symlink(subsys, link); err != nil {
			return ref, err
		}
	}

	if alreadyEnabled {
		// The namespace already exists and is enabled, so device_path/device_nguid
		// were left untouched (they are immutable while enabled). But the backing
		// zvol may have GROWN since it was first exported (online expand): nvmet
		// caches the namespace size at enable time and only re-reads the backing
		// device when told to. Write revalidate_size=1 so the target re-reads the
		// (possibly larger) zvol and advertises the new size to initiators — this
		// is the TARGET-side half of online expand; the node still rescans its
		// controller to see it. Idempotent + cheap (a no-op when the size is
		// unchanged) and reapplied on every level-triggered ensure pass, so a lost
		// write self-heals next pass. Best-effort: the attribute needs a modern
		// kernel (>=5.11); if absent the kernel's NS_CHANGED AEN path still
		// eventually propagates the new size.
		_ = n.w.WriteFile(path.Join(ns, "revalidate_size"), []byte("1"))

		// Signal to callers that the namespace itself was untouched (they treat
		// this as success), while the port-link ensure above still ran.
		return ref, ErrAlreadyExported
	}
	return ref, nil
}

func (n *NVMET) Unexport(ctx context.Context, ref TargetRef) error {
	// configfs objects are torn down ONLY by rmdir of their directories (after
	// disabling), never by unlinking their attribute files — os.RemoveAll fails
	// "operation not permitted" on every attr_* file and leaves the namespace
	// ENABLED, so it keeps the backing zvol open and a subsequent `zfs destroy`
	// fails with EBUSY ("dataset is busy") in an endless retry loop. Tear down in
	// the correct order: (1) drop the port->subsystem link, (2) disable + rmdir
	// every namespace (releases the zvol), (3) rmdir the subsystem. Each step is
	// best-effort/idempotent so a partial prior teardown still converges.
	subsys := n.subsys(ref.TargetNQN)

	// 1. Remove the port symlink so no new connections target the subsystem.
	// Guard the shared-port dir under portMu (concurrent Export/Unexport safety).
	n.portMu.Lock()
	link := path.Join(n.portDir(ref.TLS), "subsystems", ref.TargetNQN)
	if ok, _ := n.w.Lexists(link); ok {
		_ = n.w.Remove(link) // unlink the symlink
	}
	n.portMu.Unlock()

	// 2. Disable + rmdir each namespace to release the backing device.
	nsDir := path.Join(subsys, "namespaces")
	if entries, err := n.w.ReadDir(nsDir); err == nil {
		for _, ns := range entries {
			nsPath := path.Join(nsDir, ns)
			_ = n.w.WriteFile(path.Join(nsPath, "enable"), []byte("0"))
			_ = n.w.Remove(nsPath) // rmdir the (now-disabled) namespace dir
		}
	}

	// 3. rmdir the subsystem directory itself.
	if ok, _ := n.w.Lexists(subsys); !ok {
		return nil
	}

	return n.w.Remove(subsys)
}

// hostNQNPrefix namespaces a node's initiator id into a spec-valid NVMe host
// NQN. The kernel /dev/nvme-fabrics connect rejects a bare node name (e.g.
// "ip-10-0-77-26") as an invalid host NQN — NVMe requires the nqn.<date>.<domain>
// form — so the presented hostnqn no longer matches the target's allowed_hosts
// entry and the target returns NVME_SC_CONNECT_INVALID_HOST (status 0x184). Both
// the allow-list entry (MapInitiator) and the initiator's connect string
// (Attach) must use this same wrapped form so they match.
const hostNQNPrefix = "nqn.2026-01.csi.randomvariable:host:"

// hostNQN wraps a raw initiator id into a valid NVMe host NQN.
func hostNQN(initiatorID string) string { return hostNQNPrefix + initiatorID }

// HostNQN exposes the canonical initiator identity used in the target
// allow-list and the initiator's NVMe/TCP connect request.
func HostNQN(initiatorID string) string { return hostNQN(initiatorID) }

// initiatorFromHostNQN reverses hostNQN so MappedInitiators returns raw
// initiator ids (node names) — the form the volume reconciler's drift check
// compares against vol.Status.MappedInitiators. An entry without the prefix is
// returned unchanged (defensive: tolerate a manually-added host).
func initiatorFromHostNQN(nqn string) string { return strings.TrimPrefix(nqn, hostNQNPrefix) }

// hostID derives a deterministic RFC-4122-shaped UUID from an initiator id. The
// kernel NVMe fabrics layer enforces a 1:1 hostid<->hostnqn binding: reusing the
// node's single /etc/nvme/hostid with our per-node hostnqn is rejected with
// "found same hostid ... but different hostnqn". Presenting a hostid derived from
// the same initiator id keeps the pair consistent across every connect from this
// host. Deterministic (sha256) so reconnects reuse the same identity.
func hostID(initiatorID string) string {
	sum := sha256.Sum256([]byte(hostNQNPrefix + initiatorID))
	b := sum[:16]
	// RFC 4122 version 4 / variant bits so the kernel accepts it as a UUID.
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func (n *NVMET) MapInitiator(ctx context.Context, ref TargetRef, initiatorID string) error {
	if initiatorID == "" {
		return errInitiatorIDRequired
	}

	hnqn := hostNQN(initiatorID)
	host := path.Join(n.base, "hosts", hnqn)
	if err := n.w.MkdirAll(host); err != nil {
		return err
	}

	allowed := path.Join(n.subsys(ref.TargetNQN), "allowed_hosts", hnqn)
	if ok, err := n.w.Lexists(allowed); err != nil {
		return err
	} else if ok {
		return nil
	}

	return n.w.Symlink(host, allowed)
}

func (n *NVMET) UnmapInitiator(ctx context.Context, ref TargetRef, initiatorID string) error {
	// allowed_hosts entries are symlinks — unlink (Remove), not RemoveAll (which
	// would EPERM on a configfs attribute tree). The entry is stored under the
	// wrapped host NQN.
	return n.w.Remove(path.Join(n.subsys(ref.TargetNQN), "allowed_hosts", hostNQN(initiatorID)))
}

func (n *NVMET) MappedInitiators(ctx context.Context, ref TargetRef) ([]string, error) {
	// A missing subsystem is distinct from an exported target with no allowed
	// hosts. The agent uses ErrNotExported to durably report configfs loss before
	// its level-triggered Export repair recreates the target.
	if exists, err := n.w.Lexists(n.subsys(ref.TargetNQN)); err != nil {
		return nil, err
	} else if !exists {
		return nil, ErrNotExported
	}

	entries, err := n.w.ReadDir(path.Join(n.subsys(ref.TargetNQN), "allowed_hosts"))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}

		return nil, nil
	}

	// allowed_hosts are stored as wrapped host NQNs; return the raw initiator ids
	// so the reconciler's drift comparison against vol.Status.MappedInitiators
	// (node names) matches.
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, initiatorFromHostNQN(e))
	}
	sort.Strings(out)

	return out, nil
}

// ForceDisconnect fences all established controllers of the target's subsystem
// by bouncing the port→subsystem symlink. Removing the link triggers the
// kernel's nvmet_port_del_ctrls (drivers/nvme/target/configfs.c), which tears
// down EVERY controller currently connected to that subsystem — unlike dropping
// an allowed_hosts entry, which only edits the allow-list and leaves live
// controllers running. After the link is removed we immediately re-create it so
// legitimate initiators (still present in allowed_hosts) reconnect within their
// reconnect_delay; the fenced host, absent from allowed_hosts, is rejected.
//
// The port→subsystem link is shared mutable state on the single per-node port,
// so the bounce is serialised under portMu (the same lock Export/Unexport use
// for their port-link mutations). Idempotent: a missing subsystem or a missing
// link is treated as already-fenced (success) — re-creating the link when the
// subsystem is present restores reachability.
func (n *NVMET) ForceDisconnect(ctx context.Context, ref TargetRef) error {
	subsys := n.subsys(ref.TargetNQN)
	if ok, _ := n.w.Lexists(subsys); !ok {
		// No subsystem: nothing to fence.
		return nil
	}

	n.portMu.Lock()
	defer n.portMu.Unlock()

	link := path.Join(n.portDir(ref.TLS), "subsystems", ref.TargetNQN)
	// Remove the link -> kernel drops all controllers of the subsystem.
	if ok, _ := n.w.Lexists(link); ok {
		if err := n.w.Remove(link); err != nil {
			return fmt.Errorf("force-disconnect: remove port link %s: %w", link, err)
		}
	}
	// Re-create the link so surviving legitimate initiators can reconnect. If
	// either step below fails, the subsystem is left transiently unlinked
	// (unreachable) until the next reconcileEnsure pass, whose Export re-links it
	// before it re-invokes this fence — so the gap is self-healing, not durable.
	if err := n.configurePort(ref); err != nil {
		return fmt.Errorf("force-disconnect: reconfigure port: %w", err)
	}
	if err := n.w.Symlink(subsys, link); err != nil {
		return fmt.Errorf("force-disconnect: relink port %s: %w", link, err)
	}

	return nil
}

// HostFS abstracts host-side file I/O for the NVMe host client: writes to the
// /dev/nvme-fabrics misc char device and reads from /sys/class/nvme. Injectable
// for tests via NewNVMETClientWithHostFS.
type HostFS interface {
	WriteFile(path string, data []byte) error
	ReadFile(path string) ([]byte, error)
	ReadDir(dir string) ([]string, error)
}

// realHostFS uses the real OS filesystem. /dev/nvme-fabrics and sysfs
// attributes must be opened O_WRONLY (not O_TRUNC|O_CREAT) — they are kernel
// pseudo-files, not regular files.
type realHostFS struct{}

func (realHostFS) WriteFile(p string, data []byte) error {
	f, err := os.OpenFile(p, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = f.Write(data)
	return err
}

func (realHostFS) ReadFile(p string) ([]byte, error) { return os.ReadFile(p) }

func (realHostFS) ReadDir(d string) ([]string, error) {
	entries, err := os.ReadDir(d)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names, nil
}

// NVMETClient is the consumer-node NVMe-TCP attach/detach client. It talks
// directly to the kernel NVMe-oF fabrics layer via the /dev/nvme-fabrics misc
// char device (connect) and sysfs (disconnect + device discovery) — NO nvme
// binary, NO os/exec. This honours the project's no-CLI-wrapping rule.
//
// Kernel reference: drivers/nvme/host/fabrics.c — nvmf_dev_write() parses a
// comma-separated key=value string and calls nvmf_create_ctrl(); this is the
// same path nvme-cli/libnvme use internally.
type NVMETClient struct {
	fs HostFS
	// ctrlLossTMO is the kernel ctrl_loss_tmo (seconds) applied to every fabrics
	// connect. -1 means "retry forever" (the CSI NVMe-oF norm): without it the
	// kernel default of 600s deletes the controller after 10 minutes of failed
	// reconnects, so a storage-node outage longer than that vanishes the /dev
	// node under a staged mount and wedges the pod on EIO with no self-heal.
	// Detach still deletes the controller explicitly, so retry-forever does not
	// leak controllers on normal teardown.
	ctrlLossTMO int
	// reconnectDelay is the kernel reconnect_delay (seconds) between reconnect
	// attempts. Set explicitly for determinism (kernel default is also 10s).
	reconnectDelay int
}

// NVMETClientOption configures an NVMETClient.
type NVMETClientOption func(*NVMETClient)

// WithCtrlLossTMO sets the ctrl_loss_tmo (seconds) for fabrics connects. -1 =
// retry forever.
func WithCtrlLossTMO(seconds int) NVMETClientOption {
	return func(c *NVMETClient) { c.ctrlLossTMO = seconds }
}

// WithReconnectDelay sets the reconnect_delay (seconds) for fabrics connects.
func WithReconnectDelay(seconds int) NVMETClientOption {
	return func(c *NVMETClient) { c.reconnectDelay = seconds }
}

// defaultCtrlLossTMO retries forever; defaultReconnectDelay matches the kernel.
const (
	defaultCtrlLossTMO    = -1
	defaultReconnectDelay = 10
)

// NewNVMETClient returns a node-side NVMe-TCP client using the host filesystem.
func NewNVMETClient(opts ...NVMETClientOption) *NVMETClient {
	c := &NVMETClient{fs: realHostFS{}, ctrlLossTMO: defaultCtrlLossTMO, reconnectDelay: defaultReconnectDelay}
	for _, o := range opts {
		o(c)
	}

	return c
}

// NewNVMETClientWithHostFS returns a client backed by a custom HostFS (for tests).
func NewNVMETClientWithHostFS(fs HostFS, opts ...NVMETClientOption) *NVMETClient {
	c := &NVMETClient{fs: fs, ctrlLossTMO: defaultCtrlLossTMO, reconnectDelay: defaultReconnectDelay}
	for _, o := range opts {
		o(c)
	}

	return c
}

// Attach connects to the NVMe-TCP target by writing the connect string to
// /dev/nvme-fabrics, then discovers the resulting /dev/nvmeXnY device node by
// scanning /sys/class/nvme for the controller whose subsystem NQN matches.
func (c *NVMETClient) Attach(ctx context.Context, ref TargetRef, initiatorID string) (string, error) {
	host, svc := splitPortal(ref.Portal, "4420")
	if ref.TLS && svc != "4421" {
		return "", fmt.Errorf("TLS NVMe/TCP target must use dedicated service 4421, got %q", svc)
	}

	// Kernel fabrics connect string: comma-separated key=value. Writing this to
	// /dev/nvme-fabrics is synchronous — the controller appears under
	// /sys/class/nvme before the write returns (or the write fails with
	// -ECONNREFUSED if the target is unreachable).
	connect := "transport=tcp,traddr=" + host + ",trsvcid=" + svc + ",nqn=" + ref.TargetNQN
	if initiatorID != "" {
		// The target allow-lists the wrapped host NQN (MapInitiator), and the
		// kernel requires hostnqn to be a valid NQN — present the same wrapped
		// form here so the connect is authorised (else NVME_SC_CONNECT_INVALID_HOST).
		// hostid must accompany hostnqn and stay consistent with it: the kernel
		// binds hostid<->hostnqn 1:1 and rejects the node's default /etc/nvme/hostid
		// paired with our per-node hostnqn ("found same hostid but different hostnqn").
		connect += ",hostnqn=" + hostNQN(initiatorID) + ",hostid=" + hostID(initiatorID)
	}
	if ref.TLS {
		// A node-local credential sidecar must install the retained PSK before
		// attach. The transport receives no credential material and fails closed
		// if the kernel cannot complete this TLS connection.
		connect += ",tls"
	}
	// Reconnect tunables (F2): bound the reconnect behaviour explicitly so a
	// storage-node outage does not silently delete the controller (kernel
	// default ctrl_loss_tmo=600s). ctrl_loss_tmo=-1 retries forever.
	connect += ",ctrl_loss_tmo=" + strconv.Itoa(c.ctrlLossTMO) + ",reconnect_delay=" + strconv.Itoa(c.reconnectDelay)

	if err := c.fs.WriteFile(nvmeFabricsDev, []byte(connect)); err != nil {
		// EALREADY: another in-flight connect (retry) already established (or is
		// establishing) the controller — fall through to the discovery poll
		// rather than failing the whole stage.
		if !errors.Is(err, syscall.EALREADY) {
			return "", fmt.Errorf("write %s: %w", nvmeFabricsDev, err)
		}
	}

	// The controller registers synchronously, but the namespace HEAD device node
	// (/dev/nvmeXnY) is created by udev slightly after — bounded-poll for it.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if dev, err := c.findDevice(ref); err == nil {
			return dev, nil
		}
		if time.Now().After(deadline) {
			return "", errNVMEAttachDeviceNotFound
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// Detach disconnects all controllers whose subsystem NQN matches ref.TargetNQN
// by writing "1" to their sysfs delete_controller attribute. Idempotent.
func (c *NVMETClient) Detach(ctx context.Context, ref TargetRef) error {
	controllers, err := c.fs.ReadDir(nvmeSysfsClass)
	if err != nil {
		// No nvme class dir → nothing to detach.
		return nil
	}

	for _, ctl := range controllers {
		nqn, err := c.fs.ReadFile(filepath.Join(nvmeSysfsClass, ctl, "subsysnqn"))
		if err != nil {
			continue
		}

		if strings.TrimSpace(string(nqn)) != ref.TargetNQN {
			continue
		}

		// delete_controller is the kernel sysfs delete trigger; writing "1"
		// removes the controller synchronously. Ignore errors (controller may
		// already be gone).
		_ = c.fs.WriteFile(filepath.Join(nvmeSysfsClass, ctl, "delete_controller"), []byte("1"))
	}

	return nil
}

// normalizeNGUID lowercases an NGUID and strips non-hex separators (dashes,
// colons, whitespace) so two renderings of the same 16-byte identifier compare
// equal. The kernel sysfs nguid readback is dash-grouped lowercase hex, while
// the value written to device_nguid is plain 32-hex.
func normalizeNGUID(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// findDevice scans /sys/class/nvme for the controller whose subsystem NQN
// matches ref.TargetNQN, then resolves its namespace device to /dev/nvmeXnY.
// If ref.DeviceGUID is set the namespace NGUID is matched; otherwise the first
// namespace is returned.
func (c *NVMETClient) findDevice(ref TargetRef) (string, error) {
	controllers, err := c.fs.ReadDir(nvmeSysfsClass)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", nvmeSysfsClass, err)
	}

	for _, ctl := range controllers {
		nqn, err := c.fs.ReadFile(filepath.Join(nvmeSysfsClass, ctl, "subsysnqn"))
		if err != nil {
			continue
		}

		if strings.TrimSpace(string(nqn)) != ref.TargetNQN {
			continue
		}

		entries, err := c.fs.ReadDir(filepath.Join(nvmeSysfsClass, ctl))
		if err != nil {
			continue
		}

		for _, entry := range entries {
			// A namespace sysfs entry under the controller is either the head
			// form "nvme1n1" or, with NVMe multipath/ANA enabled (default on
			// modern kernels), the per-controller form "nvme1c1n1" (a "c<N>"
			// infix). head is "" for non-namespace entries (attr files, etc).
			head := headNamespaceName(ctl, entry)
			if head == "" {
				continue
			}

			if ref.DeviceGUID != "" {
				// nguid lives under the sysfs entry dir (the c-form when
				// multipath). The kernel renders nguid as dash-grouped lowercase
				// hex while ref.DeviceGUID is plain 32-hex; normalise both.
				nguid, err := c.fs.ReadFile(filepath.Join(nvmeSysfsClass, ctl, entry, "nguid"))
				if err == nil && normalizeNGUID(string(nguid)) != normalizeNGUID(ref.DeviceGUID) {
					continue
				}
			}

			// Return the HEAD device node (/dev/nvme1n1), never the c-form —
			// the c-form has no /dev node; I/O goes through the head.
			return "/dev/" + head, nil
		}
	}

	return "", errNVMEAttachDeviceNotFound
}

// headNamespaceName maps a controller-relative sysfs namespace entry to its
// head /dev name. For controller "nvme1": "nvme1n1" → "nvme1n1" and the
// multipath per-controller form "nvme1c1n1" → "nvme1n1". Returns "" if entry is
// not a namespace of ctl (e.g. an attribute file).
func headNamespaceName(ctl, entry string) string {
	if !strings.HasPrefix(entry, ctl) {
		return ""
	}
	rest := entry[len(ctl):] // "n<nsid>" or "c<cnum>n<nsid>"
	nIdx := strings.LastIndex(rest, "n")
	if nIdx < 0 {
		return ""
	}
	nsid := rest[nIdx+1:]
	if !allDigits(nsid) {
		return ""
	}
	if prefix := rest[:nIdx]; prefix != "" {
		// only a "c<digits>" multipath infix is allowed before the "n"
		if prefix[0] != 'c' || !allDigits(prefix[1:]) {
			return ""
		}
	}

	return ctl + "n" + nsid
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}

	return true
}

func (n *NVMET) configurePort(ref TargetRef) error {
	port := n.portDir(ref.TLS)
	if err := n.w.MkdirAll(path.Join(port, "subsystems")); err != nil {
		return err
	}

	host, svc := exportPortal(ref.Portal, ref.TLS)
	values := map[string]string{"addr_trtype": "tcp", "addr_adrfam": "ipv4", "addr_traddr": host, "addr_trsvcid": svc}
	if ref.TLS {
		// A dedicated port makes target TLS mandatory without affecting plaintext volumes.
		values["addr_tsas"] = "tls1.3"
	}

	// addr_* attributes become immutable after a subsystem is linked. Re-export
	// must therefore only verify every immutable value, not rewrite it. Checking
	// the complete tuple also prevents a second export from advertising a portal
	// that differs from the shared port's actual listener.
	if cur, err := n.w.ReadFile(path.Join(port, "addr_traddr")); err == nil {
		if strings.TrimSpace(string(cur)) != "" {
			for k, want := range values {
				got, err := n.w.ReadFile(path.Join(port, k))
				if err != nil {
					return fmt.Errorf("read configured NVMe/TCP port %d %s: %w", n.portIDFor(ref.TLS), k, err)
				}
				if strings.TrimSpace(string(got)) != want {
					return fmt.Errorf("NVMe/TCP port %d %s is %q, want %q", n.portIDFor(ref.TLS), k, strings.TrimSpace(string(got)), want)
				}
			}
			return nil
		}
	}

	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, k := range keys {
		if err := n.w.WriteFile(path.Join(port, k), []byte(values[k])); err != nil {
			return err
		}
	}

	return nil
}

func (n *NVMET) subsys(nqn string) string { return path.Join(n.base, "subsystems", nqn) }
func (n *NVMET) portIDFor(tls bool) int {
	if tls {
		return n.tlsPortID
	}
	return n.portID
}
func (n *NVMET) portDir(tls bool) string {
	return path.Join(n.base, "ports", strconv.Itoa(n.portIDFor(tls)))
}

// exportPortal derives the listener endpoint from a caller's portal. The TLS
// listener always uses its dedicated service; callers commonly supply the
// storage node's ordinary NVMe/TCP portal, so only its host is reused.
func exportPortal(portal string, tls bool) (string, string) {
	host, svc := splitPortal(portal, defaultNVMETPort)
	if tls {
		return host, defaultNVMETTLSPort
	}
	return host, svc
}

func splitPortal(portal, defaultPort string) (string, string) {
	if host, svc, err := net.SplitHostPort(portal); err == nil {
		return host, svc
	}
	return portal, defaultPort
}
