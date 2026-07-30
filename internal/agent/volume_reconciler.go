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

// Package agent implements the storage-agent reconcilers that materialise
// Volume + Snapshot CRs into real ZFS datasets + transport exports + encryption
// keys. This is the privileged worker that runs only on server7 (PLAN §1).
package agent

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/sys/unix"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apimachinerytypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/crypto"
	"github.com/randomvariable/zfs-csi/internal/naming"
	"github.com/randomvariable/zfs-csi/internal/nfs"
	"github.com/randomvariable/zfs-csi/internal/nfsexport"
	eventsv1 "github.com/randomvariable/zfs-csi/internal/observability/events"
	"github.com/randomvariable/zfs-csi/internal/observability/logging"
	"github.com/randomvariable/zfs-csi/internal/observability/metrics"
	"github.com/randomvariable/zfs-csi/internal/reachability"
	"github.com/randomvariable/zfs-csi/internal/transport"
	"github.com/randomvariable/zfs-csi/internal/zfs"
)

const (
	poolNotImportedRequeue = 30 * time.Second
	// defaultMaxConcurrentReconciles is the fallback parallelism when the
	// reconciler's MaxConcurrentReconciles is unset (<=0). 10 keeps E2E/
	// conformance churn moving while the shared-port critical section stays
	// mutex-guarded.
	defaultMaxConcurrentReconciles = 10
	targetNamespaceID              = 1
)

const healthRepairHoldAnnotation = "zfs.csi.randomvariable.co.uk/test-hold-health-repair"

// VolumeReconciler materialises Volume CRs into ZFS datasets + exports.
// It runs on server7 only and holds the privileged handles (zfs.Backend,
// transport.Server, crypto.KeyProvider, crypto.Stager).
type VolumeReconciler struct {
	crclient.Client

	Scheme *runtime.Scheme
	Log    logr.Logger

	ZFS                 zfs.Backend
	Export              transport.Server
	Keys                crypto.KeyProvider
	Stager              crypto.Stager
	Portal              string // host:port consumers connect to
	NFSServer           string // DNS name or unbracketed IP consumers mount from
	NVMeTLSSecretReader NVMeTLSSecretReader
	Namespace           string // driver namespace for namespaced supporting objects
	NodeName            string // storage node identity used to fence all node-local mutations
	// NFSTLSEnabled is deployment-level capability. A Volume requesting NFS TLS
	// must not become Ready until runtime is explicitly enabled and its leaf exists.
	NFSTLSEnabled bool
	// NFSExports is the in-process nfsd export table serving TLS (xprtsec=mtls)
	// filesystem volumes. OpenZFS sharenfs cannot express xprtsec, so TLS exports
	// are registered here and answered by the cache-channel responder instead of
	// libshare; the dataset is mounted with sharenfs=off (no plaintext export).
	// Nil when NFS TLS is disabled — the reconciler then uses libshare exclusively.
	NFSExports *nfsexport.MemTable
	// NFSFlusher invalidates cached nfsd export entries after a TLS export is
	// withdrawn (satisfied by *nfsexport.Server). Failures block deletion; nil-safe.
	NFSFlusher NFSExportFlusher
	// NFSWriter proactively rebuilds kernel cache entries. It is optional for
	// callers that only run the userspace responder (for example unit tests).
	NFSWriter nfsCacheWriter
	// NFSRootController asynchronously converges desired explicit-root state
	// after authorized auth-domain creation. Nil keeps table-only test wiring.
	NFSRootController nfsRootIntentController
	StatfsIdentity    func(string) (statfsIdentityInfo, error)
	// RootProbe validates only structural nfsd runtime prerequisites available
	// before any client establishes the auth domain. It is injectable for tests;
	// nil skips the check for table-only wiring.
	RootProbe          func(ctx context.Context, root string) error
	rootIdentity       string // host path of the registered root (guarded by rootMu)
	rootPool           string // pool name from which rootIdentity was derived
	rootPreflightGreen bool   // root preflight has succeeded (guarded by rootMu)
	rootMu             sync.Mutex
	nfsMu              sync.Mutex
	nfsEntries         map[string]nfsexport.Entry
	nfsPaths           map[string]string   // dataset identity -> current export path
	nfsWithdrawn       map[string]struct{} // datasets withdrawn successfully; retries are idempotent
	// NVMeTLSPSK retains target PSKs in the host .nvme keyring. It is injected
	// so reconciliation stays testable without kernel keyrings.
	NVMeTLSPSK NVMeTLSPSKProvisioner
	// APIReader bypasses the manager cache for certificate Secret reads.
	APIReader crclient.Reader
	// EnableHealthRepairHold permits the narrow E2E-only observation hold.
	// It remains false in all normal deployments.
	EnableHealthRepairHold bool
	// registerNFSExportHook is a test seam for serialization regression tests.
	registerNFSExportHook func(string, string)

	// Recorder emits Kubernetes Events (e.g. delete-blocked-in-use). Optional;
	// nil-safe. Wired from the manager in SetupWithManager.
	Recorder events.EventRecorder

	// MaxConcurrentReconciles bounds how many volumes reconcile in parallel.
	// <=0 falls back to defaultMaxConcurrentReconciles. Concurrency matters under
	// E2E/conformance churn (many PVCs at once); the shared nvmet port critical
	// section is mutex-guarded in the NVMET transport, per-dataset ops use locks.
	MaxConcurrentReconciles int

	// per-volID locks prevent concurrent ZFS ops on the same dataset (SPEC §9).
	locks sync.Map // map[string]*sync.Mutex keyed by dataset path
}

func desiredMappedInitiatorIDs(mapped []zfscsiv1.MappedInitiator) []string {
	ids := make([]string, 0, len(mapped))
	for _, initiator := range mapped {
		ids = append(ids, initiator.InitiatorID)
	}
	return ids
}

// NFSExportFlusher invalidates cached nfsd export entries. Satisfied by
// *nfsexport.Server. A flush failure is fatal to withdrawal because stale kernel
// state may still resolve a dataset that is being destroyed or de-adopted.
type NFSExportFlusher interface{ Flush() error }

// nfsCacheWriter is implemented by nfsexport.ChannelWriter. Root positives are
// owned by the local root controller; withdrawal keeps targeted negatives here.
type nfsCacheWriter interface {
	InstallRootPositive(nfsexport.Entry) error
	InvalidateEntry(nfsexport.Entry, []string, []nfsexport.Entry) error
	InvalidateRoot(root string) error
}

// nfsRootIntentController records desired explicit-root state without waiting
// for the kernel auth domain. *nfsexport.RootController satisfies it.
type nfsRootIntentController interface {
	SetDesired(nfsexport.Entry) error
	RemoveDesired(string) error
}

const zfsSuperMagic = 0x2fc12fc1 // ZFS_SUPER_MAGIC; x/sys/unix does not expose it on all targets.

type statfsIdentityInfo struct {
	Low, High uint32
	Type      int64
}

type nfsExportConfigError struct{ err error }

func (e *nfsExportConfigError) Error() string { return e.err.Error() }
func (e *nfsExportConfigError) Unwrap() error { return e.err }

type nfsExportTransientError struct{ err error }

func (e *nfsExportTransientError) Error() string { return e.err.Error() }
func (e *nfsExportTransientError) Unwrap() error { return e.err }

// Root preflight error sentinels. The reconciler uses these to classify probe
// outcomes: retryable -> Ready stays false + requeue, terminalConfig ->
// permanent config fault, terminalDeploy -> permanent deployment fault.
var (
	errRootPreflightRetryable      = errors.New("root preflight retryable")
	errRootPreflightTerminalConfig = errors.New("root preflight terminal config fault")
	errRootPreflightTerminalDeploy = errors.New("root preflight terminal deployment fault")
)

type rootPreflightError struct {
	classify error
	inner    error
}

func (e *rootPreflightError) Error() string {
	if e.inner == nil {
		return e.classify.Error()
	}
	return e.classify.Error() + ": " + e.inner.Error()
}

func (e *rootPreflightError) Unwrap() error { return e.classify }

// errRootPreflightRetryable wraps a retryable probe failure (ENOENT-before-import,
// EAGAIN, ETIMEDOUT, EIO, ENODEV, ENOMEM, ~2s probe timeout).
func newRootPreflightRetryable(inner error) error {
	return &rootPreflightError{classify: errRootPreflightRetryable, inner: inner}
}

// errRootPreflightTerminalConfig wraps EINVAL after the root path exists: the
// deployment's NFS root cannot be exported, the volume must never become Ready.
func newRootPreflightTerminalConfig(inner error) error {
	return &rootPreflightError{classify: errRootPreflightTerminalConfig, inner: inner}
}

// errRootPreflightTerminalDeploy wraps EACCES/EPERM: the agent lacks the host
// privileges required to probe nfsd.
func newRootPreflightTerminalDeploy(inner error) error {
	return &rootPreflightError{classify: errRootPreflightTerminalDeploy, inner: inner}
}

func isRootPreflightRetryable(err error) bool {
	return errors.Is(err, errRootPreflightRetryable)
}

func isRootPreflightTerminalConfig(err error) bool {
	return errors.Is(err, errRootPreflightTerminalConfig)
}

func (r *VolumeReconciler) handleRootPreflightError(ctx context.Context, vol *zfscsiv1.Volume, err error) (reconcile.Result, error) {
	if isRootPreflightRetryable(err) {
		// Returning the error delegates retry timing to controller-runtime's
		// rate-limited backoff while Ready remains false.
		return reconcile.Result{}, err
	}
	if errors.Is(err, errRootPreflightTerminalConfig) || errors.Is(err, errRootPreflightTerminalDeploy) {
		r.recordStatusWarning(ctx, vol, zfscsiv1.VolumeStateError, "NFS root preflight failed: "+err.Error(), volumeWarningEvent{
			reason: eventsv1.ReasonExportFailed, action: eventsv1.ActionExporting,
			publicNote: "NFS server deployment cannot export the configured root",
		})
		// Durable Error + no explicit requeue avoids a hot loop. A spec change or
		// informer resync may retry after operators repair configuration.
		return reconcile.Result{}, nil
	}
	return reconcile.Result{}, err
}

// statfsIdentity is injectable because filesystem identity is a host-kernel
// observation, not backend metadata. OpenZFS exposes its export identity via
// statfs.f_fsid.
func defaultStatfsIdentity(path string) (statfsIdentityInfo, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return statfsIdentityInfo{}, &nfsExportTransientError{err: err}
	}
	return statfsIdentityInfo{Low: uint32(st.Fsid.Val[0]), High: uint32(st.Fsid.Val[1]), Type: int64(st.Type)}, nil
}

// defaultRootProbe checks only runtime structure valid before the first client
// creates auth domain "*". Reactive auth -> export -> fh upcalls prove serving.
func defaultRootProbe(ctx context.Context, root string) error {
	return probeNFSDRuntime(ctx, root)
}

// DefaultRootProbe is the production root preflight probe exported for the
// zfs-csi binary wiring. Unit tests inject their own stub; this is the only
// surface a caller outside the agent package should reach for.
func DefaultRootProbe(ctx context.Context, root string) error {
	return defaultRootProbe(ctx, root)
}

// nfsExportEntry builds the responder table entry for a filesystem volume.
// exportPath is the authoritative mounted host path nfsd serves, obtained from
// the backend after Share for both dynamic and imported filesystems. The
// entry's TLS flag carries the volume's
// NFSTLSEnabled setting; the responder emits xprtsec=mtls only when it is set.
// CIDR + access-mode validation matches the previous libshare path exactly via
// nfs.NormalizeExportIntent.
func (r *VolumeReconciler) nfsExportEntry(vol *zfscsiv1.Volume, dataset, exportPath string) (nfsexport.Entry, error) {
	canonical, accessMode, err := nfs.NormalizeExportIntent(vol.Spec.NFSExportCIDRs, vol.Spec.NFSExportAccessMode)
	if err != nil {
		return nfsexport.Entry{}, &nfsExportConfigError{err: err}
	}
	cidrs := make([]netip.Prefix, 0, len(canonical))
	for _, value := range canonical {
		cidrs = append(cidrs, netip.MustParsePrefix(value))
	}
	mode := nfsexport.AccessRW
	if accessMode == string(nfsexport.AccessRO) {
		mode = nfsexport.AccessRO
	}
	identity := r.StatfsIdentity
	if identity == nil {
		identity = defaultStatfsIdentity
	}
	identityInfo, err := identity(exportPath)
	if err != nil {
		return nfsexport.Entry{}, fmt.Errorf("statfs export path %s: %w", exportPath, err)
	}
	if uint64(identityInfo.Type) != zfsSuperMagic {
		return nfsexport.Entry{}, &nfsExportTransientError{err: fmt.Errorf("export path %s is on filesystem type %#x, not ZFS", exportPath, identityInfo.Type)}
	}
	return nfsexport.Entry{
		Path:       exportPath,
		UUID:       nfsexport.UUIDFromStatFS(identityInfo.Low, identityInfo.High),
		CIDRs:      cidrs,
		AccessMode: mode,
		TLS:        vol.Spec.NFSTLSEnabled,
	}, nil
}

// reconstructedNFSExportEntry only uses statfs after the backend confirms that
// path is this dataset's mounted export path. Otherwise statfs could identify
// a parent ZFS filesystem and invalidate its UUID during deletion.
func (r *VolumeReconciler) reconstructedNFSExportEntry(ctx context.Context, vol *zfscsiv1.Volume, dataset, path string) (nfsexport.Entry, error) {
	info, err := r.ZFS.Get(ctx, dataset)
	if err != nil {
		if errors.Is(err, zfs.ErrNotFound) {
			return nfsexport.Entry{Path: path}, nil
		}
		return nfsexport.Entry{}, fmt.Errorf("resolve dataset %s before NFS withdrawal: %w", dataset, err)
	}
	if info.Kind != zfs.KindFilesystem || !info.Mounted || info.ExportPath == "" || info.ExportPath != path {
		return nfsexport.Entry{Path: path}, nil
	}
	entry, err := r.nfsExportEntry(vol, dataset, path)
	if err != nil {
		// A present dataset is safe to withdraw by path, but its unverified
		// identity must not be used to invalidate another filesystem.
		return nfsexport.Entry{Path: path}, nil
	}
	return entry, nil
}

// registerNFSExport upserts the responder entry for a filesystem volume. The
// in-process nfsd responder is the sole NFS export mechanism (libshare/sharenfs
// is bypassed: the dataset is mounted with sharenfs=off). No-op for
// non-filesystem volumes or when the responder table is not wired.
//
// Filesystem volumes additionally ensure the host pool root is registered as
// the NFSv4 pseudo-root (Root entry, fsid=0) before the volume entry, and run
// the kernel root preflight before the first volume becomes Ready.
func (r *VolumeReconciler) registerNFSExport(vol *zfscsiv1.Volume, dataset, exportPath string) error {
	return r.registerNFSExportCtx(context.Background(), vol, dataset, exportPath)
}

// registerNFSExportCtx resolves/probes/builds outside shared locks, then commits
// root intent/table state and child visibility as one transaction. Kernel root
// cache convergence continues asynchronously after Volume Ready.
func (r *VolumeReconciler) registerNFSExportCtx(ctx context.Context, vol *zfscsiv1.Volume, dataset, exportPath string) error {
	if r.NFSExports == nil || vol.Spec.Type != zfscsiv1.VolumeTypeFilesystem {
		return nil
	}
	rootPath, err := r.derivePoolRoot(ctx, vol.Spec.Pool)
	if err != nil {
		return err
	}
	if exportPath == rootPath || !strings.HasPrefix(exportPath, rootPath+string(filepath.Separator)) {
		return &nfsExportConfigError{err: fmt.Errorf("export path %q is outside NFS root %q", exportPath, rootPath)}
	}
	entry, err := r.nfsExportEntry(vol, dataset, exportPath)
	if err != nil {
		return fmt.Errorf("build NFS export entry: %w", err)
	}
	rootEntry := nfsexport.Entry{Path: rootPath, Root: true, AccessMode: nfsexport.AccessRO}
	if r.RootProbe != nil {
		if err := r.RootProbe(ctx, rootPath); err != nil {
			return err
		}
	}
	if r.registerNFSExportHook != nil {
		r.registerNFSExportHook(dataset, exportPath)
	}
	r.nfsMu.Lock()
	defer r.nfsMu.Unlock()
	r.rootMu.Lock()
	defer r.rootMu.Unlock()

	if r.rootIdentity != "" && r.rootIdentity != rootPath {
		return newRootPreflightTerminalConfig(fmt.Errorf(
			"one NFS-exportable pool root is supported per owner/nfsd instance: pool %s root %q conflicts with registered root %q",
			vol.Spec.Pool, rootPath, r.rootIdentity,
		))
	}
	previousPath := r.nfsPaths[dataset]
	previousEntry, previousEntryExisted := r.nfsEntries[previousPath]
	previousChildren := len(r.nfsEntries)
	_, rootExisted := r.NFSExports.Root()
	createdRoot := !rootExisted
	if err := r.NFSExports.Upsert(rootEntry); err != nil {
		return fmt.Errorf("register NFS root: %w", err)
	}
	createdDesired := false
	if r.NFSRootController != nil {
		if err := r.NFSRootController.SetDesired(rootEntry); err != nil {
			if createdRoot {
				r.NFSExports.RemoveRoot()
			}
			return fmt.Errorf("set desired NFS root: %w", err)
		}
		createdDesired = r.rootIdentity == ""
	} else if r.NFSWriter != nil {
		if err := r.NFSWriter.InstallRootPositive(rootEntry); err != nil {
			if createdRoot {
				r.NFSExports.RemoveRoot()
			}
			return fmt.Errorf("install NFS root positive: %w", err)
		}
	}

	if r.nfsPaths == nil {
		r.nfsPaths = make(map[string]string)
	}
	if previousPath != "" && previousPath != entry.Path {
		_ = r.NFSExports.Remove(previousPath)
		delete(r.nfsEntries, previousPath)
	}
	if err := r.NFSExports.UpsertBelowRoot(entry); err != nil {
		if previousPath != "" && previousPath != entry.Path && previousEntryExisted {
			_ = r.NFSExports.UpsertBelowRoot(previousEntry)
			r.nfsEntries[previousPath] = previousEntry
		}
		if createdDesired && previousChildren == 0 && r.NFSRootController != nil {
			_ = r.NFSRootController.RemoveDesired(rootPath)
		}
		if createdRoot && previousChildren == 0 {
			r.NFSExports.RemoveRoot()
		}
		return fmt.Errorf("register NFS export entry: %w", err)
	}
	if r.nfsEntries == nil {
		r.nfsEntries = make(map[string]nfsexport.Entry)
	}
	delete(r.nfsWithdrawn, dataset)
	r.nfsEntries[entry.Path] = entry
	r.nfsPaths[dataset] = entry.Path
	r.rootIdentity = rootPath
	r.rootPool = vol.Spec.Pool
	r.rootPreflightGreen = true
	return nil
}

// derivePoolRoot resolves the host mountpoint of the pool's root dataset via
// the ZFS backend. It is fail-closed: any error (missing pool, missing root
// dataset, missing mountpoint) propagates so the caller requeues rather than
// silently fabricating a root.
func (r *VolumeReconciler) derivePoolRoot(ctx context.Context, pool string) (string, error) {
	info, err := r.ZFS.Get(ctx, pool)
	if err != nil {
		return "", fmt.Errorf("resolve pool %s root dataset: %w", pool, err)
	}
	if info.Kind != zfs.KindFilesystem {
		return "", newRootPreflightTerminalConfig(fmt.Errorf("pool %s root dataset is not a filesystem", pool))
	}
	if !info.Mounted {
		return "", newRootPreflightRetryable(fmt.Errorf("pool %s root dataset is not mounted", pool))
	}
	root := filepath.Clean(info.ExportPath)
	if info.ExportPath == "" || !filepath.IsAbs(root) || root == "/" || root != info.ExportPath {
		return "", newRootPreflightTerminalConfig(fmt.Errorf("pool %s root mountpoint %q is not canonical absolute non-root", pool, info.ExportPath))
	}
	identity := r.StatfsIdentity
	if identity == nil {
		identity = defaultStatfsIdentity
	}
	stat, err := identity(root)
	if err != nil {
		return "", fmt.Errorf("statfs NFS root %s: %w", root, err)
	}
	if uint64(stat.Type) != zfsSuperMagic {
		return "", newRootPreflightTerminalConfig(fmt.Errorf("NFS root %s is on filesystem type %#x, not ZFS", root, stat.Type))
	}
	return root, nil
}

func (r *VolumeReconciler) mountedFilesystemPath(ctx context.Context, dataset string) (string, error) {
	info, err := r.ZFS.Get(ctx, dataset)
	if err != nil {
		return "", fmt.Errorf("resolve mounted filesystem %s: %w", dataset, err)
	}
	if info.Kind != zfs.KindFilesystem || !info.Mounted {
		return "", &nfsExportTransientError{err: fmt.Errorf("filesystem %s is not mounted", dataset)}
	}
	path := filepath.Clean(info.ExportPath)
	if info.ExportPath == "" || !filepath.IsAbs(path) || path != info.ExportPath {
		return "", &nfsExportConfigError{err: fmt.Errorf("filesystem %s mountpoint %q is not canonical absolute", dataset, info.ExportPath)}
	}
	return path, nil
}

// clearRootStateForTest resets the cached root identity + preflight flag for
// restart-reconstruction tests. Production restarts reconstruct the same state
// via ensureRootRegistered before the first registerNFSExport call.
func (r *VolumeReconciler) clearRootStateForTest() {
	r.rootMu.Lock()
	defer r.rootMu.Unlock()
	r.rootIdentity = ""
	r.rootPool = ""
	r.rootPreflightGreen = false
}

func explicitNFSClients(cidrs []netip.Prefix) []netip.Addr {
	clients := make([]netip.Addr, 0, len(cidrs))
	seen := make(map[netip.Addr]struct{}, len(cidrs))
	for _, cidr := range cidrs {
		if cidr.IsSingleIP() {
			client := cidr.Addr().Unmap()
			if _, ok := seen[client]; ok {
				continue
			}
			seen[client] = struct{}{}
			clients = append(clients, client)
		}
	}
	return clients
}

// survivingNFSVolumeEntries reads durable NFS intent before taking nfsMu. The
// in-process table is not durable, so this is needed after an agent restart.
func (r *VolumeReconciler) survivingNFSVolumeEntries(ctx context.Context, log logr.Logger, withdrawnPath string) ([]nfsexport.Entry, error) {
	reader := crclient.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	if reader == nil {
		return nil, errors.New("list surviving NFS volumes: no Kubernetes reader")
	}
	volumes := &zfscsiv1.VolumeList{}
	if err := reader.List(ctx, volumes); err != nil {
		return nil, fmt.Errorf("list surviving NFS volumes: %w", err)
	}
	byPath := make(map[string]nfsexport.Entry, len(volumes.Items))
	for i := range volumes.Items {
		vol := &volumes.Items[i]
		if vol.Spec.OwnerNode != r.NodeName || vol.Spec.Type != zfscsiv1.VolumeTypeFilesystem ||
			vol.DeletionTimestamp != nil || vol.Status.CurrentState() == zfscsiv1.VolumeStateDeleting ||
			vol.Status.CurrentState() == zfscsiv1.VolumeStateDestroyed || vol.Status.ExportPath == "" ||
			vol.Status.ExportPath == withdrawnPath {
			continue
		}
		canonical, _, err := nfs.NormalizeExportIntent(vol.Spec.NFSExportCIDRs, vol.Spec.NFSExportAccessMode)
		if err != nil {
			// Excluding malformed intent fails closed for this authorization
			// entry: it cannot accidentally preserve stale access.
			log.Error(err, "skip malformed surviving NFS export intent", "volume", vol.Name)
			continue
		}
		cidrs := make([]netip.Prefix, 0, len(canonical))
		for _, value := range canonical {
			cidrs = append(cidrs, netip.MustParsePrefix(value))
		}
		byPath[vol.Status.ExportPath] = nfsexport.Entry{Path: vol.Status.ExportPath, CIDRs: cidrs}
	}
	entries := make([]nfsexport.Entry, 0, len(byPath))
	for _, entry := range byPath {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	return entries, nil
}

// withdrawNFSExport removes the responder entry for a filesystem volume and
// flushes stale cache entries so a destroyed/de-adopted dataset stops resolving.
// No-op for non-filesystem volumes or when the table is not wired.
func (r *VolumeReconciler) withdrawNFSExport(ctx context.Context, log logr.Logger, vol *zfscsiv1.Volume, dataset string) error {
	if r.NFSExports == nil || vol.Spec.Type != zfscsiv1.VolumeTypeFilesystem {
		return nil
	}
	r.nfsMu.Lock()
	locked := true
	defer func() {
		if locked {
			r.nfsMu.Unlock()
		}
	}()
	if _, withdrawn := r.nfsWithdrawn[dataset]; withdrawn {
		locked = false
		r.nfsMu.Unlock()
		return nil
	}
	if r.nfsEntries == nil {
		r.nfsEntries = make(map[string]nfsexport.Entry)
	}
	if r.nfsPaths == nil {
		r.nfsPaths = make(map[string]string)
	}
	path := r.nfsPaths[dataset]
	locked = false
	r.nfsMu.Unlock()
	if path == "" {
		path = vol.Status.ExportPath
	}
	if path == "" {
		if vol.Spec.Provenance == zfscsiv1.VolumeProvenanceImported {
			info, err := r.ZFS.Get(ctx, dataset)
			if err != nil {
				if errors.Is(err, zfs.ErrNotFound) {
					// Missing imported dataset leaves no identity to reconstruct;
					// withdraw by its safe derived path.
					path = "/" + dataset
					goto resolved
				}
				return fmt.Errorf("resolve imported export path %s before withdrawal: %w", dataset, err)
			}
			path = info.ExportPath
			if path == "" {
				return fmt.Errorf("resolve imported export path %s before withdrawal: backend returned empty path", dataset)
			}
		} else {
			path = "/" + dataset
		}
	}

resolved:
	oldEntry, hadEntry := nfsexport.Entry{Path: path}, false
	apiEntries := []nfsexport.Entry(nil)
	if r.NFSWriter != nil {
		var err error
		apiEntries, err = r.survivingNFSVolumeEntries(ctx, log, path)
		if err != nil {
			return err
		}
	}
	r.nfsMu.Lock()
	locked = true
	if _, withdrawn := r.nfsWithdrawn[dataset]; withdrawn {
		locked = false
		r.nfsMu.Unlock()
		return nil
	}
	if trackedPath := r.nfsPaths[dataset]; trackedPath != "" {
		path = trackedPath
	}
	if tracked, ok := r.nfsEntries[path]; ok {
		oldEntry, hadEntry = tracked, true
	} else if tracked, ok := r.NFSExports.LookupRealExport("*", path); ok {
		oldEntry, hadEntry = tracked, true
	}
	invalidationByPath := make(map[string]nfsexport.Entry, len(r.nfsEntries)+len(apiEntries))
	for _, current := range apiEntries {
		if current.Path != path {
			invalidationByPath[current.Path] = current
		}
	}
	for _, current := range r.nfsEntries {
		if current.Path == path {
			continue
		}
		invalidationByPath[current.Path] = current
	}
	invalidationSurvivors := make([]nfsexport.Entry, 0, len(invalidationByPath))
	for _, current := range invalidationByPath {
		invalidationSurvivors = append(invalidationSurvivors, current)
	}
	sort.Slice(invalidationSurvivors, func(i, j int) bool { return invalidationSurvivors[i].Path < invalidationSurvivors[j].Path })
	// Reconstruct identity after restart, but retain it even though no local
	// entry was found: its UUID and clients are required for cache invalidation.
	if !hadEntry && r.NFSWriter != nil {
		locked = false
		r.nfsMu.Unlock()
		resolvedEntry, err := r.reconstructedNFSExportEntry(ctx, vol, dataset, path)
		if err != nil {
			return fmt.Errorf("resolve NFS export identity %s before withdrawal: %w", path, err)
		}
		r.nfsMu.Lock()
		locked = true
		if _, withdrawn := r.nfsWithdrawn[dataset]; withdrawn {
			locked = false
			r.nfsMu.Unlock()
			return nil
		}
		if trackedPath := r.nfsPaths[dataset]; trackedPath != "" {
			path = trackedPath
		}
		if tracked, ok := r.nfsEntries[path]; ok {
			oldEntry, hadEntry = tracked, true
		} else {
			oldEntry = resolvedEntry
		}
		invalidationByPath = make(map[string]nfsexport.Entry, len(r.nfsEntries)+len(apiEntries))
		for _, current := range apiEntries {
			if current.Path != path {
				invalidationByPath[current.Path] = current
			}
		}
		for _, current := range r.nfsEntries {
			if current.Path != path {
				invalidationByPath[current.Path] = current
			}
		}
		invalidationSurvivors = invalidationSurvivors[:0]
		for _, current := range invalidationByPath {
			invalidationSurvivors = append(invalidationSurvivors, current)
		}
		sort.Slice(invalidationSurvivors, func(i, j int) bool { return invalidationSurvivors[i].Path < invalidationSurvivors[j].Path })
	}
	// Keep nfsMu held through responder mutation, targeted invalidation, and the
	// withdrawn-state commit so registration cannot race withdrawal.
	r.NFSExports.Remove(path)
	delete(r.nfsEntries, path)
	if r.nfsPaths != nil && r.nfsPaths[dataset] == path {
		delete(r.nfsPaths, dataset)
	}
	restore := func() {
		if hadEntry {
			if r.nfsEntries == nil {
				r.nfsEntries = make(map[string]nfsexport.Entry)
			}
			if r.nfsPaths == nil {
				r.nfsPaths = make(map[string]string)
			}
			r.NFSExports.Upsert(oldEntry)
			r.nfsEntries[path] = oldEntry
			r.nfsPaths[dataset] = path
		}
	}
	rootPath, rootPresent := r.NFSExports.Root()
	rootEntry, _ := r.NFSExports.LookupRealExport("*", rootPath)
	r.rootMu.Lock()
	oldRootIdentity, oldRootPool, oldRootGreen := r.rootIdentity, r.rootPool, r.rootPreflightGreen
	r.rootMu.Unlock()
	rollbackAll := func() {
		restore()
		if rootPresent {
			_ = r.NFSExports.Upsert(rootEntry)
		}
		r.rootMu.Lock()
		r.rootIdentity, r.rootPool, r.rootPreflightGreen = oldRootIdentity, oldRootPool, oldRootGreen
		r.rootMu.Unlock()
	}
	if r.NFSWriter != nil {
		clients := explicitNFSClients(oldEntry.CIDRs)
		clientStrings := make([]string, 0, len(clients))
		for _, client := range clients {
			clientStrings = append(clientStrings, client.String())
		}
		if err := r.NFSWriter.InvalidateEntry(oldEntry, clientStrings, invalidationSurvivors); err != nil {
			rollbackAll()
			locked = false
			r.nfsMu.Unlock()
			log.Error(err, "invalidate nfsd export cache")
			return fmt.Errorf("invalidate nfsd export cache: %w", err)
		}
	} else if r.NFSFlusher != nil {
		if err := r.NFSFlusher.Flush(); err != nil {
			rollbackAll()
			locked = false
			r.nfsMu.Unlock()
			log.Error(err, "flush nfsd export cache after withdraw")
			return err
		}
	}
	// Last-volume withdrawal: drop the explicit root + invalidate fsid=0 so
	// the kernel cannot keep resolving a stale root after the host has no more
	// filesystem exports. The pool root dataset is never unmounted or
	// destroyed — only the in-process NFS root state is withdrawn.
	if len(invalidationSurvivors) == 0 {
		rootEntry, hadRoot := r.NFSExports.RemoveRoot()
		if hadRoot {
			if r.NFSRootController != nil {
				if err := r.NFSRootController.RemoveDesired(rootEntry.Path); err != nil {
					rollbackAll()
					log.Error(err, "remove desired nfsd root")
					return fmt.Errorf("remove desired nfsd root: %w", err)
				}
			}
			if r.NFSWriter != nil {
				if err := r.NFSWriter.InvalidateRoot(rootEntry.Path); err != nil {
					if r.NFSRootController != nil {
						_ = r.NFSRootController.SetDesired(rootEntry)
					}
					rollbackAll()
					log.Error(err, "invalidate nfsd root export cache")
					return fmt.Errorf("invalidate nfsd root export cache: %w", err)
				}
			} else if r.NFSFlusher != nil {
				if err := r.NFSFlusher.Flush(); err != nil {
					rollbackAll()
					log.Error(err, "flush nfsd root export cache")
					return err
				}
			}
			r.rootMu.Lock()
			r.rootIdentity = ""
			r.rootPool = ""
			r.rootPreflightGreen = false
			r.rootMu.Unlock()
		}
	}
	if r.nfsWithdrawn == nil {
		r.nfsWithdrawn = make(map[string]struct{})
	}
	r.nfsWithdrawn[dataset] = struct{}{}
	locked = false
	r.nfsMu.Unlock()
	return nil
}

// ownerVolumeKey + lock helpers -------------------------------------------------

func (r *VolumeReconciler) lockFor(dataset string) *sync.Mutex {
	v, _ := r.locks.LoadOrStore(dataset, &sync.Mutex{})

	return v.(*sync.Mutex)
}

// Reconcile is the typed reconcile loop.
func (r *VolumeReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := r.Log.WithValues(logging.KeyVolume, req.String())

	vol := &zfscsiv1.Volume{}
	if err := r.Get(ctx, req.NamespacedName, vol); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}

		return reconcile.Result{}, fmt.Errorf("get volume %s: %w", req.NamespacedName, err)
	}
	if vol.Spec.OwnerNode != r.NodeName {
		return reconcile.Result{}, nil
	}
	deleting := !vol.DeletionTimestamp.IsZero() || vol.Status.CurrentState() == zfscsiv1.VolumeStateDeleting
	// Teardown must not depend on TLS liveness. Certificates, tlshd, and even
	// the TLS volume shape may be unavailable by the time deletion starts; the
	// normal delete path still preserves pool and owner identity protections.
	if !deleting && vol.Spec.NFSTLSEnabled {
		if vol.Spec.Type != zfscsiv1.VolumeTypeFilesystem {
			return reconcile.Result{}, fmt.Errorf("NFS TLS requested for non-filesystem volume")
		}
		if !r.NFSTLSEnabled {
			return r.nfsTLSUnavailable(ctx, vol, "NFS TLS runtime is not enabled on storage agent")
		}
		reader := r.APIReader
		if reader == nil {
			return r.nfsTLSUnavailable(ctx, vol, "NFS TLS direct API reader is unavailable")
		}
		if err := EnsureNFSTLS(ctx, reader, r.Namespace, r.NodeName, r.NFSServer); err != nil {
			return r.nfsTLSUnavailable(ctx, vol, "NFS TLS server certificate unavailable: "+err.Error())
		}
		if err := nfs.EnsureTLSRuntime(r.NFSServer); err != nil {
			return r.nfsTLSUnavailable(ctx, vol, "NFS TLS server runtime unavailable: "+err.Error())
		}
	}
	if !deleting && vol.Spec.NVMeTLSEnabled {
		if vol.Spec.Type != zfscsiv1.VolumeTypeBlock || vol.Spec.Transport != zfscsiv1.TransportNVMeTCP {
			return reconcile.Result{}, fmt.Errorf("NVMe TLS requested for non-NVMe-TCP block volume")
		}
	}
	if err := verifyPoolIdentity(ctx, r.ZFS, vol.Spec.Pool, vol.Spec.PoolGUID); err != nil {
		return reconcile.Result{}, err
	}
	if !deleting && vol.Spec.Type == zfscsiv1.VolumeTypeBlock {
		host, port, err := reachability.ParsePortal(r.Portal)
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("invalid owner NVMe endpoint: %w", err)
		}
		vol.Status.PortalHost, vol.Status.PortalPort = host, port
	} else if !deleting && vol.Spec.Type == zfscsiv1.VolumeTypeFilesystem {
		if err := reachability.ValidateHost(r.NFSServer); err != nil {
			return reconcile.Result{}, fmt.Errorf("invalid owner NFS endpoint: %w", err)
		}
		vol.Status.NFSServer = r.NFSServer
	}

	// Parse the volume id → dataset path.
	p, err := naming.ParseVolID(vol.Spec.VolumeID)
	if err != nil {
		log.Error(err, "malformed volumeID; marking error")

		r.recordStatusWarning(ctx, vol, zfscsiv1.VolumeStateError, "malformed volume id", volumeWarningEvent{
			reason: eventsv1.ReasonVolumeCreateFailed, action: eventsv1.ActionProvisioning, publicNote: "volume specification is invalid",
		})

		return reconcile.Result{}, nil
	}

	dataset := backendPathForVolume(vol, p)
	mu := r.lockFor(dataset)
	mu.Lock()
	defer mu.Unlock()

	// Deleting: destroy + crypto-shred.
	if deleting {
		return r.reconcileDelete(ctx, log, vol, p, dataset)
	}

	// Otherwise: ensure created + exported + keyed.
	switch vol.Status.CurrentState() {
	case "", zfscsiv1.VolumeStatePending:
		return r.reconcileCreate(ctx, log, vol, p, dataset)
	case zfscsiv1.VolumeStateReady, zfscsiv1.VolumeStateReadyToPublish:
		return r.reconcileEnsure(ctx, log, vol, p, dataset)
	case zfscsiv1.VolumeStateError:
		// Error is a RETRYABLE state, not terminal: reconcileCreate/reconcileExport
		// set Error alongside a RequeueAfter expecting the next pass to retry (e.g.
		// a transient udev/export race). Routing Error back through the idempotent
		// create path lets the volume self-heal; a genuinely permanent fault (e.g.
		// malformed id) is caught at parse time above and never reaches here.
		return r.reconcileCreate(ctx, log, vol, p, dataset)
	case zfscsiv1.VolumeStateDeleting, zfscsiv1.VolumeStateDestroyed:
		return reconcile.Result{}, nil
	}

	return reconcile.Result{}, nil
}

func (r *VolumeReconciler) nfsTLSUnavailable(ctx context.Context, vol *zfscsiv1.Volume, message string) (reconcile.Result, error) {
	r.recordStatusWarning(ctx, vol, zfscsiv1.VolumeStateError, message, volumeWarningEvent{
		reason: eventsv1.ReasonVolumeCreateFailed, action: eventsv1.ActionProvisioning, publicNote: "NFS TLS server is unavailable",
	})

	return reconcile.Result{}, fmt.Errorf("%s", message)
}

// reconcileCreate does the full create: ZFS dataset (with key) + transport export.
func (r *VolumeReconciler) reconcileCreate(
	ctx context.Context,
	log logr.Logger,
	vol *zfscsiv1.Volume,
	p naming.ParsedVolID,
	dataset string,
) (reconcile.Result, error) {
	if vol.Spec.Provenance == zfscsiv1.VolumeProvenanceImported {
		return r.reconcileImported(ctx, log, vol, p, dataset)
	}
	// 1. DEK fetch + tmpfs staging (if encrypted).
	//
	// The DEK is only needed to CREATE the encrypted dataset (zfs create loads
	// the raw key from the staged keylocation). Once the dataset exists, the key
	// is already loaded into ZFS and re-fetching it is pure waste. This matters
	// because reconcileCreate re-enters from the top on an ErrDeviceNotReady
	// requeue (the udev device-race retry, every ~1s): without this guard every
	// still-pending encrypted volume would hammer OpenBao (Keys.Fetch) and
	// re-stage+shred a tmpfs key on every requeue — a fetch storm under a
	// 128-volume burst. Skip the crypto round-trip when the dataset is already
	// present; the later KeyStatus check still verifies the key is loaded.
	var keyLocation string

	// Fail SAFE: only skip the fetch when the dataset is definitively present.
	// Any ambiguity (error, or the libzfs backend's open-failure -> (false,nil))
	// leaves datasetExists=false so a genuine first create still fetches+stages.
	datasetExists := false
	if vol.Spec.EncryptionKeyRef != "" {
		if exists, existsErr := r.ZFS.Exists(ctx, dataset); existsErr == nil {
			datasetExists = exists
		} else {
			log.V(1).Info("dataset exists check before DEK stage failed; proceeding with fetch",
				logging.KeyDataset, dataset, "error", existsErr.Error())
		}
	}

	if vol.Spec.EncryptionKeyRef != "" && !datasetExists {
		fetchOp := logging.LogWith(log, logging.OpCryptoFetch, logging.KeyKeyRef, vol.Spec.EncryptionKeyRef).
			Metric(metrics.CryptoOperationsTotal, "fetch")
		rawKey, err := r.Keys.Fetch(ctx, vol.Spec.EncryptionKeyRef)
		if err != nil {
			fetchOp.Failed(err)
			log.Error(err, "fetch DEK failed")
			r.recordStatusWarning(ctx, vol, zfscsiv1.VolumeStateError, "fetch DEK: "+err.Error(), volumeWarningEvent{
				reason: eventsv1.ReasonVolumeCreateFailed, action: eventsv1.ActionProvisioning, publicNote: "volume encryption key is unavailable",
			})

			return reconcile.Result{}, fmt.Errorf("fetch DEK: %w", err)
		}
		fetchOp.OK()

		stageOp := logging.LogWith(log, logging.OpCryptoStage, logging.KeyVolumeID, vol.Spec.VolumeID).
			Metric(metrics.CryptoOperationsTotal, "stage")
		loc, path, err := r.Stager.Stage(vol.Spec.VolumeID, rawKey)
		if err != nil {
			stageOp.Failed(err)
			log.Error(err, "stage DEK failed")
			r.recordStatusWarning(ctx, vol, zfscsiv1.VolumeStateError, "stage DEK: "+err.Error(), volumeWarningEvent{
				reason: eventsv1.ReasonVolumeCreateFailed, action: eventsv1.ActionProvisioning, publicNote: "volume encryption key is unavailable",
			})

			return reconcile.Result{}, fmt.Errorf("stage DEK: %w", err)
		}
		stageOp.OK()

		keyLocation = loc
		// shred happens after create (key is loaded into ZFS at create time).
		defer func() {
			op := logging.LogWith(log, logging.OpCryptoShred, logging.KeyPath, path).
				Metric(metrics.CryptoOperationsTotal, "shred")
			if err := r.Stager.Shred(path); err != nil {
				op.Failed(err)
			} else {
				op.OK()
			}
		}()
	}

	// 2. ZFS create/clone (idempotent: already-exists → verify + continue).
	shareNFS := ""
	if vol.Spec.Type == zfscsiv1.VolumeTypeFilesystem {
		// Validate export intent (CIDRs + access mode) before creating the
		// dataset, preserving the prior fail-fast behavior. The export itself is
		// served by the in-process nfsd responder (registered after create), so
		// sharenfs=off mounts the dataset (+ chmod) but exports nothing via
		// libshare — the responder answers the nfsd cache-channel upcalls,
		// emitting xprtsec=mtls only for TLS volumes.
		if _, _, err := nfs.NormalizeExportIntent(vol.Spec.NFSExportCIDRs, vol.Spec.NFSExportAccessMode); err != nil {
			log.Error(err, "invalid NFS export intent")
			r.recordStatusWarning(
				ctx,
				vol,
				zfscsiv1.VolumeStateError,
				"invalid NFS export: "+err.Error(),
				volumeWarningEvent{
					reason: eventsv1.ReasonVolumeCreateFailed, action: eventsv1.ActionProvisioning, publicNote: "volume provisioning configuration is invalid",
				},
			)

			return reconcile.Result{}, nil
		}
		shareNFS = "off"
	}

	createOpts := zfs.CreateOptions{
		Name:        dataset,
		Kind:        p.Kind,
		Capacity:    vol.Spec.Capacity,
		VolBlockSz:  vol.Spec.VolBlockSize,
		Compression: vol.Spec.Compression,
		Encrypted:   vol.Spec.EncryptionKeyRef != "",
		KeyFormat:   zfs.KeyFormatRaw,
		KeyLocation: keyLocation,
		ShareNFS:    shareNFS,
	}
	if p.Kind == zfs.KindFilesystem {
		// Creation-only defaults avoid metadata writes on NFS reads without
		// mutating existing datasets during steady-state reconciliation.
		createOpts.Atime = "off"
		createOpts.XAttr = "sa"
	}
	if vol.Spec.SourceSnapshotID != "" {
		result, err := r.reconcileSnapshotClone(ctx, log, vol, dataset, shareNFS)
		if err != nil || result.RequeueAfter != 0 {
			return result, err
		}
	} else if vol.Spec.SourceVolumeID != "" {
		result, err := r.reconcileVolumeClone(ctx, log, vol, p, dataset, shareNFS)
		if err != nil || result.RequeueAfter != 0 {
			return result, err
		}
	} else {
		createOp := logging.LogWith(log, logging.OpZFSCreate, logging.KeyDataset, dataset, logging.KeyCapacity, vol.Spec.Capacity, logging.KeyNFSExportCIDRs, vol.Spec.NFSExportCIDRs, logging.KeyNFSExportAccessMode, vol.Spec.NFSExportAccessMode).
			Metric(metrics.ZFSOperationsTotal, "create")
		if err := r.ZFS.Create(ctx, createOpts); err != nil && !errors.Is(err, zfs.ErrAlreadyExists) {
			createOp.Failed(err)
			r.recordStatusWarning(ctx, vol, zfscsiv1.VolumeStateError, "zfs create: "+err.Error(), volumeWarningEvent{
				reason: eventsv1.ReasonVolumeCreateFailed, action: eventsv1.ActionProvisioning, publicNote: "volume provisioning failed",
			})

			// Return the error for rate-limited exponential backoff (not a fixed
			// interval). Create is forward progress we WANT prioritised, so keep
			// the request's default priority — a failing create must NOT sink
			// below cleanup work.
			return reconcile.Result{}, fmt.Errorf("create dataset %s: %w", dataset, err)
		}
		createOp.OK()
	}

	// Register the responder export now that the filesystem is mounted with
	// sharenfs=off. The in-process responder serves every filesystem volume;
	// TLS volumes additionally emit xprtsec=mtls. Block volumes remain a no-op.
	// Registration is idempotent under create retry.
	filesystemExportPath := ""
	if vol.Spec.Type == zfscsiv1.VolumeTypeFilesystem {
		var err error
		filesystemExportPath, err = r.mountedFilesystemPath(ctx, dataset)
		if err != nil {
			return reconcile.Result{}, err
		}
	}
	if err := r.registerNFSExportCtx(ctx, vol, dataset, filesystemExportPath); err != nil {
		if isRootPreflightRetryable(err) || errors.Is(err, errRootPreflightTerminalConfig) || errors.Is(err, errRootPreflightTerminalDeploy) {
			return r.handleRootPreflightError(ctx, vol, err)
		}
		var configErr *nfsExportConfigError
		if !errors.As(err, &configErr) {
			return reconcile.Result{}, err
		}
		r.recordStatusWarning(ctx, vol, zfscsiv1.VolumeStateError, "register NFS TLS export: "+err.Error(), volumeWarningEvent{
			reason: eventsv1.ReasonVolumeCreateFailed, action: eventsv1.ActionProvisioning, publicNote: "volume provisioning configuration is invalid",
		})

		return reconcile.Result{}, nil
	}

	// 2b. Re-entry key reload. When the dataset already existed we skipped the
	// DEK fetch/stage above (create loads the key atomically, so on the normal
	// device-race requeue the key is still available and this is a cheap
	// KeyStatus no-op). But if the key was UNLOADED out of band (storage-node
	// reboot/crash while the CR is still Pending), an encrypted BLOCK zvol
	// exposes no /dev node until the key is reloaded — so reconcileExport below
	// would hit ErrDeviceNotReady and requeue at 1s FOREVER, never reaching the
	// step-4 KeyStatus safety net (reconcileExport early-returns on the device
	// wait). Reload the key here before export so that state self-heals instead
	// of looping silently.
	if datasetExists && vol.Spec.EncryptionKeyRef != "" {
		if err := r.ensureKeyLoaded(ctx, log, vol, dataset); err != nil {
			r.recordStatusWarning(ctx, vol, zfscsiv1.VolumeStateError, "reload encryption key: "+err.Error(), volumeWarningEvent{
				reason: eventsv1.ReasonVolumeCreateFailed, action: eventsv1.ActionProvisioning, publicNote: "volume encryption key is unavailable",
			})

			return reconcile.Result{}, fmt.Errorf("reload encryption key during create: %w", err)
		}
	}

	// 3. For block: export the zvol over the transport.
	zvolPath := "/dev/zvol/" + dataset

	if vol.Spec.Type == zfscsiv1.VolumeTypeBlock {
		if vol.Spec.NVMeTLSEnabled {
			nqn, err := naming.TargetNQN(vol.Spec.OwnerNode, vol.Spec.PoolGUID, p.Kind, p.ID)
			if err != nil {
				return reconcile.Result{}, fmt.Errorf("derive owner-qualified target NQN: %w", err)
			}
			if err := r.ensureNVMeTLSPSK(ctx, vol.Spec.NVMeTLSPSKSecretName, nqn, desiredMappedInitiatorIDs(vol.Status.MappedInitiators)); err != nil {
				return reconcile.Result{}, fmt.Errorf("prepare NVMe TLS PSK: %w", err)
			}
		}
		result, err := r.reconcileExport(ctx, log, vol, p, zvolPath)
		if err != nil || result.RequeueAfter != 0 {
			return result, err
		}
	}

	// 4. Verify key loaded (encrypted) + write Ready status.
	if vol.Spec.EncryptionKeyRef != "" {
		ks, err := r.ZFS.KeyStatus(ctx, dataset)
		if err != nil || ks != zfs.KeyAvailable {
			keyStatusErr := fmt.Errorf("keystatus=%s", ks)
			if err != nil {
				keyStatusErr = fmt.Errorf("keystatus=%s: %w", ks, err)
			}
			log.Error(keyStatusErr, "key not available after create")

			r.recordStatusWarning(ctx, vol, zfscsiv1.VolumeStateError, "key not available", volumeWarningEvent{
				reason: eventsv1.ReasonVolumeCreateFailed, action: eventsv1.ActionProvisioning, publicNote: "volume encryption key is unavailable",
			})

			return reconcile.Result{}, keyStatusErr
		}

		vol.Status.KeyStatus = zfscsiv1.KeyStatusAvailable
	}

	// 5. Final state. Re-read the persisted object so the MergeFrom baseline
	// reflects any status we already patched above (targetNQN/portal), and the
	// KeyStatus/State mutations below are the actual diff that gets written.
	fresh := &zfscsiv1.Volume{}
	if err := r.Get(ctx, apimachinerytypes.NamespacedName{Name: vol.Name}, fresh); err != nil {
		return reconcile.Result{}, fmt.Errorf("get fresh volume %s/%s: %w", vol.Namespace, vol.Name, err)
	}

	previousReadyStatus, previousReadyReason := volumeReadyCondition(fresh.Status.Conditions)
	previousHealthStatus, previousHealthReason := backendHealthCondition(fresh.Status.Conditions)
	before := fresh.DeepCopy()
	fresh.Status.KeyStatus = vol.Status.KeyStatus
	fresh.Status.State = zfscsiv1.VolumeStateReady
	fresh.Status.ObservedGeneration = fresh.Generation
	fresh.Status.ZvolPath = zvolPath
	fresh.Status.DatasetPath = dataset
	fresh.Status.PortalHost = vol.Status.PortalHost
	fresh.Status.PortalPort = vol.Status.PortalPort
	fresh.Status.NFSServer = vol.Status.NFSServer
	if vol.Spec.Type == zfscsiv1.VolumeTypeFilesystem {
		// Persist the exact mounted backend path and explicit NFS root used by
		// nfsd. Custom ZFS mountpoints must remain unchanged end-to-end.
		fresh.Status.ExportPath = filesystemExportPath
		r.rootMu.Lock()
		fresh.Status.NFSRootPath = r.rootIdentity
		r.rootMu.Unlock()
	}

	fresh.Status.ActualCapacity = vol.Spec.Capacity
	if fresh.Status.TargetNQN == "" {
		fresh.Status.TargetNQN = vol.Status.TargetNQN
		fresh.Status.Portal = vol.Status.Portal
		fresh.Status.DeviceGUID = vol.Status.DeviceGUID
	}

	fresh.Status.Conditions = setCondition(fresh.Status.Conditions, fresh.Generation,
		string(zfscsiv1.VolumeConditionReady), metav1.ConditionTrue,
		eventsv1.ReasonVolumeReady, "dataset created + exported")
	fresh.Status.Conditions = setCondition(fresh.Status.Conditions, fresh.Generation,
		string(zfscsiv1.VolumeConditionBackendHealthy), metav1.ConditionTrue,
		eventsv1.ReasonBackendRecovered, "backend is healthy")
	if vol.Spec.EncryptionKeyRef != "" {
		fresh.Status.Conditions = setCondition(fresh.Status.Conditions, fresh.Generation,
			string(zfscsiv1.VolumeConditionEncrypted), metav1.ConditionTrue,
			"KeyAvailable", "key available")
	}

	statusOp := logging.LogWith(log, logging.OpPatchVolumeStatus,
		logging.KeyVolume, fresh.Name,
		logging.KeyNamespace, fresh.Namespace,
		logging.KeyState, zfscsiv1.VolumeStateReady)
	if err := r.patchVolumeStatus(ctx, before, fresh,
		string(zfscsiv1.VolumeConditionReady),
		string(zfscsiv1.VolumeConditionBackendHealthy),
		string(zfscsiv1.VolumeConditionEncrypted)); err != nil {
		statusOp.Failed(err)

		return reconcile.Result{}, fmt.Errorf("patch volume ready status %s/%s: %w", fresh.Namespace, fresh.Name, err)
	}
	statusOp.OK()
	if previousHealthStatus == metav1.ConditionFalse &&
		backendHealthChanged(previousHealthStatus, previousHealthReason, fresh.Status.Conditions) {
		r.recordEvent(fresh, eventsv1.TypeNormal, eventsv1.ReasonBackendRecovered,
			eventsv1.ActionHealthChecking, "volume backend recovered")
	} else if readyConditionChanged(previousReadyStatus, previousReadyReason, fresh.Status.Conditions) {
		reason := eventsv1.ReasonVolumeReady
		// An export failure recovering is distinct from ordinary provisioning success.
		if previousReadyStatus == metav1.ConditionFalse && previousReadyReason == eventsv1.ReasonExportFailed {
			reason = eventsv1.ReasonExportRecovered
		}
		r.recordEvent(fresh, eventsv1.TypeNormal, reason, eventsv1.ActionProvisioning, "volume is ready")
	}

	log.Info("volume ready", logging.KeyDataset, dataset, logging.KeyZvol, zvolPath)

	// No RequeueAfter here: the reconcile is level-triggered and idempotent, and
	// periodic drift correction (re-applying export/link state) comes from the
	// manager's SyncPeriod (--sync-period, default 10m), which re-delivers every
	// object. A per-object requeue would be the wrong tool — reserve RequeueAfter
	// for an object that legitimately needs a *sooner* re-check, and return an
	// error for a transient failure that needs backoff.
	return reconcile.Result{}, nil
}

func backendPathForVolume(vol *zfscsiv1.Volume, parsed naming.ParsedVolID) string {
	if vol.Spec.Provenance == zfscsiv1.VolumeProvenanceImported {
		return vol.Spec.BackendPath
	}
	return parsed.DatasetPath()
}

func (r *VolumeReconciler) reconcileImported(ctx context.Context, log logr.Logger, vol *zfscsiv1.Volume, parsed naming.ParsedVolID, dataset string) (reconcile.Result, error) {
	info, err := r.ZFS.Get(ctx, dataset)
	if err != nil {
		if errors.Is(err, zfs.ErrNotFound) {
			r.recordStatusWarning(ctx, vol, zfscsiv1.VolumeStateError, "imported backend is missing", volumeWarningEvent{reason: eventsv1.ReasonVolumeCreateFailed, action: eventsv1.ActionProvisioning, publicNote: "imported backend is missing"})
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("get imported backend %s: %w", dataset, err)
	}
	wantKind := zfs.KindBlock
	if vol.Spec.Type == zfscsiv1.VolumeTypeFilesystem {
		wantKind = zfs.KindFilesystem
	}
	if info.Kind != wantKind || info.Encrypted || info.Capacity < vol.Spec.Capacity {
		return reconcile.Result{}, fmt.Errorf("imported backend no longer satisfies validated intent")
	}
	if vol.Spec.Type == zfscsiv1.VolumeTypeFilesystem {
		if result, err := r.ensureFilesystemShared(ctx, log, vol, dataset); err != nil || result.RequeueAfter != 0 {
			return result, err
		}
	} else {
		if info.DevPath == "" {
			return reconcile.Result{}, fmt.Errorf("imported zvol has no device path")
		}
		if result, err := r.reconcileExport(ctx, log, vol, parsed, info.DevPath); err != nil || result.RequeueAfter != 0 {
			return result, err
		}
	}

	fresh := &zfscsiv1.Volume{}
	if err := r.Get(ctx, apimachinerytypes.NamespacedName{Name: vol.Name}, fresh); err != nil {
		return reconcile.Result{}, err
	}
	before := fresh.DeepCopy()
	fresh.Status.State = zfscsiv1.VolumeStateReady
	fresh.Status.ObservedGeneration = fresh.Generation
	fresh.Status.DatasetPath = dataset
	fresh.Status.ZvolPath = info.DevPath
	fresh.Status.ExportPath = info.ExportPath
	fresh.Status.ActualCapacity = info.Capacity
	// publishContextForVolume requires the owner endpoint materialized in
	// status. The dynamic path sets these in-memory before its own status
	// patch; this branch re-fetches `fresh`, so they must be written here or
	// ControllerPublishVolume fails closed for every imported volume.
	if vol.Spec.Type == zfscsiv1.VolumeTypeFilesystem {
		fresh.Status.NFSServer = r.NFSServer
		r.rootMu.Lock()
		fresh.Status.NFSRootPath = r.rootIdentity
		r.rootMu.Unlock()
	} else {
		fresh.Status.TargetNQN = vol.Status.TargetNQN
		fresh.Status.Portal = vol.Status.Portal
		fresh.Status.PortalHost = vol.Status.PortalHost
		fresh.Status.PortalPort = vol.Status.PortalPort
		fresh.Status.DeviceGUID = vol.Status.DeviceGUID
	}
	fresh.Status.Conditions = setCondition(fresh.Status.Conditions, fresh.Generation, string(zfscsiv1.VolumeConditionReady), metav1.ConditionTrue, eventsv1.ReasonVolumeReady, "imported backend validated and exported")
	if err := r.patchVolumeStatus(ctx, before, fresh, string(zfscsiv1.VolumeConditionReady)); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

func (r *VolumeReconciler) reconcileVolumeClone(ctx context.Context, log logr.Logger, vol *zfscsiv1.Volume,
	target naming.ParsedVolID, dataset, shareNFS string,
) (reconcile.Result, error) {
	source, err := naming.ParseVolID(vol.Spec.SourceVolumeID)
	if err != nil {
		r.recordStatusWarning(
			ctx,
			vol,
			zfscsiv1.VolumeStateError,
			"parse source volume id: "+err.Error(),
			volumeWarningEvent{
				reason: eventsv1.ReasonVolumeCloneFailed, action: eventsv1.ActionProvisioning, publicNote: "volume clone source is invalid",
			},
		)

		return reconcile.Result{}, nil
	}

	snapName := "clone-" + target.ID
	if len(snapName) > 63 {
		snapName = "clone-" + target.ID[:57]
	}

	snapshotOp := logging.LogWith(log, logging.OpZFSSnapshot, logging.KeyDataset, source.DatasetPath(), logging.KeySnapshot, snapName).
		Metric(metrics.ZFSOperationsTotal, "snapshot")
	if err := r.ZFS.Snapshot(ctx, source.DatasetPath(), snapName); err != nil && !errors.Is(err, zfs.ErrAlreadyExists) {
		snapshotOp.Failed(err)
		log.Error(err, "zfs source snapshot for clone failed")
		r.recordStatusWarning(
			ctx,
			vol,
			zfscsiv1.VolumeStateError,
			"zfs source snapshot for clone: "+err.Error(),
			volumeWarningEvent{
				reason: eventsv1.ReasonVolumeCloneFailed, action: eventsv1.ActionProvisioning, publicNote: "volume clone provisioning failed",
			},
		)

		return reconcile.Result{}, fmt.Errorf("snapshot source for clone: %w", err)
	}
	snapshotOp.OK()

	return r.cloneAndGrow(ctx, log, vol, source.DatasetPath(), snapName, dataset, shareNFS)
}

func (r *VolumeReconciler) reconcileSnapshotClone(
	ctx context.Context,
	log logr.Logger,
	vol *zfscsiv1.Volume,
	dataset, shareNFS string,
) (reconcile.Result, error) {
	source, snapName, err := naming.ParseSnapID(vol.Spec.SourceSnapshotID)
	if err != nil {
		r.recordStatusWarning(
			ctx,
			vol,
			zfscsiv1.VolumeStateError,
			"parse source snapshot id: "+err.Error(),
			volumeWarningEvent{
				reason: eventsv1.ReasonVolumeCloneFailed, action: eventsv1.ActionProvisioning, publicNote: "volume clone source is invalid",
			},
		)

		return reconcile.Result{}, nil
	}

	return r.cloneAndGrow(ctx, log, vol, source.DatasetPath(), snapName, dataset, shareNFS)
}

func (r *VolumeReconciler) cloneAndGrow(ctx context.Context, log logr.Logger, vol *zfscsiv1.Volume,
	sourceDataset, snapName, dataset, shareNFS string,
) (reconcile.Result, error) {
	op := logging.LogWith(log, logging.OpZFSClone, logging.KeyDataset, sourceDataset, logging.KeySnapshot, snapName, logging.KeyName, dataset).
		Metric(metrics.ZFSOperationsTotal, "clone")
	if err := r.ZFS.Clone(ctx, sourceDataset, snapName, dataset); err != nil && !errors.Is(err, zfs.ErrAlreadyExists) {
		op.Failed(err)
		log.Error(err, "zfs clone failed")
		r.recordStatusWarning(ctx, vol, zfscsiv1.VolumeStateError, "zfs clone: "+err.Error(), volumeWarningEvent{
			reason: eventsv1.ReasonVolumeCloneFailed, action: eventsv1.ActionProvisioning, publicNote: "volume clone provisioning failed",
		})

		return reconcile.Result{}, fmt.Errorf("clone dataset: %w", err)
	}
	op.OK()

	info, err := r.ZFS.Get(ctx, dataset)
	if err != nil {
		log.Error(err, "zfs get clone failed")
		r.recordStatusWarning(ctx, vol, zfscsiv1.VolumeStateError, "zfs get clone: "+err.Error(), volumeWarningEvent{
			reason: eventsv1.ReasonVolumeCloneFailed, action: eventsv1.ActionProvisioning, publicNote: "volume clone provisioning failed",
		})

		return reconcile.Result{}, fmt.Errorf("get cloned dataset: %w", err)
	}
	if info.Capacity < vol.Spec.Capacity {
		expandOp := logging.LogWith(log, logging.OpZFSExpand, logging.KeyDataset, dataset, logging.KeyCapacity, vol.Spec.Capacity, logging.KeyActualCapacity, info.Capacity).
			Metric(metrics.ZFSOperationsTotal, "expand")
		if err := r.ZFS.Expand(ctx, dataset, vol.Spec.Capacity); err != nil {
			expandOp.Failed(err)
			log.Error(err, "zfs expand clone failed")
			r.recordStatusWarning(
				ctx,
				vol,
				zfscsiv1.VolumeStateError,
				"zfs expand clone: "+err.Error(),
				volumeWarningEvent{
					reason: eventsv1.ReasonExpansionFailed, action: eventsv1.ActionExpanding, publicNote: "volume capacity expansion failed",
				},
			)

			return reconcile.Result{}, fmt.Errorf("expand cloned dataset: %w", err)
		}
		expandOp.OK()
	}

	// NFS clones must be mounted+shared: zfs_clone only reparents COW data, so
	// unlike Create (which sets sharenfs on the fresh dataset) the clone is never
	// exported and the consumer mount fails "No such file or directory". Share is
	// idempotent and a no-op for block (shareNFS empty). This is the clone-path
	// analogue of the Create-path mountAndShare.
	if shareNFS != "" {
		shareOp := logging.LogWith(log, logging.OpZFSShare, logging.KeyDataset, dataset).
			Metric(metrics.ZFSOperationsTotal, "share")
		if err := r.ZFS.Share(ctx, dataset, shareNFS); err != nil {
			shareOp.Failed(err)
			log.Error(err, "zfs share clone failed")
			r.recordStatusWarning(
				ctx,
				vol,
				zfscsiv1.VolumeStateError,
				"zfs share clone: "+err.Error(),
				volumeWarningEvent{
					reason: eventsv1.ReasonVolumeCloneFailed, action: eventsv1.ActionProvisioning, publicNote: "volume clone provisioning failed",
				},
			)

			return reconcile.Result{}, fmt.Errorf("share cloned dataset: %w", err)
		}
		shareOp.OK()
	}

	return reconcile.Result{}, nil
}

func (r *VolumeReconciler) reconcileExport(
	ctx context.Context,
	log logr.Logger,
	vol *zfscsiv1.Volume,
	p naming.ParsedVolID,
	zvolPath string,
) (reconcile.Result, error) {
	if r.Portal == "" {
		return reconcile.Result{}, fmt.Errorf("NVMe target portal is not configured")
	}
	nqn, err := naming.TargetNQN(vol.Spec.OwnerNode, vol.Spec.PoolGUID, p.Kind, p.ID)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("derive owner-qualified target NQN: %w", err)
	}
	deviceGUID, err := naming.DeviceGUID(vol.Spec.OwnerNode, vol.Spec.PoolGUID, p.Kind, p.ID)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("derive owner-qualified device GUID: %w", err)
	}
	transportKind := string(transport.Kind(vol.Spec.Transport))
	exportOp := logging.LogWith(log, logging.OpTransportExport, logging.KeyTargetNQN, nqn, logging.KeyZvol, zvolPath, logging.KeyTransport, transportKind).
		Metric(metrics.TransportOperationsTotal, transportKind, "export")
	ref, err := r.Export.Export(ctx, transport.ExportOptions{
		ZvolPath:   zvolPath,
		DeviceGUID: deviceGUID,
		TargetNQN:  nqn,
		Portal:     r.Portal,
		Kind:       transport.Kind(vol.Spec.Transport),
		TLS:        vol.Spec.NVMeTLSEnabled,
	})
	if err != nil && !errors.Is(err, transport.ErrAlreadyExported) {
		exportOp.Failed(err)
		log.Error(err, "transport export failed")
		if errors.Is(err, transport.ErrDeviceNotReady) {
			// The zvol has been created but udev has not surfaced its /dev node.
			// Keep the Volume pending and retry; marking Error makes external-
			// provisioner fail the PVC instead of allowing this known transient to heal.
			return reconcile.Result{RequeueAfter: time.Second}, nil
		}
		r.recordStatusWarning(ctx, vol, zfscsiv1.VolumeStateError, "export: "+err.Error(), volumeWarningEvent{
			reason: eventsv1.ReasonExportFailed, action: eventsv1.ActionExporting, publicNote: "volume transport export failed",
		})

		return reconcile.Result{}, fmt.Errorf("export dataset: %w", err)
	}
	exportOp.OK()

	before := vol.DeepCopy()
	after := vol.DeepCopy()
	if err := materializeTargetRefStatus(&after.Status, ref); err != nil {
		return reconcile.Result{}, fmt.Errorf("materialize exported target endpoint: %w", err)
	}
	statusOp := logging.LogWith(
		log,
		logging.OpPatchVolumeStatus,
		logging.KeyVolume,
		vol.Name,
		logging.KeyNamespace,
		vol.Namespace,
		logging.KeyTargetNQN,
		ref.TargetNQN,
	)
	if err := r.patchVolumeStatus(ctx, before, after); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		statusOp.Failed(err)

		return reconcile.Result{}, fmt.Errorf("patch volume target status %s/%s: %w", vol.Namespace, vol.Name, err)
	}
	statusOp.OK()
	vol.Status.TargetNQN = after.Status.TargetNQN
	vol.Status.Portal = after.Status.Portal
	vol.Status.PortalHost = after.Status.PortalHost
	vol.Status.PortalPort = after.Status.PortalPort
	vol.Status.DeviceGUID = after.Status.DeviceGUID

	return reconcile.Result{}, nil
}

func materializeTargetRefStatus(status *zfscsiv1.VolumeStatus, ref transport.TargetRef) error {
	host, port, err := reachability.ParsePortal(ref.Portal)
	if err != nil {
		return fmt.Errorf("parse returned portal: %w", err)
	}
	status.TargetNQN = ref.TargetNQN
	status.Portal = ref.Portal
	status.PortalHost = host
	status.PortalPort = port
	status.DeviceGUID = ref.DeviceGUID
	return nil
}

// reconcileEnsure is a level-triggered check on an already-Ready volume. It is
// the sole reboot-recovery path: after a storage-node reboot configfs is empty,
// NFS exports are gone, and encryption keys are unloaded, yet the Volume CR is
// still Ready and never re-enters reconcileCreate. Every ensure pass therefore
// re-applies whatever backing state may have been lost:
//
//   - dataset-exists check (gated on pool import — see below),
//   - encryption key reload (both block and filesystem),
//   - block: idempotent transport re-export + initiator-map drift reconcile,
//   - filesystem: idempotent NFS re-share (unconditional every pass).
//
// Ordering matters: key reload precedes mount/share/export because an encrypted
// dataset cannot be mounted or exported while its key is unavailable.
func (r *VolumeReconciler) reconcileEnsure(
	ctx context.Context,
	log logr.Logger,
	vol *zfscsiv1.Volume,
	p naming.ParsedVolID,
	dataset string,
) (reconcile.Result, error) {
	if vol.Spec.Provenance != zfscsiv1.VolumeProvenanceImported {
		if result, err := r.reconcileExpand(ctx, log, vol, dataset); err != nil || result.RequeueAfter != 0 {
			return result, err
		}

		if err := r.reconcileCompression(ctx, log, vol, dataset); err != nil {
			return reconcile.Result{}, err
		}
	}

	// Dataset-exists check applies to BOTH volume types (F3+F4). A vanished
	// dataset means either the backing store was destroyed OR — the far more
	// common case after a storage-node reboot — the pool has not been imported
	// yet (zpool import runs asynchronously via zfs-import-cache.service). We
	// MUST NOT treat "pool not imported yet" as "dataset destroyed": doing so
	// would drive reconcileCreate, silently creating a fresh EMPTY dataset and
	// masking the real data. Only transition to Pending when the pool is
	// confirmed imported; otherwise hold Ready and requeue with backoff.
	exists, err := r.ZFS.Exists(ctx, dataset)
	if err != nil {
		// Transient libzfs failure — do NOT interpret as "dataset gone". Preserve
		// current state and back off.
		log.Error(err, "dataset exists check failed during ensure", logging.KeyDataset, dataset)
		if healthErr := r.recordBackendHealthWarning(ctx, vol, "check dataset: "+err.Error()); healthErr != nil {
			return reconcile.Result{}, errors.Join(
				fmt.Errorf("dataset exists check during ensure: %w", err), healthErr,
			)
		}

		return reconcile.Result{}, fmt.Errorf("dataset exists check during ensure: %w", err)
	}
	if !exists {
		poolImported, poolErr := r.isPoolImported(ctx, p.Pool)
		if poolErr != nil {
			// We cannot determine whether recreation is safe. Return the error so
			// controller-runtime applies rate-limited backoff rather than a fixed
			// delay intended only for a known-unimported pool.
			log.Error(poolErr, "pool import check failed during ensure", logging.KeyPool, p.Pool)

			return reconcile.Result{}, fmt.Errorf("pool import check during ensure: %w", poolErr)
		}
		if !poolImported {
			// Pool absent or not yet imported: the dataset's absence is not
			// authoritative. Never recreate. Requeue and let the pool come up.
			log.Info("dataset absent but pool not imported; holding Ready and requeuing",
				logging.KeyPool, p.Pool, logging.KeyDataset, dataset)

			if err := r.recordBackendHealthWarning(ctx, vol, "backing dataset unavailable while pool is not imported"); err != nil {
				return reconcile.Result{}, err
			}

			return reconcile.Result{RequeueAfter: poolNotImportedRequeue}, nil
		}
		// Conditions are an RFC 7386-replaced array. Re-fetch and retry so this
		// transition cannot erase a concurrent Ready or BackendHealthy update.
		if err := r.recordDatasetMissing(ctx, vol); err != nil {
			return reconcile.Result{}, err
		}

		return reconcile.Result{RequeueAfter: time.Second}, nil
	}
	// Reload key if encrypted + unavailable (post-reboot). Applies to both block
	// and filesystem volumes; MUST run before mount/share/export below.
	if err := r.ensureKeyLoaded(ctx, log, vol, dataset); err != nil {
		if healthErr := r.recordBackendHealthWarning(ctx, vol, "reload encryption key: "+err.Error()); healthErr != nil {
			return reconcile.Result{}, errors.Join(
				fmt.Errorf("reload encryption key during ensure: %w", err), healthErr,
			)
		}

		return reconcile.Result{}, fmt.Errorf("reload encryption key during ensure: %w", err)
	}

	if vol.Spec.Type != zfscsiv1.VolumeTypeBlock {
		// Filesystem: re-ensure the NFS export on every pass so a reboot that
		// wiped the kernel export is healed. Share is idempotent (Backend.Share
		// guards zfs_mount with zfs_is_mounted and re-runs the sharenfs changelist
		// on a fresh handle), so this is called unconditionally — mirroring the
		// block re-export path below.
		if result, err := r.ensureFilesystemShared(ctx, log, vol, dataset); err != nil || result.RequeueAfter != 0 {
			return result, err
		}
		// Heal status written by older owners or drifted state. ExportPath stays
		// the authoritative backend mountpoint; NFSRootPath must match the root
		// that just passed preflight. Ready state is preserved.
		exportPath, err := r.mountedFilesystemPath(ctx, dataset)
		if err != nil {
			return reconcile.Result{}, err
		}
		r.rootMu.Lock()
		rootPath := r.rootIdentity
		r.rootMu.Unlock()
		if vol.Status.ExportPath == "" || vol.Status.NFSRootPath != rootPath {
			before := vol.DeepCopy()
			if vol.Status.ExportPath == "" {
				vol.Status.ExportPath = exportPath
			}
			vol.Status.NFSRootPath = rootPath
			if err := r.patchVolumeStatus(ctx, before, vol); err != nil {
				return reconcile.Result{}, fmt.Errorf("materialize NFS export status %s/%s: %w", vol.Namespace, vol.Name, err)
			}
		}
		if err := r.recordBackendHealthy(ctx, vol); err != nil {
			return reconcile.Result{}, err
		}

		return r.patchObservedGeneration(ctx, log, vol)
	}
	// Ensure export exists. Some transports cannot reliably distinguish an empty
	// allow-list from a missing target after reboot, so re-apply export every pass.
	nqn, err := naming.TargetNQN(vol.Spec.OwnerNode, vol.Spec.PoolGUID, p.Kind, p.ID)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("derive owner-qualified target NQN: %w", err)
	}
	zvolPath := "/dev/zvol/" + dataset
	if vol.Spec.Provenance == zfscsiv1.VolumeProvenanceImported {
		info, getErr := r.ZFS.Get(ctx, dataset)
		if getErr != nil {
			return reconcile.Result{}, fmt.Errorf("get imported zvol device path: %w", getErr)
		}
		zvolPath = info.DevPath
	}
	deviceGUID := vol.Status.DeviceGUID
	if deviceGUID == "" {
		deviceGUID, err = naming.DeviceGUID(vol.Spec.OwnerNode, vol.Spec.PoolGUID, p.Kind, p.ID)
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("derive owner-qualified device GUID: %w", err)
		}
	}
	ref := transport.TargetRef{
		Kind: transport.Kind(vol.Spec.Transport), TargetNQN: nqn, Portal: r.Portal,
		NamespaceID: targetNamespaceID, DeviceGUID: deviceGUID, TLS: vol.Spec.NVMeTLSEnabled,
	}
	// Probe before repair. A missing configfs target must become durable health
	// state before Export recreates it, otherwise controller health polling can
	// race the repair and report a false healthy result.
	mapped, mapErr := r.Export.MappedInitiators(ctx, ref)
	targetMissing := errors.Is(mapErr, transport.ErrNotExported)
	if mapErr != nil && !targetMissing {
		log.Error(mapErr, "transport mapped initiators lookup failed")
		if healthErr := r.recordBackendHealthWarning(ctx, vol, "inspect target mappings: "+mapErr.Error()); healthErr != nil {
			return reconcile.Result{}, errors.Join(
				fmt.Errorf("mapped initiators lookup during ensure: %w", mapErr), healthErr,
			)
		}
		return reconcile.Result{}, fmt.Errorf("mapped initiators lookup during ensure: %w", mapErr)
	}
	if targetMissing {
		if err := r.recordBackendHealthWarning(ctx, vol, "target export is missing"); err != nil {
			return reconcile.Result{}, err
		}
		// The E2E fault scenario holds repair only after durable status and Event
		// publication. Production volumes never carry this test-only annotation.
		if r.healthRepairHoldEnabled(vol) {
			return reconcile.Result{RequeueAfter: time.Minute}, nil
		}
	}
	if vol.Spec.NVMeTLSEnabled {
		if err := r.ensureNVMeTLSPSK(ctx, vol.Spec.NVMeTLSPSKSecretName, nqn, desiredMappedInitiatorIDs(vol.Status.MappedInitiators)); err != nil {
			return reconcile.Result{}, fmt.Errorf("prepare NVMe TLS PSK during ensure: %w", err)
		}
	}

	exportOp := logging.LogWith(log, logging.OpTransportExport, logging.KeyTargetNQN, nqn, logging.KeyZvol, zvolPath, logging.KeyTransport, string(transport.Kind(vol.Spec.Transport))).
		Metric(metrics.TransportOperationsTotal, string(transport.Kind(vol.Spec.Transport)), "export")
	exportedRef, err := r.Export.Export(ctx, transport.ExportOptions{
		ZvolPath:   zvolPath,
		DeviceGUID: deviceGUID,
		TargetNQN:  nqn,
		Portal:     r.Portal,
		Kind:       transport.Kind(vol.Spec.Transport),
		TLS:        vol.Spec.NVMeTLSEnabled,
	})
	if err != nil && !errors.Is(err, transport.ErrAlreadyExported) {
		exportOp.Failed(err)
		log.Error(err, "transport export failed during ensure")
		if healthErr := r.recordBackendHealthWarning(ctx, vol, "repair target export: "+err.Error()); healthErr != nil {
			return reconcile.Result{}, errors.Join(
				fmt.Errorf("transport export during ensure: %w", err), healthErr,
			)
		}

		return reconcile.Result{}, fmt.Errorf("transport export during ensure: %w", err)
	}
	exportOp.OK()

	if exportedRef.TargetNQN != "" {
		ref = exportedRef
	}
	beforeExportStatus := vol.DeepCopy()
	if err := materializeTargetRefStatus(&vol.Status, exportedRef); err != nil {
		return reconcile.Result{}, fmt.Errorf("materialize ensured target endpoint: %w", err)
	}
	if err := r.patchVolumeStatus(ctx, beforeExportStatus, vol); err != nil {
		return reconcile.Result{}, fmt.Errorf("patch ensured target status %s/%s: %w", vol.Namespace, vol.Name, err)
	}
	transportKind := string(ref.Kind)

	// Reconcile drift: ensure every status.mappedInitiator is allowed; remove orphans.
	desired := map[string]string{}
	for _, m := range vol.Status.MappedInitiators {
		desired[m.InitiatorID] = m.InitiatorID
	}

	// Track whether we unmapped a live initiator that is no longer desired. This
	// signals a single-writer failover replacement ([A]->[B]) and, together with
	// a single desired initiator, is the trigger for fencing (F1, applied by the
	// caller after MapInitiator so the incoming initiator is admitted first).
	orphanUnmapped := false
	for _, live := range mapped {
		if _, ok := desired[live]; !ok {
			op := logging.LogWith(log, logging.OpTransportUnmapInitiator, logging.KeyTargetNQN, nqn, logging.KeyInitiatorID, live, logging.KeyTransport, transportKind).
				Metric(metrics.TransportOperationsTotal, transportKind, "unmap")
			if err := r.Export.UnmapInitiator(ctx, ref, live); err != nil {
				op.Failed(err)
			} else {
				op.OK()
				orphanUnmapped = true
			}
		}
	}

	// Build the confirmed set: desired initiators whose MapInitiator succeeded
	// plus live mappings that are still desired. The agent is the SOLE writer
	// of status.publishedInitiators.
	published := make([]string, 0, len(desired))
	for _, live := range mapped {
		if _, ok := desired[live]; ok {
			published = append(published, live)
		}
	}
	var mappingErr error
	for _, d := range desired {
		op := logging.LogWith(log, logging.OpTransportMapInitiator, logging.KeyTargetNQN, nqn, logging.KeyInitiatorID, d, logging.KeyTransport, transportKind).
			Metric(metrics.TransportOperationsTotal, transportKind, "map")
		if err := r.Export.MapInitiator(ctx, ref, d); err != nil {
			op.Failed(err)
			if healthErr := r.recordBackendHealthWarning(ctx, vol, "repair target mapping: "+err.Error()); healthErr != nil {
				return reconcile.Result{}, healthErr
			}
			if mappingErr == nil {
				mappingErr = fmt.Errorf("map initiator %s: %w", d, err)
			}
		} else {
			published = append(published, d)
			op.OK()
		}
	}

	// Fence stale controllers (F1). A single-writer failover replaces the
	// allow-list [A]->[B]; unmapping A from the allow-list does NOT terminate
	// A's established NVMe controller, so a zombie node A keeps writing to the
	// namespace B just mounted -> split-brain corruption. ForceDisconnect drops
	// ALL controllers of the subsystem; the legitimate initiator B (already
	// re-admitted above) reconnects within reconnect_delay, A is rejected by the
	// allow-list.
	//
	// Scope precisely to a genuine single-writer REPLACEMENT, not merely
	// len(desired)==1: a multi-node (ROX/RWX) volume scaled down [A,B]->[A] also
	// has len(desired)==1 with an orphan unmap (B), but its surviving initiator A
	// was already live and must NOT be bounced (dropping A's controllers is a
	// spurious I/O stall for a legitimate co-tenant). The distinguishing signal
	// is that in a real failover the single desired initiator is NEW — it was not
	// in the prior live set. [A]->[B]: desired={B}, B was not live -> fence.
	// [A,B]->[A]: desired={A}, A was live -> subset shrink, no fence.
	fenced := false
	if orphanUnmapped && len(desired) == 1 && fenceIsReplacement(desired, mapped) {
		fenceOp := logging.LogWith(log, logging.OpTransportForceDisconnect, logging.KeyTargetNQN, nqn, logging.KeyTransport, transportKind).
			Metric(metrics.TransportOperationsTotal, transportKind, "fence")
		if err := r.Export.ForceDisconnect(ctx, ref); err != nil {
			fenceOp.Failed(err)
		} else {
			fenceOp.OK()
			fenced = true
		}
	}

	published = dedupStrings(published)
	if !slicesEqual(vol.Status.PublishedInitiators, published) || fenced {
		if err := r.patchPublishedInitiators(ctx, vol, published, fenced); err != nil {
			return reconcile.Result{}, err
		}
	}

	if mappingErr != nil {
		// F1 above intentionally runs even when B could not be mapped: A has
		// already been removed from the allow-list and must not remain a writer.
		return reconcile.Result{}, mappingErr
	}
	if targetMissing {
		// Repair succeeded, but leave the persisted Warning observable until the
		// next pass verifies the replacement target and records recovery.
		return reconcile.Result{RequeueAfter: time.Second}, nil
	}
	// A warning may have been persisted by an earlier ensure pass. Fetch the
	// latest object before recording recovery so a stale reconcile copy cannot
	// miss the BackendHealthy=False transition.
	latest := &zfscsiv1.Volume{}
	if r.Get(ctx, apimachinerytypes.NamespacedName{Name: vol.Name}, latest) == nil {
		vol = latest
	}
	if err := r.recordBackendHealthy(ctx, vol); err != nil {
		return reconcile.Result{}, err
	}
	return r.patchObservedGeneration(ctx, log, vol)
}

// fenceIsReplacement reports whether the single desired initiator is a genuine
// failover replacement (absent from the prior live set) rather than the survivor
// of a multi-node scale-down. Caller guarantees len(desired)==1.
func fenceIsReplacement(desired map[string]string, priorLive []string) bool {
	for id := range desired {
		if slices.Contains(priorLive, id) {
			// The single desired initiator was already live -> subset shrink.
			return false
		}
	}

	return true
}

// isPoolImported reports whether the named ZFS pool is currently imported on
// this host. Used to distinguish a genuinely destroyed dataset from one that is
// simply not visible yet because the pool has not been imported after a reboot.
func (r *VolumeReconciler) isPoolImported(ctx context.Context, pool string) (bool, error) {
	names, err := r.ZFS.PoolNames(ctx)
	if err != nil {
		return false, err
	}

	return slices.Contains(names, pool), nil
}

// ensureKeyLoaded reloads the ZFS native-encryption key for an encrypted dataset
// whose key is currently unavailable (post-reboot). No-op for unencrypted
// volumes or when the key is already loaded. Returns an error only when a fetch/
// stage/load step fails so the caller can back off; the key simply staying
// unavailable (fetch failure) is surfaced via the returned error.
func (r *VolumeReconciler) ensureKeyLoaded(
	ctx context.Context,
	log logr.Logger,
	vol *zfscsiv1.Volume,
	dataset string,
) error {
	if vol.Spec.EncryptionKeyRef == "" {
		return nil
	}
	ks, err := r.ZFS.KeyStatus(ctx, dataset)
	if err != nil {
		log.Error(err, "keystatus check failed during ensure", logging.KeyDataset, dataset)

		return err
	}
	if ks != zfs.KeyUnavailable {
		return nil
	}

	fetchOp := logging.LogWith(log, logging.OpCryptoFetch, logging.KeyKeyRef, vol.Spec.EncryptionKeyRef).
		Metric(metrics.CryptoOperationsTotal, "fetch")
	rawKey, err := r.Keys.Fetch(ctx, vol.Spec.EncryptionKeyRef)
	if err != nil {
		fetchOp.Failed(err)

		return err
	}
	fetchOp.OK()

	stageOp := logging.LogWith(log, logging.OpCryptoStage, logging.KeyVolumeID, vol.Spec.VolumeID).
		Metric(metrics.CryptoOperationsTotal, "stage")
	loc, path, err := r.Stager.Stage(vol.Spec.VolumeID, rawKey)
	if err != nil {
		stageOp.Failed(err)

		return err
	}
	stageOp.OK()

	loadOp := logging.LogWith(log, logging.OpZFSLoadKey, logging.KeyDataset, dataset).
		Metric(metrics.ZFSOperationsTotal, "loadkey")
	loadErr := r.ZFS.LoadKey(ctx, dataset, loc)
	if loadErr != nil {
		loadOp.Failed(loadErr)
	} else {
		loadOp.OK()
		vol.Status.KeyStatus = zfscsiv1.KeyStatusAvailable
	}

	shredOp := logging.LogWith(log, logging.OpCryptoShred, logging.KeyPath, path).
		Metric(metrics.CryptoOperationsTotal, "shred")
	if err := r.Stager.Shred(path); err != nil {
		shredOp.Failed(err)
	} else {
		shredOp.OK()
	}

	return loadErr
}

// ensureFilesystemShared re-applies the NFS export for a filesystem volume on
// every ensure pass (reboot recovery). The in-process nfsd responder is the sole
// export mechanism: the dataset is mounted with sharenfs=off (libshare exports
// nothing) and the responder answers the nfsd cache-channel upcalls, emitting
// xprtsec=mtls only for TLS volumes. Backend.Share/ShareImported is idempotent
// on an already-mounted dataset (it guards zfs_mount with zfs_is_mounted), so
// this safely runs unconditionally to heal a reboot that wiped the kernel
// mount/export cache — mirroring the block re-export path.
func (r *VolumeReconciler) ensureFilesystemShared(
	ctx context.Context,
	log logr.Logger,
	vol *zfscsiv1.Volume,
	dataset string,
) (reconcile.Result, error) {
	// Mount with sharenfs=off (no libshare export) on every pass so a reboot
	// that wiped the kernel mount/export state is healed. Deliberately
	// unconditional: there is no reliable cheap "currently kernel-exported?"
	// signal (the sharenfs property persists across reboot and so cannot
	// distinguish "still exported" from "property set but export lost";
	// zfs_is_shared has a version-divergent cgo signature we avoid binding).
	shareOp := logging.LogWith(log, logging.OpZFSShare, logging.KeyDataset, dataset).
		Metric(metrics.ZFSOperationsTotal, "share")
	share := r.ZFS.Share
	if vol.Spec.Provenance == zfscsiv1.VolumeProvenanceImported {
		share = r.ZFS.ShareImported
	}
	if err := share(ctx, dataset, "off"); err != nil {
		shareOp.Failed(err)
		if healthErr := r.recordBackendHealthWarning(ctx, vol, "repair filesystem export: "+err.Error()); healthErr != nil {
			return reconcile.Result{}, errors.Join(
				fmt.Errorf("re-share filesystem dataset: %w", err), healthErr,
			)
		}

		return reconcile.Result{}, fmt.Errorf("re-share filesystem dataset: %w", err)
	}
	shareOp.OK()

	// Resolve the live backend mountpoint after Share. Never synthesize
	// "/"+dataset: custom ZFS mountpoints must remain authoritative end-to-end.
	exportPath, err := r.mountedFilesystemPath(ctx, dataset)
	if err != nil {
		return reconcile.Result{}, err
	}
	if err := r.registerNFSExportCtx(ctx, vol, dataset, exportPath); err != nil {
		if isRootPreflightRetryable(err) || errors.Is(err, errRootPreflightTerminalConfig) || errors.Is(err, errRootPreflightTerminalDeploy) {
			return r.handleRootPreflightError(ctx, vol, err)
		}
		var configErr *nfsExportConfigError
		if !errors.As(err, &configErr) {
			return reconcile.Result{}, err
		}
		r.recordStatusWarning(ctx, vol, zfscsiv1.VolumeStateError, "register NFS export: "+err.Error(), volumeWarningEvent{
			reason: eventsv1.ReasonExportFailed, action: eventsv1.ActionExporting, publicNote: "volume export configuration is invalid",
		})

		return reconcile.Result{}, nil
	}

	return reconcile.Result{}, nil
}

func (r *VolumeReconciler) patchObservedGeneration(
	ctx context.Context,
	log logr.Logger,
	vol *zfscsiv1.Volume,
) (reconcile.Result, error) {
	if vol.Status.ObservedGeneration == vol.Generation {
		return reconcile.Result{}, nil
	}

	before := vol.DeepCopy()
	after := vol.DeepCopy()
	after.Status.ObservedGeneration = after.Generation
	statusOp := logging.LogWith(
		log,
		logging.OpPatchVolumeStatus,
		logging.KeyVolume,
		vol.Name,
		logging.KeyNamespace,
		vol.Namespace,
	)
	if err := r.patchVolumeStatus(ctx, before, after); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		statusOp.Failed(err)

		return reconcile.Result{}, fmt.Errorf(
			"patch volume observed generation %s/%s: %w",
			vol.Namespace,
			vol.Name,
			err,
		)
	}
	statusOp.OK()

	return reconcile.Result{}, nil
}

func dedupStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}

		seen[s] = struct{}{}
		out = append(out, s)
	}

	return out
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

// reconcileCompression applies a compression change (from ControllerModifyVolume
// / VolumeAttributesClass) to the live dataset. Level-triggered + idempotent:
// reads the current property and only issues `zfs set compression=` when it
// drifts from Spec.Compression. Empty Spec.Compression means "inherit / don't
// manage" — we never force it back. A transient set failure returns an error for
// backoff; it does not block the rest of the ensure path from a fresh reconcile.
func (r *VolumeReconciler) reconcileCompression(
	ctx context.Context,
	log logr.Logger,
	vol *zfscsiv1.Volume,
	dataset string,
) error {
	want := vol.Spec.Compression
	if want == "" {
		return nil
	}

	cur, err := r.ZFS.GetProperty(ctx, dataset, "compression")
	if err != nil {
		// Dataset may not be materialised yet on this pass; a later reconcile
		// retries. Don't fail the whole ensure for a read miss.
		return nil
	}
	if strings.EqualFold(strings.TrimSpace(cur), want) {
		return nil
	}

	setOp := logging.LogWith(log, logging.OpZFSSetProperty, logging.KeyDataset, dataset, logging.KeyCompression, want).
		Metric(metrics.ZFSOperationsTotal, "setprop")
	if err := r.ZFS.SetProperty(ctx, dataset, "compression", want); err != nil {
		setOp.Failed(err)

		return fmt.Errorf("set compression=%s on %s: %w", want, dataset, err)
	}
	setOp.OK()

	return nil
}

func (r *VolumeReconciler) reconcileExpand(
	ctx context.Context,
	log logr.Logger,
	vol *zfscsiv1.Volume,
	dataset string,
) (reconcile.Result, error) {
	if vol.Status.ActualCapacity == 0 {
		before := vol.DeepCopy()
		vol.Status.ActualCapacity = vol.Spec.Capacity
		vol.Status.ObservedGeneration = vol.Generation
		statusOp := logging.LogWith(log, logging.OpPatchVolumeStatus,
			logging.KeyVolume, vol.Name,
			logging.KeyNamespace, vol.Namespace,
			logging.KeyCapacity, vol.Spec.Capacity)
		if err := r.patchVolumeStatus(ctx, before, vol); err != nil {
			statusOp.Failed(err)

			return reconcile.Result{}, fmt.Errorf(
				"patch initial actual capacity %s/%s: %w",
				vol.Namespace,
				vol.Name,
				err,
			)
		}
		statusOp.OK()

		return reconcile.Result{}, nil
	}

	if vol.Spec.Capacity <= vol.Status.ActualCapacity {
		return reconcile.Result{}, nil
	}

	expandOp := logging.LogWith(log, logging.OpZFSExpand,
		logging.KeyDataset, dataset,
		logging.KeyCapacity, vol.Spec.Capacity,
		logging.KeyActualCapacity, vol.Status.ActualCapacity).
		Metric(metrics.ZFSOperationsTotal, "expand")
	if err := r.ZFS.Expand(ctx, dataset, vol.Spec.Capacity); err != nil {
		expandOp.Failed(err)
		log.Error(err, "zfs expand failed")
		r.recordStatusWarning(ctx, vol, zfscsiv1.VolumeStateError, "zfs expand: "+err.Error(), volumeWarningEvent{
			reason: eventsv1.ReasonExpansionFailed, action: eventsv1.ActionExpanding, publicNote: "volume capacity expansion failed",
		})

		return reconcile.Result{}, fmt.Errorf("expand dataset: %w", err)
	}
	expandOp.OK()

	previousReadyStatus, previousReadyReason := volumeReadyCondition(vol.Status.Conditions)
	before := vol.DeepCopy()
	vol.Status.ActualCapacity = vol.Spec.Capacity
	vol.Status.ObservedGeneration = vol.Generation
	vol.Status.Conditions = setCondition(vol.Status.Conditions, vol.Generation,
		string(zfscsiv1.VolumeConditionReady), metav1.ConditionTrue,
		eventsv1.ReasonVolumeExpanded, "volume capacity expanded")
	statusOp := logging.LogWith(log, logging.OpPatchVolumeStatus,
		logging.KeyVolume, vol.Name,
		logging.KeyNamespace, vol.Namespace,
		logging.KeyActualCapacity, vol.Status.ActualCapacity)
	if err := r.patchVolumeStatus(ctx, before, vol, string(zfscsiv1.VolumeConditionReady)); err != nil {
		statusOp.Failed(err)

		return reconcile.Result{}, fmt.Errorf("patch expanded actual capacity %s/%s: %w", vol.Namespace, vol.Name, err)
	}
	statusOp.OK()
	if readyConditionChanged(previousReadyStatus, previousReadyReason, vol.Status.Conditions) {
		r.recordEvent(vol, eventsv1.TypeNormal, eventsv1.ReasonVolumeExpanded,
			eventsv1.ActionExpanding, "volume capacity expansion completed")
	}

	return reconcile.Result{}, nil
}

// reconcileDelete destroys the dataset, unexports the target, crypto-shreds.
func (r *VolumeReconciler) reconcileDelete(
	ctx context.Context,
	log logr.Logger,
	vol *zfscsiv1.Volume,
	p naming.ParsedVolID,
	dataset string,
) (reconcile.Result, error) {
	// In-use guard (F6): do NOT unexport/destroy while the volume is still mapped
	// to a node — an out-of-band CR delete (one that bypassed the controller's
	// DeleteVolume guard) must not rip the target out from under live I/O. The
	// controller normally blocks this, so a mapped-yet-deleting volume here is a
	// stale VolumeAttachment or manual delete. Requeue at low priority (so it
	// cannot crowd out provisioning) and emit a Warning naming the node. The
	// operator escape is the force-delete annotation, which clears the guard.
	if len(vol.Status.MappedInitiators) > 0 && vol.Annotations[zfscsiv1.ForceDeleteAnnotation] != "true" {
		nodes := make([]string, 0, len(vol.Status.MappedInitiators))
		for _, m := range vol.Status.MappedInitiators {
			nodes = append(nodes, m.NodeName)
		}
		log.Info("refusing to delete volume still mapped to node(s); requeuing",
			logging.KeyDataset, dataset, "mappedNodes", nodes,
			"forceAnnotation", zfscsiv1.ForceDeleteAnnotation)
		r.recordStatusWarning(ctx, vol, zfscsiv1.VolumeStateDeleting, "volume is still published", volumeWarningEvent{
			reason: eventsv1.ReasonDeleteBlockedInUse, action: eventsv1.ActionDeleting, publicNote: "volume deletion is blocked while it remains published",
		})

		return reconcile.Result{Priority: new(handler.LowPriority)}, nil
	}

	// Unexport first.
	if vol.Spec.Type == zfscsiv1.VolumeTypeBlock {
		nqn, err := naming.TargetNQN(vol.Spec.OwnerNode, vol.Spec.PoolGUID, p.Kind, p.ID)
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("derive owner-qualified target NQN: %w", err)
		}
		ref := transport.TargetRef{
			Kind:        transport.Kind(vol.Spec.Transport),
			TargetNQN:   nqn,
			Portal:      r.Portal,
			NamespaceID: 1,
			TLS:         vol.Spec.NVMeTLSEnabled,
		}
		transportKind := string(ref.Kind)
		op := logging.LogWith(log, logging.OpTransportUnexport, logging.KeyTargetNQN, nqn, logging.KeyTransport, transportKind).
			Metric(metrics.TransportOperationsTotal, transportKind, "unexport")
		if err := r.Export.Unexport(ctx, ref); err != nil {
			op.Failed(err)

			// Leave backend, finalizer, and TLS credentials intact while target
			// teardown remains incomplete; retry deletion through reconciliation.
			return reconcile.Result{}, fmt.Errorf("unexport target %s: %w", nqn, err)
		} else {
			op.OK()
			if vol.Spec.NVMeTLSEnabled {
				r.revokeNVMeTLSPSK(ctx, vol.Spec.NVMeTLSPSKSecretName, nqn, desiredMappedInitiatorIDs(vol.Status.MappedInitiators))
			}
		}
	} else if vol.Spec.Provenance == zfscsiv1.VolumeProvenanceImported {
		if err := r.ZFS.Unshare(ctx, dataset); err != nil && !errors.Is(err, zfs.ErrNotFound) {
			return reconcile.Result{}, fmt.Errorf("unshare imported filesystem %s: %w", dataset, err)
		}
	}
	// Withdraw the in-process nfsd responder export for filesystem volumes so a
	// destroyed/de-adopted dataset stops resolving (and stale cache entries are
	// flushed). No-op for block volumes. Runs after backend unshare so the
	// responder export is the last thing withdrawn before destroy/de-adoption.
	if err := r.withdrawNFSExport(ctx, log, vol, dataset); err != nil {
		// Withdrawal is a deletion safety boundary: never destroy or de-adopt
		// while stale kernel state may still resolve the backend.
		return reconcile.Result{}, fmt.Errorf("withdraw NFS export: %w", err)
	}
	if vol.Spec.Provenance == zfscsiv1.VolumeProvenanceImported || vol.Spec.DeletionPolicy == zfscsiv1.VolumeDeletionPolicyRetain {
		before := vol.DeepCopy()
		vol.Status.State = zfscsiv1.VolumeStateDestroyed
		vol.Status.ObservedGeneration = vol.Generation
		vol.Status.Conditions = setCondition(vol.Status.Conditions, vol.Generation, string(zfscsiv1.VolumeConditionReady), metav1.ConditionFalse, eventsv1.ReasonVolumeDestroyed, "imported volume de-adopted; backend retained")
		if err := r.patchVolumeStatus(ctx, before, vol, string(zfscsiv1.VolumeConditionReady)); err != nil && !apierrors.IsNotFound(err) {
			return reconcile.Result{}, err
		}
		if hasFinalizer(vol.Finalizers, zfscsiv1.VolumeFinalizer) {
			patch := crclient.MergeFrom(vol.DeepCopy())
			removeFinalizer(&vol.Finalizers, zfscsiv1.VolumeFinalizer)
			if err := r.Patch(ctx, vol, patch); err != nil && !apierrors.IsNotFound(err) {
				return reconcile.Result{}, err
			}
		}
		r.clearNFSWithdrawal(dataset)
		return reconcile.Result{}, nil
	}
	// Unload key (if loaded) then destroy.
	if vol.Spec.EncryptionKeyRef != "" {
		op := logging.LogWith(log, logging.OpZFSUnloadKey, logging.KeyDataset, dataset).
			Metric(metrics.ZFSOperationsTotal, "unloadkey")
		if err := r.ZFS.UnloadKey(ctx, dataset); err != nil {
			op.Failed(err)
		} else {
			op.OK()
		}
	}

	destroyOp := logging.LogWith(log, logging.OpZFSDestroy, logging.KeyDataset, dataset).
		Metric(metrics.ZFSOperationsTotal, "destroy")
	if err := r.ZFS.Destroy(ctx, dataset); err != nil && !errors.Is(err, zfs.ErrNotFound) {
		destroyOp.Failed(err)
		r.recordStatusWarning(
			ctx,
			vol,
			zfscsiv1.VolumeStateDeleting,
			"destroy dataset: "+err.Error(),
			volumeWarningEvent{
				reason: eventsv1.ReasonVolumeDeleteFailed, action: eventsv1.ActionDeleting, publicNote: "volume deletion failed",
			},
		)

		// A failing destroy (e.g. a dataset with dependent snapshots/clones that
		// can never be destroyed until the dependent is gone) must NOT starve
		// provisioning. Return the error for rate-limited exponential backoff AND
		// drop the retry to LowPriority so the priority queue services fresh
		// Volume-create events ahead of a doomed-delete storm. Without an explicit
		// Priority the requeue keeps the request's default priority and competes
		// 1:1 with creates — exactly the starvation observed under conformance.
		return reconcile.Result{Priority: new(handler.LowPriority)}, fmt.Errorf("destroy dataset %s: %w", dataset, err)
	}
	destroyOp.OK()
	// Crypto-shred the DEK.
	if vol.Spec.EncryptionKeyRef != "" {
		op := logging.LogWith(log, logging.OpCryptoDelete, logging.KeyKeyRef, vol.Spec.EncryptionKeyRef).
			Metric(metrics.CryptoOperationsTotal, "delete")
		if err := r.Keys.Delete(ctx, vol.Spec.EncryptionKeyRef); err != nil {
			op.Failed(err)
			r.recordStatusWarning(
				ctx,
				vol,
				zfscsiv1.VolumeStateDeleting,
				"crypto-shred key: "+err.Error(),
				volumeWarningEvent{
					reason: eventsv1.ReasonVolumeDeleteFailed, action: eventsv1.ActionDeleting, publicNote: "volume deletion failed",
				},
			)

			// Same as the destroy failure above: low-priority backoff so a stuck
			// crypto-shred can't crowd out provisioning.
			return reconcile.Result{
					Priority: new(handler.LowPriority),
				}, fmt.Errorf(
					"crypto-shred DEK %s: %w",
					vol.Spec.EncryptionKeyRef,
					err,
				)
		}
		op.OK()
	}
	// Delete-policy PSK cleanup runs only after irreversible data + key removal.
	if result, err := r.deleteNVMeTLSPSKSecret(ctx, vol); err != nil {
		return result, err
	}
	// Mark destroyed + let the CR be collected.
	previousReadyStatus, previousReadyReason := volumeReadyCondition(vol.Status.Conditions)
	before := vol.DeepCopy()
	vol.Status.State = zfscsiv1.VolumeStateDestroyed
	vol.Status.ObservedGeneration = vol.Generation
	vol.Status.Conditions = setCondition(vol.Status.Conditions, vol.Generation,
		string(zfscsiv1.VolumeConditionReady), metav1.ConditionFalse,
		eventsv1.ReasonVolumeDestroyed, "volume destroyed")
	statusOp := logging.LogWith(log, logging.OpPatchVolumeStatus,
		logging.KeyVolume, vol.Name,
		logging.KeyNamespace, vol.Namespace,
		logging.KeyState, zfscsiv1.VolumeStateDestroyed)
	if err := r.patchVolumeStatus(ctx, before, vol, string(zfscsiv1.VolumeConditionReady)); err != nil {
		if apierrors.IsNotFound(err) {
			// A concurrent finalizer removal may have let Kubernetes collect the CR.
			// Its already-destroyed backend state makes this an idempotent success.
			statusOp.OK()

			return reconcile.Result{}, nil
		}
		statusOp.Failed(err)
	} else {
		statusOp.OK()
		if readyConditionChanged(previousReadyStatus, previousReadyReason, vol.Status.Conditions) {
			r.recordEvent(vol, eventsv1.TypeNormal, eventsv1.ReasonVolumeDestroyed,
				eventsv1.ActionDeleting, "volume destruction completed")
		}
	}

	if hasFinalizer(vol.Finalizers, zfscsiv1.VolumeFinalizer) {
		patch := crclient.MergeFrom(vol.DeepCopy())
		removeFinalizer(&vol.Finalizers, zfscsiv1.VolumeFinalizer)
		if err := r.Patch(ctx, vol, patch); err != nil {
			if apierrors.IsNotFound(err) {
				return reconcile.Result{}, nil
			}

			return reconcile.Result{}, fmt.Errorf("remove volume finalizer %s/%s: %w", vol.Namespace, vol.Name, err)
		}
	}
	r.clearNFSWithdrawal(dataset)

	return reconcile.Result{}, nil
}

func (r *VolumeReconciler) clearNFSWithdrawal(dataset string) {
	r.nfsMu.Lock()
	delete(r.nfsWithdrawn, dataset)
	r.nfsMu.Unlock()
}

func hasFinalizer(finalizers []string, finalizer string) bool {
	return slices.Contains(finalizers, finalizer)
}

func removeFinalizer(finalizers *[]string, finalizer string) {
	out := (*finalizers)[:0]
	for _, existing := range *finalizers {
		if existing != finalizer {
			out = append(out, existing)
		}
	}

	*finalizers = out
}

// volumeWarningEvent contains the public, static Event fields. Persisted status
// details are intentionally a separate parameter to prevent sensitive values
// from being published in Kubernetes Events.
type volumeWarningEvent struct {
	reason     string
	action     string
	publicNote string
}

// patchVolumeStatus records the Volume condition types owned by this reconciler
// separately from the remaining status fields. This avoids replacing unrelated
// condition types when a concurrent writer updates the status subresource.
func (r *VolumeReconciler) patchVolumeStatus(ctx context.Context, before, after *zfscsiv1.Volume, ownedTypes ...string) error {
	if err := patchStatusWithConditions(ctx, r.Client, before, after, ownedTypes...); err != nil {
		return err
	}
	return nil
}

// recordStatusWarning persists an actionable Ready=False transition before
// reporting it. The Event is intentionally best-effort and never changes retry
// behavior; a failed status patch suppresses the Event.
func (r *VolumeReconciler) recordStatusWarning(
	ctx context.Context,
	vol *zfscsiv1.Volume,
	state zfscsiv1.VolumeState,
	statusMessage string,
	event volumeWarningEvent,
) {
	previousStatus, previousReason := volumeReadyCondition(vol.Status.Conditions)
	before := vol.DeepCopy()
	after := vol.DeepCopy()
	after.Status.State = state
	after.Status.ObservedGeneration = after.Generation
	after.Status.Conditions = setCondition(after.Status.Conditions, after.Generation,
		string(zfscsiv1.VolumeConditionReady), metav1.ConditionFalse, event.reason, statusMessage)

	op := logging.LogWith(logr.FromContextOrDiscard(ctx), logging.OpPatchVolumeStatus,
		logging.KeyVolume, vol.Name, logging.KeyNamespace, vol.Namespace, logging.KeyState, state)
	if err := r.patchVolumeStatus(ctx, before, after, string(zfscsiv1.VolumeConditionReady)); err != nil {
		if apierrors.IsNotFound(err) {
			return
		}
		op.Failed(fmt.Errorf("patch volume status %s/%s: %w", vol.Namespace, vol.Name, err))
		return
	}
	op.OK()
	if readyConditionChanged(previousStatus, previousReason, after.Status.Conditions) {
		r.recordEvent(after, eventsv1.TypeWarning, event.reason, event.action, event.publicNote)
	}
}

// recordBackendHealthWarning persists health before returning a repair error so
// controller-side health polling remains truthful across retry and restart.
func (r *VolumeReconciler) recordDatasetMissing(ctx context.Context, vol *zfscsiv1.Volume) error {
	key := apimachinerytypes.NamespacedName{Name: vol.Name}
	before := vol.DeepCopy()
	after := vol.DeepCopy()
	previousHealthStatus, previousHealthReason := backendHealthCondition(before.Status.Conditions)
	after.Status.State = zfscsiv1.VolumeStatePending
	after.Status.ObservedGeneration = after.Generation
	after.Status.Conditions = setCondition(after.Status.Conditions, after.Generation,
		string(zfscsiv1.VolumeConditionBackendHealthy), metav1.ConditionFalse,
		eventsv1.ReasonBackendUnhealthy, "backing dataset is missing")
	err := r.patchVolumeStatus(ctx, before, after, string(zfscsiv1.VolumeConditionBackendHealthy))
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("patch missing dataset status %s/%s: %w", key.Namespace, key.Name, err)
	}
	if backendHealthChanged(previousHealthStatus, previousHealthReason, after.Status.Conditions) {
		current := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}}
		r.recordEvent(current, eventsv1.TypeWarning, eventsv1.ReasonBackendUnhealthy,
			eventsv1.ActionHealthChecking, "volume backend is unhealthy")
	}

	return nil
}

func (r *VolumeReconciler) patchPublishedInitiators(ctx context.Context, vol *zfscsiv1.Volume, published []string, fenced bool) error {
	key := apimachinerytypes.NamespacedName{Name: vol.Name}
	before := vol.DeepCopy()
	after := vol.DeepCopy()
	previousReadyStatus, previousReadyReason := volumeReadyCondition(before.Status.Conditions)
	after.Status.PublishedInitiators = published
	after.Status.ObservedGeneration = after.Generation
	if fenced {
		after.Status.Conditions = setCondition(after.Status.Conditions, after.Generation,
			string(zfscsiv1.VolumeConditionReady), metav1.ConditionTrue,
			eventsv1.ReasonInitiatorFenced, "stale transport connections fenced")
	}
	err := r.patchVolumeStatus(ctx, before, after, string(zfscsiv1.VolumeConditionReady))
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("patch published initiators %s/%s: %w", key.Namespace, key.Name, err)
	}
	if fenced && readyConditionChanged(previousReadyStatus, previousReadyReason, after.Status.Conditions) {
		current := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}}
		r.recordEvent(current, eventsv1.TypeNormal, eventsv1.ReasonInitiatorFenced,
			eventsv1.ActionExporting, "stale volume transport connections were fenced")
	}

	return nil
}

func (r *VolumeReconciler) recordBackendHealthWarning(ctx context.Context, vol *zfscsiv1.Volume, message string) error {
	key := apimachinerytypes.NamespacedName{Name: vol.Name}
	before := &zfscsiv1.Volume{}
	if err := r.Get(ctx, key, before); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get volume before patching backend health %s/%s: %w", key.Namespace, key.Name, err)
	}
	after := before.DeepCopy()
	previousStatus, previousReason := backendHealthCondition(before.Status.Conditions)
	after.Status.Conditions = setCondition(after.Status.Conditions, after.Generation,
		string(zfscsiv1.VolumeConditionBackendHealthy), metav1.ConditionFalse,
		eventsv1.ReasonBackendUnhealthy, message)
	statusOp := logging.LogWith(logr.FromContextOrDiscard(ctx), logging.OpPatchVolumeStatus,
		logging.KeyVolume, after.Name, logging.KeyNamespace, after.Namespace)
	err := r.patchVolumeStatus(ctx, before, after, string(zfscsiv1.VolumeConditionBackendHealthy))
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		statusOp.Failed(err)
		return fmt.Errorf("patch backend health %s/%s: %w", key.Namespace, key.Name, err)
	}
	statusOp.OK()
	if backendHealthChanged(previousStatus, previousReason, after.Status.Conditions) {
		current := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}}
		r.recordEvent(current, eventsv1.TypeWarning, eventsv1.ReasonBackendUnhealthy,
			eventsv1.ActionHealthChecking, "volume backend is unhealthy")
	}

	return nil
}

func (r *VolumeReconciler) recordBackendHealthy(ctx context.Context, vol *zfscsiv1.Volume) error {
	key := apimachinerytypes.NamespacedName{Name: vol.Name}
	// The warning can have been written earlier in this same reconcile pass, so
	// reload before deciding whether recovery is warranted.
	before := &zfscsiv1.Volume{}
	if err := r.Get(ctx, key, before); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get volume before marking backend healthy %s/%s: %w", key.Namespace, key.Name, err)
	}
	previousStatus, previousReason := backendHealthCondition(before.Status.Conditions)
	if previousStatus != metav1.ConditionFalse {
		return nil
	}
	after := before.DeepCopy()
	after.Status.Conditions = setCondition(after.Status.Conditions, after.Generation,
		string(zfscsiv1.VolumeConditionBackendHealthy), metav1.ConditionTrue,
		eventsv1.ReasonBackendRecovered, "backend is healthy")
	err := r.patchVolumeStatus(ctx, before, after, string(zfscsiv1.VolumeConditionBackendHealthy))
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("patch backend health %s/%s: %w", key.Namespace, key.Name, err)
	}
	if backendHealthChanged(previousStatus, previousReason, after.Status.Conditions) {
		current := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}}
		r.recordEvent(current, eventsv1.TypeNormal, eventsv1.ReasonBackendRecovered,
			eventsv1.ActionHealthChecking, "volume backend recovered")
	}

	return nil
}

func (r *VolumeReconciler) recordEvent(vol *zfscsiv1.Volume, eventType, reason, action, note string) {
	if r.Recorder != nil {
		r.Recorder.Eventf(vol, nil, eventType, reason, action, note)
	}
}

func volumeReadyCondition(conditions []metav1.Condition) (metav1.ConditionStatus, string) {
	for _, condition := range conditions {
		if condition.Type == string(zfscsiv1.VolumeConditionReady) {
			return condition.Status, condition.Reason
		}
	}

	return "", ""
}

func readyConditionChanged(
	previousStatus metav1.ConditionStatus,
	previousReason string,
	conditions []metav1.Condition,
) bool {
	status, reason := volumeReadyCondition(conditions)

	return status != previousStatus || reason != previousReason
}

func backendHealthCondition(conditions []metav1.Condition) (metav1.ConditionStatus, string) {
	for _, condition := range conditions {
		if condition.Type == string(zfscsiv1.VolumeConditionBackendHealthy) {
			return condition.Status, condition.Reason
		}
	}

	return "", ""
}

func backendHealthChanged(
	previousStatus metav1.ConditionStatus,
	previousReason string,
	conditions []metav1.Condition,
) bool {
	status, reason := backendHealthCondition(conditions)

	return status != previousStatus || reason != previousReason
}

func (r *VolumeReconciler) healthRepairHoldEnabled(vol *zfscsiv1.Volume) bool {
	return r.EnableHealthRepairHold && r.Namespace != "" && vol.Namespace == r.Namespace &&
		strings.HasPrefix(vol.Name, "zfs-csi-e2e-health") &&
		vol.Annotations[healthRepairHoldAnnotation] == "true"
}

// SetupWithManager registers the reconciler + its watches with typed generics.
func (r *VolumeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	maxConcurrent := r.MaxConcurrentReconciles
	if maxConcurrent <= 0 {
		maxConcurrent = defaultMaxConcurrentReconciles
	}

	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("zfs-csi-volume")
	}

	if err := ctrl.NewControllerManagedBy(mgr).
		Named("volume").
		WithOptions(controller.Options{
			UsePriorityQueue: new(true),
			// Provision volumes concurrently — under E2E/conformance churn many
			// PVCs are created at once and serial reconciles (the default of 1)
			// make the whole suite crawl. This exercises the shared-nvmet-port
			// critical section concurrently, which the NVMET transport guards
			// with a mutex around configurePort (the port addr_* attributes are
			// shared mutable configfs state; the per-volume subsystem/namespace
			// ops stay lock-free). Configurable via --max-concurrent-reconciles.
			MaxConcurrentReconciles: maxConcurrent,
		}).
		For(&zfscsiv1.Volume{}).
		Complete(r); err != nil {
		return fmt.Errorf("complete volume controller: %w", err)
	}

	return nil
}

// Compile-time assertion that VolumeReconciler is a typed reconciler.
var _ reconcile.TypedReconciler[reconcile.Request] = (*VolumeReconciler)(nil)
