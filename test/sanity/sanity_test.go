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

//go:build sanity && envtest

package sanity_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/go-logr/logr"
	csisanity "github.com/kubernetes-csi/csi-test/v5/pkg/sanity"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apimachinerytypes "k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"

	nvmetv1 "github.com/randomvariable/zfs-csi/api/nvmet/v1alpha1"
	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/agent"
	"github.com/randomvariable/zfs-csi/internal/crypto"
	"github.com/randomvariable/zfs-csi/internal/driver"
	"github.com/randomvariable/zfs-csi/internal/inventory"
	"github.com/randomvariable/zfs-csi/internal/stage"
	stagepb "github.com/randomvariable/zfs-csi/internal/stagepb/stage"
	testenv "github.com/randomvariable/zfs-csi/internal/testutil/envtest"
	"github.com/randomvariable/zfs-csi/internal/transport"
	"github.com/randomvariable/zfs-csi/internal/zfs"
	zfsfake "github.com/randomvariable/zfs-csi/internal/zfs/fake"
)

const (
	sanityNamespace     = "default"
	sanityNodeID        = "sanity"
	sanityOwnerNode     = "sanity-node"
	sanityPoolGUID      = "1"
	sanityNetworkDomain = "workers"
)

func TestCSISanity(t *testing.T) {
	tmp := t.TempDir()
	addr := filepath.Join(tmp, "csi.sock")
	stop := startSanityDriver(t, addr)
	defer stop()

	config := csisanity.NewTestConfig()
	config.Address = "unix://" + addr
	config.TargetPath = filepath.Join(tmp, "target")
	config.StagingPath = filepath.Join(tmp, "staging")
	config.CreateTargetDir = recreateDir
	config.CreateStagingDir = recreateDir
	config.RemoveTargetPath = removePath
	config.RemoveStagingPath = removePath
	config.TestVolumeParameters = map[string]string{"pool": "tank", "type": "block"}
	config.IdempotentCount = 2
	config.TestNodeVolumeAttachLimit = true

	csisanity.Test(t, config)
}

func recreateDir(path string) (string, error) {
	if err := os.RemoveAll(path); err != nil {
		return "", err
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return "", err
	}
	return path, nil
}

func removePath(path string) error { return os.RemoveAll(path) }

func startSanityDriver(t *testing.T, addr string) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	h := testenv.Start(t)

	if err := h.Client.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: sanityNamespace}}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create sanity namespace: %v", err)
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := zfscsiv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add zfs-csi scheme: %v", err)
	}
	if err := nvmetv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add nvmet scheme: %v", err)
	}
	mgr, err := ctrl.NewManager(h.Config, ctrl.Options{Scheme: scheme, Metrics: server.Options{BindAddress: "0"}})
	if err != nil {
		t.Fatalf("create sanity manager: %v", err)
	}

	// Placement only selects pools advertised by a fresh, Ready StorageNode, so
	// publish one for the fake pool and keep its observation timestamps current
	// for the duration of the run (inventory.FreshnessTimeout bounds staleness).
	// The reconciler also verifies each CR's pool GUID against the backend, so
	// the fake pool carries the same identity the inventory advertises.
	zfsBackend := zfsfake.New().WithPoolIdentity("tank", 1<<40, sanityPoolGUID, "ONLINE")
	publishSanityInventory(ctx, t, h.Client)
	// ControllerPublishVolume verifies the attachment target is a real node, so
	// the harness must register the node id NodeGetInfo advertises.
	if err := h.Client.Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: sanityNodeID}}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create sanity Node: %v", err)
	}

	export := transport.New()
	volRec := &agent.VolumeReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Log:      logr.Discard(),
		ZFS:      zfsBackend,
		Export:   export,
		Keys:     nopKeyProvider{},
		Stager:   nopStager{},
		Portal:   "server7:4420",
		NodeName: "sanity-node",
	}
	if err := volRec.SetupWithManager(mgr); err != nil {
		t.Fatalf("setup volume reconciler: %v", err)
	}
	snapRec := &agent.SnapshotReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Log: logr.Discard(), ZFS: zfsBackend, NodeName: "sanity-node"}
	if err := snapRec.SetupWithManager(mgr); err != nil {
		t.Fatalf("setup snapshot reconciler: %v", err)
	}

	mgrErr := make(chan error, 1)
	go func() { mgrErr <- mgr.Start(ctx) }()
	if ok := mgr.GetCache().WaitForCacheSync(ctx); !ok {
		t.Fatal("manager cache did not sync")
	}

	if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
		t.Fatalf("remove stale sanity socket: %v", err)
	}
	lis, err := net.Listen("unix", addr)
	if err != nil {
		t.Fatalf("listen sanity driver: %v", err)
	}

	srv := grpc.NewServer()
	csi.RegisterIdentityServer(srv, driver.NewIdentityServer(nil))
	csi.RegisterControllerServer(srv, driver.NewControllerServer(driver.ControllerConfig{
		Log:       logr.Discard(),
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Namespace: sanityNamespace,
		Portal:    "server7:4420",
	}))
	// Wire in-process StagePlugin sidecars (the node driver routes staging via
	// gRPC now; spin up local plugin servers backed by the sanity fakes).
	nvmetStageSrv := stage.NewNVMeServer("sanity", logr.Discard(),
		transport.NewNVMETClientWithHostFS(newSanityHostFS()), newFakeMounter())
	nfsStageSrv := stage.NewNFSServer("sanity", logr.Discard(), newFakeMounter())

	nvmetSock := filepath.Join(t.TempDir(), "nvmet-stage.sock")
	nfsSock := filepath.Join(t.TempDir(), "nfs-stage.sock")
	nvmetStageListener, err := net.Listen("unix", nvmetSock)
	if err != nil {
		t.Fatalf("listen nvmet stage: %v", err)
	}
	nfsStageListener, err := net.Listen("unix", nfsSock)
	if err != nil {
		t.Fatalf("listen nfs stage: %v", err)
	}
	nvmetStageGS := grpc.NewServer()
	nfsStageGS := grpc.NewServer()
	stagepb.RegisterStagePluginServer(nvmetStageGS, nvmetStageSrv)
	stagepb.RegisterStagePluginServer(nfsStageGS, nfsStageSrv)
	go func() { _ = nvmetStageGS.Serve(nvmetStageListener) }()
	go func() { _ = nfsStageGS.Serve(nfsStageListener) }()
	// NOTE: the stage servers are stopped in the returned stop-closure below, NOT
	// via a defer here. A defer would fire the moment startSanityDriver returns
	// (its return value is the closure), tearing the stage sidecars down before
	// csisanity.Test ever dials them — every NodeStage/NodeUnstage would then fail
	// "nvmet-stage.sock: connect: no such file or directory".

	nvmetStageCli, err := stage.Dial(ctx, nvmetSock, logr.Discard())
	if err != nil {
		t.Fatalf("dial nvmet stage: %v", err)
	}
	nfsStageCli, err := stage.Dial(ctx, nfsSock, logr.Discard())
	if err != nil {
		t.Fatalf("dial nfs stage: %v", err)
	}

	csi.RegisterNodeServer(srv, driver.NewNodeServer(driver.NodeConfig{
		Log:    logr.Discard(),
		NodeID: sanityNodeID,
		// NodeGetInfo advertises this segment, and the controller requires the
		// resulting topology on CreateVolume, so it must match the StorageNode.
		NetworkDomain: sanityNetworkDomain,
		PortalHost:    "server7",
		Mounter:       newFakeMounter(),
		NFSServer:     "server7",
		StagePlugins: map[zfs.VolumeKind]*stage.Client{
			zfs.KindBlock:      nvmetStageCli,
			zfs.KindFilesystem: nfsStageCli,
		},
	}))

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()

	return func() {
		nvmetStageGS.Stop()
		nfsStageGS.Stop()
		srv.Stop()
		select {
		case err := <-serveErr:
			if err != nil && err != grpc.ErrServerStopped {
				t.Fatalf("sanity driver serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out stopping sanity driver")
		}
		cancel()
		select {
		case err := <-mgrErr:
			if err != nil {
				t.Fatalf("sanity manager: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out stopping sanity manager")
		}
		h.Stop(t)
	}
}

// publishSanityInventory creates the StorageNode the controller places against
// and refreshes its observation timestamps until the context is cancelled.
func publishSanityInventory(ctx context.Context, t *testing.T, c crclient.Client) {
	t.Helper()
	enabled := true
	node := &zfscsiv1.StorageNode{
		ObjectMeta: metav1.ObjectMeta{Name: sanityOwnerNode, Generation: 1},
		Spec: zfscsiv1.StorageNodeSpec{
			AuthoritativePoolGUIDs: []string{sanityPoolGUID},
			Enabled:                &enabled,
			NetworkDomain:          sanityNetworkDomain,
		},
	}
	if err := c.Create(ctx, node); err != nil {
		t.Fatalf("create sanity StorageNode: %v", err)
	}

	refresh := func() error {
		current := &zfscsiv1.StorageNode{}
		if err := c.Get(ctx, apimachinerytypes.NamespacedName{Name: sanityOwnerNode}, current); err != nil {
			return err
		}
		patch := crclient.MergeFrom(current.DeepCopy())
		observed := metav1.Now()
		current.Status = zfscsiv1.StorageNodeStatus{
			ObservedGeneration: current.Generation,
			LastObservedTime:   &observed,
			ReachableFrom:      []string{sanityNetworkDomain},
			Endpoints: []zfscsiv1.StorageNodeEndpoint{
				{Protocol: zfscsiv1.StorageProtocolNFS, Host: "server7", Port: 2049},
				{Protocol: zfscsiv1.StorageProtocolNVMeTCP, Host: "server7", Port: 4420},
			},
			Conditions: []metav1.Condition{{
				Type: zfscsiv1.StorageNodeConditionReady, Status: metav1.ConditionTrue,
				Reason: "Ready", LastTransitionTime: observed, ObservedGeneration: current.Generation,
			}},
			Pools: []zfscsiv1.StorageNodePoolStatus{{
				GUID: sanityPoolGUID, Name: "tank", FreeBytes: 1 << 40,
				CapacityObservedAt: observed, Ready: true,
			}},
		}

		return c.Status().Patch(ctx, current, patch)
	}
	if err := refresh(); err != nil {
		t.Fatalf("publish sanity StorageNode status: %v", err)
	}

	go func() {
		ticker := time.NewTicker(inventory.FreshnessTimeout / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = refresh()
			}
		}
	}()
}

type nopKeyProvider struct{}

func (nopKeyProvider) Generate(context.Context, string) (string, error) { return "", nil }
func (nopKeyProvider) Fetch(context.Context, string) ([]byte, error) {
	return nil, crypto.ErrKeyNotFound
}
func (nopKeyProvider) Delete(context.Context, string) error { return nil }

type nopStager struct{}

func (nopStager) Stage(string, []byte) (string, string, error) { return "", "", nil }
func (nopStager) Shred(string) error                           { return nil }

type fakeMounter struct {
	mu        sync.Mutex
	formatted map[string]bool
}

func newFakeMounter() *fakeMounter { return &fakeMounter{formatted: map[string]bool{}} }

func (m *fakeMounter) Format(_ context.Context, device, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.formatted[device] = true
	return nil
}

func (m *fakeMounter) IsFormatted(_ context.Context, device, _ string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.formatted[device], nil
}

func (m *fakeMounter) Mount(_ context.Context, _, target, _ string, _ []string) error {
	return os.MkdirAll(target, 0o755)
}

func (m *fakeMounter) Unmount(_ context.Context, target string) error { return os.RemoveAll(target) }

func (m *fakeMounter) IsMounted(context.Context, string, string) (bool, error) { return true, nil }

func (m *fakeMounter) Resize(context.Context, string, string, string) error { return nil }

func (m *fakeMounter) BindMount(_ context.Context, _, target string, _ bool) error {
	return os.MkdirAll(target, 0o755)
}

func (m *fakeMounter) BindMountDevice(_ context.Context, _, target string, _ bool) error {
	// Raw-block bind: the real impl binds a device node onto a regular file at
	// target. For the fake, create the parent dir and an empty target file.
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(target, os.O_CREATE, 0o644)
	if err != nil {
		return err
	}

	return f.Close()
}

func (m *fakeMounter) DeviceFromMount(_ context.Context, mountpoint string) (string, error) {
	// The fake has no mountinfo; return a synthetic device path so
	// NodeExpandVolume has a non-empty device to pass through.
	return "/dev/fake" + strings.ReplaceAll(mountpoint, "/", "-"), nil
}

type sanityHostFS struct {
	mu          sync.Mutex
	controllers map[string]string
}

func newSanityHostFS() *sanityHostFS { return &sanityHostFS{controllers: map[string]string{}} }

func (fs *sanityHostFS) WriteFile(path string, data []byte) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if path == "/dev/nvme-fabrics" {
		values := parseNVMeConnectString(string(data))
		ctl := fmt.Sprintf("nvme%d", len(fs.controllers))
		fs.controllers[ctl] = values["nqn"]
	}
	return nil
}

func (fs *sanityHostFS) ReadFile(path string) ([]byte, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	for ctl, nqn := range fs.controllers {
		if path == "/sys/class/nvme/"+ctl+"/subsysnqn" {
			return []byte(nqn + "\n"), nil
		}
	}
	return nil, os.ErrNotExist
}

func (fs *sanityHostFS) ReadDir(path string) ([]string, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if path == "/sys/class/nvme" {
		controllers := make([]string, 0, len(fs.controllers))
		for ctl := range fs.controllers {
			controllers = append(controllers, ctl)
		}
		return controllers, nil
	}
	for ctl := range fs.controllers {
		if path == "/sys/class/nvme/"+ctl {
			return []string{ctl + "n1"}, nil
		}
	}
	return nil, os.ErrNotExist
}

func parseNVMeConnectString(s string) map[string]string {
	out := map[string]string{}
	for _, field := range strings.Split(s, ",") {
		key, value, ok := strings.Cut(field, "=")
		if ok {
			out[key] = value
		}
	}
	return out
}

var (
	_ crypto.KeyProvider = nopKeyProvider{}
	_ crypto.Stager      = nopStager{}
	_ transport.HostFS   = (*sanityHostFS)(nil)
)
