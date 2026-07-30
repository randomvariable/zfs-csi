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

//go:build envtest

package envtest

import (
	"context"
	"io"
	"path/filepath"
	"runtime"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/envtest/komega"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	nvmetv1 "github.com/randomvariable/zfs-csi/api/nvmet/v1alpha1"
	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
)

// Harness owns a real API server + etcd pair for controller-runtime tests.
type Harness struct {
	Env    *envtest.Environment
	Config *rest.Config
	Client client.Client
}

const DefaultOwnerNode = "envtest-storage-node"

// VolumeSpec supplies the required storage owner for envtest Volume fixtures.
// Tests that verify CRD rejection must construct their spec without this helper.
func VolumeSpec(spec zfscsiv1.VolumeSpec) zfscsiv1.VolumeSpec {
	if spec.OwnerNode == "" {
		spec.OwnerNode = DefaultOwnerNode
	}
	if spec.PoolGUID == "" {
		spec.PoolGUID = "1"
	}
	if spec.NetworkDomain == "" {
		spec.NetworkDomain = "workers"
	}
	return spec
}

// SnapshotSpec supplies the required pool identity for envtest fixtures.
func SnapshotSpec(spec zfscsiv1.SnapshotSpec) zfscsiv1.SnapshotSpec {
	if spec.PoolGUID == "" {
		spec.PoolGUID = "1"
	}
	return spec
}

// Start launches envtest with the project's CRDs installed.
func Start(t *testing.T) *Harness {
	t.Helper()
	crlog.SetLogger(zap.New(zap.WriteTo(io.Discard)))
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join(root, "deploy", "crd")}}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	s := clientgoscheme.Scheme
	if err := zfscsiv1.AddToScheme(s); err != nil {
		t.Fatalf("add zfs-csi scheme: %v", err)
	}
	if err := nvmetv1.AddToScheme(s); err != nil {
		t.Fatalf("add nvmet scheme: %v", err)
	}
	c, err := client.New(cfg, client.Options{Scheme: s})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	komega.SetClient(c)
	h := &Harness{Env: env, Config: cfg, Client: c}
	return h
}

// EnsureStorageNode installs one eligible logical-owner inventory fixture.
func EnsureStorageNode(t *testing.T, h *Harness, name, guid string) {
	t.Helper()
	enabled := true
	observed := metav1.Now()
	node := &zfscsiv1.StorageNode{
		ObjectMeta: metav1.ObjectMeta{Name: name, Generation: 1},
		Spec:       zfscsiv1.StorageNodeSpec{AuthoritativePoolGUIDs: []string{guid}, Enabled: &enabled, NetworkDomain: "envtest"},
	}
	if err := h.Client.Create(t.Context(), node); err != nil {
		t.Fatalf("create StorageNode fixture: %v", err)
	}
	current := &zfscsiv1.StorageNode{}
	if err := h.Client.Get(t.Context(), client.ObjectKey{Name: name}, current); err != nil {
		t.Fatalf("get StorageNode fixture: %v", err)
	}
	current.Status = zfscsiv1.StorageNodeStatus{
		ObservedGeneration: current.Generation,
		LastObservedTime:   &observed,
		ReachableFrom:      []string{"envtest"},
		Endpoints: []zfscsiv1.StorageNodeEndpoint{
			{Protocol: zfscsiv1.StorageProtocolNFS, Host: name, Port: 2049},
			{Protocol: zfscsiv1.StorageProtocolNVMeTCP, Host: name, Port: 4420},
		},
		Conditions: []metav1.Condition{{Type: zfscsiv1.StorageNodeConditionReady, Status: metav1.ConditionTrue, Reason: "FixtureReady", LastTransitionTime: observed, ObservedGeneration: current.Generation}},
		Pools:      []zfscsiv1.StorageNodePoolStatus{{GUID: guid, Name: "tank", FreeBytes: 1 << 40, CapacityObservedAt: observed, Ready: true}},
	}
	if err := h.Client.Status().Update(t.Context(), current); err != nil {
		t.Fatalf("ready StorageNode fixture: %v", err)
	}
}

// Stop shuts envtest down and fails the test on teardown errors.
func (h *Harness) Stop(t *testing.T) {
	t.Helper()
	if err := h.Env.Stop(); err != nil {
		t.Fatalf("stop envtest: %v", err)
	}
}

// CRDsInstalled fails if the CRD discovery endpoint does not expose the API.
func CRDsInstalled(ctx context.Context, t *testing.T, h *Harness) {
	t.Helper()
	list := &zfscsiv1.VolumeList{}
	if err := h.Client.List(ctx, list); err != nil {
		t.Fatalf("list %s: %v", schema.GroupVersionResource{Group: zfscsiv1.GroupVersion.Group, Version: zfscsiv1.GroupVersion.Version, Resource: "volumes"}, err)
	}
	imports := &zfscsiv1.VolumeImportList{}
	if err := h.Client.List(ctx, imports); err != nil {
		t.Fatalf("list %s: %v", schema.GroupVersionResource{Group: zfscsiv1.GroupVersion.Group, Version: zfscsiv1.GroupVersion.Version, Resource: "volumeimports"}, err)
	}
}
