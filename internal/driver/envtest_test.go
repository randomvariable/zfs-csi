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

package driver

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/go-logr/logr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/psk"
	testenv "github.com/randomvariable/zfs-csi/internal/testutil/envtest"
)

func TestEnvtestCreateVolumeWritesCR(t *testing.T) {
	ctx := context.Background()
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.EnsureStorageNode(t, h, "server7", "1")

	cs := NewControllerServer(ControllerConfig{
		Log:       logr.Discard(),
		Client:    h.Client,
		APIReader: h.Client,
		Namespace: "default",
		Portal:    "server7:4420",
	})
	go envtestAutoReady(t, h.Client, "pvc-env")
	resp, err := cs.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:          "pvc-env",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:    map[string]string{"pool": "tank", "type": "block"},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if resp.GetVolume().GetVolumeId() != "csi:tank:block:pvc-env" {
		t.Fatalf("volume id = %q", resp.GetVolume().GetVolumeId())
	}
	got := &zfscsiv1.Volume{}
	if err := h.Client.Get(ctx, types.NamespacedName{Name: "pvc-env", Namespace: "default"}, got); err != nil {
		t.Fatalf("get created Volume: %v", err)
	}
	if got.Spec.Pool != "tank" || got.Spec.Capacity != 1<<30 {
		t.Fatalf("spec = %+v", got.Spec)
	}
}

func TestEnvtestCreateVolumeNVMeTLSSecretLifecycleContract(t *testing.T) {
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.EnsureStorageNode(t, h, "server7", "1")
	cs := NewControllerServer(ControllerConfig{Log: logr.Discard(), Client: h.Client, APIReader: h.Client, Namespace: "default", PSKReader: bytes.NewReader(bytes.Repeat([]byte{0x7a}, 64))})
	go envtestAutoReady(t, h.Client, "pvc-env-nvme-tls")
	req := envtestCreateRequest("pvc-env-nvme-tls", 1<<30)
	req.Parameters["nvmeTLS"] = "true"
	if _, err := cs.CreateVolume(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	vol := &zfscsiv1.Volume{}
	if err := h.Client.Get(t.Context(), types.NamespacedName{Name: req.Name}, vol); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{}
	if err := h.Client.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: vol.Spec.NVMeTLSPSKSecretName}, secret); err != nil {
		t.Fatal(err)
	}
	if secret.Immutable == nil || !*secret.Immutable || len(secret.OwnerReferences) != 0 {
		t.Fatalf("Secret immutable=%v ownerReferences=%v", secret.Immutable, secret.OwnerReferences)
	}
	if _, err := psk.Parse(string(secret.Data[nvmeTLSPSKSecretDataKey])); err != nil {
		t.Fatal(err)
	}
}

func TestEnvtestNVMeTLSCreateAndPublishContextContract(t *testing.T) {
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.EnsureStorageNode(t, h, "server7", "1")
	cs := NewControllerServer(ControllerConfig{Log: logr.Discard(), Client: h.Client, APIReader: h.Client, Namespace: "default", PSKReader: bytes.NewReader(bytes.Repeat([]byte{0x55}, 64))})
	go envtestAutoReady(t, h.Client, "pvc-env-nvme-publish")
	req := envtestCreateRequest("pvc-env-nvme-publish", 1<<30)
	req.Parameters["nvmeTLS"] = "true"
	created, err := cs.CreateVolume(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	vol := &zfscsiv1.Volume{}
	if err := h.Client.Get(t.Context(), types.NamespacedName{Name: req.Name}, vol); err != nil {
		t.Fatal(err)
	}
	if !vol.Spec.NVMeTLSEnabled || vol.Spec.NVMeTLSPSKSecretName == "" {
		t.Fatalf("NVMe TLS Volume spec = %+v", vol.Spec)
	}
	secret := &corev1.Secret{}
	if err := h.Client.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: vol.Spec.NVMeTLSPSKSecretName}, secret); err != nil {
		t.Fatal(err)
	}
	patch := client.MergeFrom(vol.DeepCopy())
	vol.Status.State = zfscsiv1.VolumeStateReady
	vol.Status.TargetNQN = "nqn.2026-01.io.randomvariable:zfs-csi:server7:1:block:pvc-env-nvme-publish"
	vol.Status.DeviceGUID = "device-guid"
	vol.Status.PortalHost = "10.0.0.7"
	vol.Status.PortalPort = 4421
	if err := h.Client.Status().Patch(t.Context(), vol, patch); err != nil {
		t.Fatal(err)
	}
	confirmed := make(chan error, 1)
	go func() { confirmed <- envtestConfirmMappedInitiator(t.Context(), h.Client, req.Name, "node-a") }()
	published, err := cs.ControllerPublishVolume(t.Context(), &csi.ControllerPublishVolumeRequest{VolumeId: created.GetVolume().GetVolumeId(), NodeId: "node-a"})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-confirmed; err != nil {
		t.Fatal(err)
	}
	pubCtx := published.GetPublishContext()
	if pubCtx[publishContextTLS] != "true" || pubCtx[publishContextPSKSecret] != vol.Spec.NVMeTLSPSKSecretName || pubCtx[publishContextPortal] != "10.0.0.7:4421" {
		t.Fatalf("NVMe TLS publish context = %#v", pubCtx)
	}
	raw := string(secret.Data[nvmeTLSPSKSecretDataKey])
	for _, value := range pubCtx {
		if strings.Contains(value, raw) {
			t.Fatal("raw PSK leaked in publish context")
		}
	}
}

func TestEnvtestNVMeTLSPublishRejectsNonTLSPortal(t *testing.T) {
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.EnsureStorageNode(t, h, "server7", "1")
	cs := NewControllerServer(ControllerConfig{Log: logr.Discard(), Client: h.Client, APIReader: h.Client, Namespace: "default", PSKReader: bytes.NewReader(bytes.Repeat([]byte{0x56}, 64))})
	go envtestAutoReady(t, h.Client, "pvc-env-nvme-wrong-portal")
	req := envtestCreateRequest("pvc-env-nvme-wrong-portal", 1<<30)
	req.Parameters["nvmeTLS"] = "true"
	created, err := cs.CreateVolume(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	vol := &zfscsiv1.Volume{}
	if err := h.Client.Get(t.Context(), types.NamespacedName{Name: req.Name}, vol); err != nil {
		t.Fatal(err)
	}
	patch := client.MergeFrom(vol.DeepCopy())
	vol.Status.State = zfscsiv1.VolumeStateReady
	vol.Status.TargetNQN = "nqn.2026-01.io.randomvariable:zfs-csi:server7:1:block:pvc-env-nvme-wrong-portal"
	vol.Status.DeviceGUID = "device-guid"
	vol.Status.PortalHost = "10.0.0.7"
	vol.Status.PortalPort = 4420
	if err := h.Client.Status().Patch(t.Context(), vol, patch); err != nil {
		t.Fatal(err)
	}
	confirmed := make(chan error, 1)
	go func() { confirmed <- envtestConfirmMappedInitiator(t.Context(), h.Client, req.Name, "node-a") }()
	published, err := cs.ControllerPublishVolume(t.Context(), &csi.ControllerPublishVolumeRequest{VolumeId: created.GetVolume().GetVolumeId(), NodeId: "node-a"})
	if err == nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ControllerPublishVolume error = %v, want FailedPrecondition", err)
	}
	if published != nil && len(published.GetPublishContext()) != 0 {
		t.Fatalf("unexpected publish context for non-TLS portal: %#v", published.GetPublishContext())
	}
	if err := <-confirmed; err != nil {
		t.Fatal(err)
	}
}

func TestEnvtestNFSTLSCreateAndPublishContextContract(t *testing.T) {
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.EnsureStorageNode(t, h, "server7", "1")
	cs := NewControllerServer(ControllerConfig{Log: logr.Discard(), Client: h.Client, APIReader: h.Client, Namespace: "default"})
	go envtestAutoReady(t, h.Client, "pvc-env-nfs-publish")
	req := &csi.CreateVolumeRequest{Name: "pvc-env-nfs-publish", CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30}, Parameters: map[string]string{"pool": "tank", "type": "filesystem", "nfsExportCIDRs": "10.0.0.0/24", "nfsTLS": "true"}, VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "nfs4"}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER}}}}
	created, err := cs.CreateVolume(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	vol := &zfscsiv1.Volume{}
	if err := h.Client.Get(t.Context(), types.NamespacedName{Name: req.Name}, vol); err != nil {
		t.Fatal(err)
	}
	if !vol.Spec.NFSTLSEnabled || created.GetVolume().GetVolumeContext()[publishContextTLS] != "" {
		t.Fatalf("NFS TLS persisted/context = spec=%+v context=%#v", vol.Spec, created.GetVolume().GetVolumeContext())
	}
	patch := client.MergeFrom(vol.DeepCopy())
	vol.Status.State = zfscsiv1.VolumeStateReady
	vol.Status.NFSServer = "10.0.0.7"
	vol.Status.ExportPath = "/tank/pvc-env-nfs-publish"
	if err := h.Client.Status().Patch(t.Context(), vol, patch); err != nil {
		t.Fatal(err)
	}
	published, err := cs.ControllerPublishVolume(t.Context(), &csi.ControllerPublishVolumeRequest{VolumeId: created.GetVolume().GetVolumeId(), NodeId: "node-a"})
	if err != nil {
		t.Fatal(err)
	}
	pubCtx := published.GetPublishContext()
	if pubCtx[publishContextTLS] != "true" || pubCtx[publishContextNFSServer] != "10.0.0.7" || pubCtx[publishContextExportPath] != "/tank/pvc-env-nfs-publish" {
		t.Fatalf("NFS TLS publish context = %#v", pubCtx)
	}
}

func TestEnvtestCreateVolumeReusesPreexistingNVMeTLSPSKOrphan(t *testing.T) {
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.EnsureStorageNode(t, h, "server7", "1")

	const volumeName = "pvc-env-nvme-tls-orphan"
	interchange, err := psk.Generate(bytes.NewReader(bytes.Repeat([]byte{0x31}, 32)), psk.HMACSHA256)
	if err != nil {
		t.Fatal(err)
	}
	formatted, err := interchange.Format()
	if err != nil {
		t.Fatal(err)
	}
	immutable := true
	secretName := nvmeTLSPSKSecretName(crNameFor(volumeName))
	orphan := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: "default"},
		Immutable:  &immutable,
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{nvmeTLSPSKSecretDataKey: []byte(formatted)},
	}
	if err := h.Client.Create(t.Context(), orphan); err != nil {
		t.Fatal(err)
	}

	// Failing entropy proves CreateVolume takes the existing-Secret branch.
	cs := NewControllerServer(ControllerConfig{
		Log: logr.Discard(), Client: h.Client, APIReader: h.Client, Namespace: "default",
		PSKReader: failingEntropyReader{},
	})
	go envtestAutoReady(t, h.Client, volumeName)
	req := envtestCreateRequest(volumeName, 1<<30)
	req.Parameters["nvmeTLS"] = "true"
	if _, err := cs.CreateVolume(t.Context(), req); err != nil {
		t.Fatal(err)
	}

	got := &corev1.Secret{}
	if err := h.Client.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: secretName}, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Data[nvmeTLSPSKSecretDataKey], orphan.Data[nvmeTLSPSKSecretDataKey]) {
		t.Fatal("CreateVolume overwrote pre-existing orphan PSK bytes")
	}
	if got.UID != orphan.UID || got.ResourceVersion != orphan.ResourceVersion {
		t.Fatalf("CreateVolume mutated or replaced orphan Secret: before uid/rv=%s/%s after=%s/%s", orphan.UID, orphan.ResourceVersion, got.UID, got.ResourceVersion)
	}
	vol := &zfscsiv1.Volume{}
	if err := h.Client.Get(t.Context(), types.NamespacedName{Name: volumeName}, vol); err != nil {
		t.Fatal(err)
	}
	if vol.Spec.NVMeTLSPSKSecretName != secretName {
		t.Fatalf("Volume PSK reference = %q, want %q", vol.Spec.NVMeTLSPSKSecretName, secretName)
	}
}

func TestEnvtestNVMeTLSCloneAndRestoreUseSeparateDestinationSecrets(t *testing.T) {
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.EnsureStorageNode(t, h, "server7", "1")

	cs := NewControllerServer(ControllerConfig{
		Log: logr.Discard(), Client: h.Client, APIReader: h.Client, Namespace: "default",
		PSKReader: bytes.NewReader(bytes.Repeat([]byte{0x42}, 128)),
	})
	immutable := true
	sourceSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: nvmeTLSPSKSecretName("tls-source"), Namespace: "default"},
		Immutable:  &immutable,
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{nvmeTLSPSKSecretDataKey: []byte("source-psk")},
	}
	source := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "tls-source"}, Spec: zfscsiv1.VolumeSpec{
		Pool: "tank", PoolGUID: "1", OwnerNode: "server7", NetworkDomain: "workers", Type: zfscsiv1.VolumeTypeBlock,
		Transport: zfscsiv1.TransportNVMeTCP, VolumeID: "csi:tank:block:tls-source", VolName: "tls-source", Capacity: 1 << 30,
		NVMeTLSEnabled: true, NVMeTLSPSKSecretName: nvmeTLSPSKSecretName("tls-source"),
	}}
	if err := h.Client.Create(t.Context(), sourceSecret); err != nil {
		t.Fatal(err)
	}
	if err := h.Client.Create(t.Context(), source); err != nil {
		t.Fatal(err)
	}

	cloneReq := envtestCreateRequest("pvc-env-tls-clone", 1<<30)
	cloneReq.Parameters["nvmeTLS"] = "true"
	cloneReq.VolumeContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: source.Spec.VolumeID}}}
	go envtestAutoReady(t, h.Client, cloneReq.Name)
	if _, err := cs.CreateVolume(t.Context(), cloneReq); err != nil {
		t.Fatalf("create TLS clone: %v", err)
	}

	snapshotID := "csi:tank:block:tls-source@snap"
	if err := h.Client.Create(t.Context(), &zfscsiv1.Snapshot{ObjectMeta: metav1.ObjectMeta{Name: "tls-source-snap"}, Spec: zfscsiv1.SnapshotSpec{SourceVolumeID: source.Spec.VolumeID, SnapshotID: snapshotID, OwnerNode: "server7", PoolGUID: "1"}}); err != nil {
		t.Fatal(err)
	}
	restoreReq := envtestCreateRequest("pvc-env-tls-restore", 1<<30)
	restoreReq.Parameters["nvmeTLS"] = "true"
	restoreReq.VolumeContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapshotID}}}
	go envtestAutoReady(t, h.Client, restoreReq.Name)
	if _, err := cs.CreateVolume(t.Context(), restoreReq); err != nil {
		t.Fatalf("restore TLS snapshot: %v", err)
	}

	var clone, restore zfscsiv1.Volume
	if err := h.Client.Get(t.Context(), types.NamespacedName{Name: cloneReq.Name}, &clone); err != nil {
		t.Fatal(err)
	}
	if err := h.Client.Get(t.Context(), types.NamespacedName{Name: restoreReq.Name}, &restore); err != nil {
		t.Fatal(err)
	}
	if clone.Spec.NVMeTLSPSKSecretName == source.Spec.NVMeTLSPSKSecretName || restore.Spec.NVMeTLSPSKSecretName == source.Spec.NVMeTLSPSKSecretName || clone.Spec.NVMeTLSPSKSecretName == restore.Spec.NVMeTLSPSKSecretName {
		t.Fatalf("NVMe TLS secret refs source=%q clone=%q restore=%q, want all distinct", source.Spec.NVMeTLSPSKSecretName, clone.Spec.NVMeTLSPSKSecretName, restore.Spec.NVMeTLSPSKSecretName)
	}
	for _, destination := range []zfscsiv1.Volume{clone, restore} {
		secret := &corev1.Secret{}
		if err := h.Client.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: destination.Spec.NVMeTLSPSKSecretName}, secret); err != nil {
			t.Fatal(err)
		}
		if secret.Immutable == nil || !*secret.Immutable || secret.Type != corev1.SecretTypeOpaque {
			t.Fatalf("destination Secret %q must be immutable opaque", secret.Name)
		}
	}
	if err := h.Client.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: sourceSecret.Name}, sourceSecret); err != nil {
		t.Fatal(err)
	}
	if got := string(sourceSecret.Data[nvmeTLSPSKSecretDataKey]); got != "source-psk" {
		t.Fatalf("source PSK = %q, want unchanged", got)
	}
}

type failingEntropyReader struct{}

func (failingEntropyReader) Read([]byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}

func TestEnvtestConcurrentPlacementDoesNotOvercommit(t *testing.T) {
	ctx := t.Context()
	h := testenv.Start(t)
	defer h.Stop(t)
	testenv.EnsureStorageNode(t, h, "server7", "1")
	node := &zfscsiv1.StorageNode{}
	if err := h.Client.Get(ctx, types.NamespacedName{Name: "server7"}, node); err != nil {
		t.Fatal(err)
	}
	before := node.DeepCopy()
	node.Status.Pools[0].FreeBytes = 100
	if err := h.Client.Status().Patch(ctx, node, client.MergeFrom(before)); err != nil {
		t.Fatal(err)
	}
	servers := []*ControllerServer{
		NewControllerServer(ControllerConfig{Log: logr.Discard(), Client: h.Client, APIReader: h.Client, Namespace: "default", Portal: "server7:4420"}),
		NewControllerServer(ControllerConfig{Log: logr.Discard(), Client: h.Client, APIReader: h.Client, Namespace: "default", Portal: "server7:4420"}),
	}
	errs := make(chan error, 2)
	for i := range servers {
		go func(i int) {
			requestCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()
			_, err := servers[i].CreateVolume(requestCtx, &csi.CreateVolumeRequest{Name: fmt.Sprintf("concurrent-%d", i), CapacityRange: &csi.CapacityRange{RequiredBytes: 60}, Parameters: map[string]string{"pool": "tank", "type": "block"}, VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}}})
			errs <- err
		}(i)
	}
	resourceExhausted := 0
	created := 0
	for range 2 {
		err := <-errs
		switch status.Code(err) {
		case codes.ResourceExhausted:
			resourceExhausted++
		case codes.DeadlineExceeded:
			created++ // CR was reserved; readiness intentionally absent.
		default:
			t.Fatalf("unexpected concurrent CreateVolume error: %v", err)
		}
	}
	if created != 1 || resourceExhausted != 1 {
		t.Fatalf("created=%d ResourceExhausted=%d, want 1/1", created, resourceExhausted)
	}
	volumes := &zfscsiv1.VolumeList{}
	if err := h.Client.List(ctx, volumes); err != nil {
		t.Fatal(err)
	}
	if len(volumes.Items) != 1 {
		t.Fatalf("Volume reservations=%d, want 1", len(volumes.Items))
	}
}

func TestEnvtestConcurrentCreateAndExpandShareReservation(t *testing.T) {
	h, cs := envtestExpansionController(t, 100)
	defer h.Stop(t)
	volume := envtestCreateVolume(t, h, "existing", 40)
	markEnvtestCapacityAccounted(t, h.Client, volume.Name)

	start := make(chan struct{})
	errs := make(chan error, 2)
	go func() {
		<-start
		requestCtx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
		defer cancel()
		_, err := cs[0].ControllerExpandVolume(requestCtx, &csi.ControllerExpandVolumeRequest{VolumeId: volume.Spec.VolumeID, CapacityRange: &csi.CapacityRange{RequiredBytes: 100}})
		errs <- err
	}()
	go func() {
		<-start
		requestCtx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
		defer cancel()
		_, err := cs[1].CreateVolume(requestCtx, envtestCreateRequest("new", 60))
		errs <- err
	}()
	close(start)
	assertOneReservationFits(t, <-errs, <-errs)
}

func TestEnvtestConcurrentExpandsDoNotLostUpdateOrOvercommit(t *testing.T) {
	h, cs := envtestExpansionController(t, 100)
	defer h.Stop(t)
	first := envtestCreateVolume(t, h, "grow-a", 40)
	second := envtestCreateVolume(t, h, "grow-b", 40)
	markEnvtestCapacityAccounted(t, h.Client, first.Name)
	markEnvtestCapacityAccounted(t, h.Client, second.Name)

	start := make(chan struct{})
	errs := make(chan error, 2)
	for i, volume := range []*zfscsiv1.Volume{first, second} {
		go func(i int, volume *zfscsiv1.Volume) {
			<-start
			_, err := cs[i].ControllerExpandVolume(t.Context(), &csi.ControllerExpandVolumeRequest{VolumeId: volume.Spec.VolumeID, CapacityRange: &csi.CapacityRange{RequiredBytes: 100}})
			errs <- err
		}(i, volume)
	}
	close(start)
	assertOneReservationFits(t, <-errs, <-errs)

	var grown int
	for _, name := range []string{first.Name, second.Name} {
		got := &zfscsiv1.Volume{}
		if err := h.Client.Get(t.Context(), types.NamespacedName{Name: name}, got); err != nil {
			t.Fatal(err)
		}
		if got.Spec.Capacity == 100 {
			grown++
		}
	}
	if grown != 1 {
		t.Fatalf("grown volumes=%d, want one", grown)
	}
}

func envtestExpansionController(t *testing.T, free int64) (*testenv.Harness, []*ControllerServer) {
	t.Helper()
	h := testenv.Start(t)
	testenv.EnsureStorageNode(t, h, "server7", "1")
	node := &zfscsiv1.StorageNode{}
	if err := h.Client.Get(t.Context(), types.NamespacedName{Name: "server7"}, node); err != nil {
		t.Fatal(err)
	}
	before := node.DeepCopy()
	node.Status.Pools[0].FreeBytes = free
	if err := h.Client.Status().Patch(t.Context(), node, client.MergeFrom(before)); err != nil {
		t.Fatal(err)
	}
	return h, []*ControllerServer{
		NewControllerServer(ControllerConfig{Log: logr.Discard(), Client: h.Client, APIReader: h.Client, Namespace: "default"}),
		NewControllerServer(ControllerConfig{Log: logr.Discard(), Client: h.Client, APIReader: h.Client, Namespace: "default"}),
	}
}

func envtestCreateVolume(t *testing.T, h *testenv.Harness, name string, capacity int64) *zfscsiv1.Volume {
	t.Helper()
	volume := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: testenv.VolumeSpec(zfscsiv1.VolumeSpec{Pool: "tank", PoolGUID: "1", OwnerNode: "server7", NetworkDomain: "envtest", Type: zfscsiv1.VolumeTypeBlock, VolumeID: "csi:tank:block:" + name, VolName: name, Capacity: capacity})}
	if err := h.Client.Create(t.Context(), volume); err != nil {
		t.Fatal(err)
	}
	return volume
}

func markEnvtestCapacityAccounted(t *testing.T, c client.Client, name string) {
	t.Helper()
	node := &zfscsiv1.StorageNode{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: "server7"}, node); err != nil {
		t.Fatal(err)
	}
	volume := &zfscsiv1.Volume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: name}, volume); err != nil {
		t.Fatal(err)
	}
	before := volume.DeepCopy()
	volume.Status.CapacityAccountedAt = node.Status.Pools[0].CapacityObservedAt.DeepCopy()
	if err := c.Status().Patch(t.Context(), volume, client.MergeFrom(before)); err != nil {
		t.Fatal(err)
	}
}

func envtestCreateRequest(name string, capacity int64) *csi.CreateVolumeRequest {
	return &csi.CreateVolumeRequest{Name: name, CapacityRange: &csi.CapacityRange{RequiredBytes: capacity}, Parameters: map[string]string{"pool": "tank", "type": "block"}, VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}}}
}

func assertOneReservationFits(t *testing.T, errs ...error) {
	t.Helper()
	var success, exhausted int
	for _, err := range errs {
		switch status.Code(err) {
		case codes.OK, codes.DeadlineExceeded:
			success++
		case codes.ResourceExhausted:
			exhausted++
		default:
			t.Fatalf("unexpected reservation result: %v", err)
		}
	}
	if success != 1 || exhausted != 1 {
		t.Fatalf("successful reservations=%d ResourceExhausted=%d, want 1/1", success, exhausted)
	}
}

func envtestAutoReady(t *testing.T, c client.Client, name string) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		v := &zfscsiv1.Volume{}
		if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, v); err == nil {
			patch := client.MergeFrom(v.DeepCopy())
			v.Status.State = zfscsiv1.VolumeStateReady
			v.Status.ActualCapacity = v.Spec.Capacity
			if err := c.Status().Patch(ctx, v, patch); err != nil {
				t.Logf("autoReady patch %q: %v", name, err)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Errorf("autoReady never observed CR %q", name)
}

func envtestConfirmMappedInitiator(ctx context.Context, c client.Client, name, initiatorID string) error {
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		v := &zfscsiv1.Volume{}
		if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, v); err == nil {
			for _, mapped := range v.Status.MappedInitiators {
				if mapped.InitiatorID != initiatorID {
					continue
				}
				patch := client.MergeFrom(v.DeepCopy())
				v.Status.PublishedInitiators = []string{initiatorID}
				if err := c.Status().Patch(ctx, v, patch); err != nil {
					return err
				}
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	return fmt.Errorf("publish mapping never observed for %q", name)
}
