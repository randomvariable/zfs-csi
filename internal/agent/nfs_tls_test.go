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

package agent

import (
	"context"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apimachinerytypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/naming"
	"github.com/randomvariable/zfs-csi/internal/tlsca"
	"github.com/randomvariable/zfs-csi/internal/zfs"
)

func TestEnsureNFSTLSFailsClosedWithoutSignerIssuedPublicMaterial(t *testing.T) {
	d := newTestDeps(t)
	if err := EnsureNFSTLS(t.Context(), d.Client, "zfs-csi-system", "storage-a", "192.0.2.10"); err == nil {
		t.Fatal("EnsureNFSTLS() succeeded without controller-issued TLS material")
	}
	leafName, err := tlsca.ServerSecretName("storage-a")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{tlsca.CAPublicSecretName, leafName} {
		secret := &corev1.Secret{}
		if err := d.Client.Get(t.Context(), apimachinerytypes.NamespacedName{Namespace: "zfs-csi-system", Name: name}, secret); err == nil {
			t.Fatalf("storage readiness created controller-owned Secret %s", name)
		}
	}
}

func TestEnsureNFSTLSReadsOnlyPublicCAAndServerLeaf(t *testing.T) {
	d := newTestDeps(t)
	ca, err := tlsca.EnsureCA(t.Context(), d.Client, "zfs-csi-system")
	if err != nil {
		t.Fatal(err)
	}
	if err := tlsca.EnsurePublicCA(t.Context(), d.Client, "zfs-csi-system", ca); err != nil {
		t.Fatal(err)
	}
	if err := tlsca.EnsureNodeLeaf(t.Context(), d.Client, "zfs-csi-system", "storage-a", "192.0.2.10", ca); err != nil {
		t.Fatal(err)
	}

	reader := &recordingSecretReader{Reader: d.Client}
	if err := EnsureNFSTLS(t.Context(), reader, "zfs-csi-system", "storage-a", "192.0.2.10"); err != nil {
		t.Fatalf("EnsureNFSTLS() error = %v", err)
	}
	if slices.Contains(reader.names, tlsca.CASecretName) {
		t.Fatalf("storage TLS readiness requested private CA Secret %q", tlsca.CASecretName)
	}
	leafName, err := tlsca.ServerSecretName("storage-a")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(reader.names, tlsca.CAPublicSecretName) || !slices.Contains(reader.names, leafName) {
		t.Fatalf("storage TLS readiness reads = %v, want public CA and server leaf", reader.names)
	}
}

type recordingSecretReader struct {
	client.Reader
	names []string
}

func (r *recordingSecretReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	r.names = append(r.names, key.Name)
	return r.Reader.Get(ctx, key, obj, opts...)
}

func TestEnsureNFSTLSRepairsInvalidExistingLeaf(t *testing.T) {
	d := newTestDeps(t)
	leafName, err := tlsca.ServerSecretName("storage-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Client.Create(t.Context(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "zfs-csi-system", Name: leafName},
		Data:       map[string][]byte{"ca.crt": []byte("invalid")},
	}); err != nil {
		t.Fatal(err)
	}

	if err := EnsureNFSTLS(t.Context(), d.Client, "zfs-csi-system", "storage-a", "server7.example.test"); err == nil {
		t.Fatal("EnsureNFSTLS() accepted malformed controller-issued server certificate")
	}
}

func TestEnsureNFSTLSRepairsLeafSignedByDifferentCA(t *testing.T) {
	d := newTestDeps(t)
	ca, err := tlsca.EnsureCA(t.Context(), d.Client, "zfs-csi-system")
	if err != nil {
		t.Fatal(err)
	}
	if err := tlsca.EnsurePublicCA(t.Context(), d.Client, "zfs-csi-system", ca); err != nil {
		t.Fatal(err)
	}
	if err := tlsca.EnsureNodeLeaf(t.Context(), d.Client, "zfs-csi-system", "storage-a", "server7.example.test", ca); err != nil {
		t.Fatal(err)
	}

	leaf := &corev1.Secret{}
	leafName, err := tlsca.ServerSecretName("storage-a")
	if err != nil {
		t.Fatal(err)
	}
	leafKey := apimachinerytypes.NamespacedName{Namespace: "zfs-csi-system", Name: leafName}
	if err := d.Client.Get(t.Context(), leafKey, leaf); err != nil {
		t.Fatal(err)
	}
	publicCA := &corev1.Secret{}
	publicKey := apimachinerytypes.NamespacedName{Namespace: "zfs-csi-system", Name: tlsca.CAPublicSecretName}
	if err := d.Client.Get(t.Context(), publicKey, publicCA); err != nil {
		t.Fatal(err)
	}
	wrongCA, err := tlsca.NewCA("wrong")
	if err != nil {
		t.Fatal(err)
	}
	publicCA.Data["ca.crt"] = wrongCA.CertPEM
	if err := d.Client.Update(t.Context(), publicCA); err != nil {
		t.Fatal(err)
	}
	if err := EnsureNFSTLS(t.Context(), d.Client, "zfs-csi-system", "storage-a", "server7.example.test"); err == nil {
		t.Fatal("EnsureNFSTLS() accepted mismatched CA")
	}
	if err := d.Client.Get(t.Context(), leafKey, leaf); err != nil {
		t.Fatal(err)
	}
	if err := d.Client.Get(t.Context(), publicKey, publicCA); err != nil {
		t.Fatal(err)
	}
	if string(publicCA.Data["ca.crt"]) != string(wrongCA.CertPEM) {
		t.Fatal("storage reader changed invalid public CA")
	}
}

func TestEnsureNFSTLSRejectsEmptyEndpoint(t *testing.T) {
	d := newTestDeps(t)
	if err := EnsureNFSTLS(t.Context(), d.Client, "zfs-csi-system", "storage-a", ""); err == nil {
		t.Fatal("EnsureNFSTLS() succeeded without endpoint")
	}
}

func TestReconcileRejectsTLSWhenRuntimeDisabledBeforeMaterialization(t *testing.T) {
	d := newTestDeps(t)
	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "tls-volume"},
		Spec:       zfscsiv1.VolumeSpec{Pool: "tank", PoolGUID: "1", Type: zfscsiv1.VolumeTypeFilesystem, OwnerNode: "storage-a", VolumeID: "csi:tank:filesystem:tls-volume", NFSTLSEnabled: true},
	}
	if err := d.Client.Create(t.Context(), vol); err != nil {
		t.Fatal(err)
	}
	r := d.reconciler()
	r.NFSTLSEnabled = true
	if _, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: vol.Name}}); err == nil {
		t.Fatal("Reconcile() succeeded with NFS TLS runtime disabled")
	}
	if exists, err := d.zfsb.Exists(t.Context(), "tank/csi/filesystem/tls-volume"); err != nil || exists {
		t.Fatalf("TLS-gated reconcile materialized dataset: exists=%t err=%v", exists, err)
	}
}

func TestReconcileDeletingTLSVolumeBypassesReadinessGate(t *testing.T) {
	for _, tc := range []struct {
		name              string
		deleteAfterCreate bool
		volume            *zfscsiv1.Volume
	}{
		{
			name:              "deletion timestamp with missing certificate",
			deleteAfterCreate: true,
			volume: &zfscsiv1.Volume{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "tls-delete-timestamp",
					Finalizers: []string{zfscsiv1.VolumeFinalizer},
				},
				Spec: zfscsiv1.VolumeSpec{
					Pool: "tank", Type: zfscsiv1.VolumeTypeFilesystem,
					OwnerNode: "storage-a", VolumeID: "csi:tank:filesystem:tls-delete-timestamp",
					NFSTLSEnabled: true,
				},
			},
		},
		{
			name: "deleting state skips non-filesystem TLS validation",
			volume: &zfscsiv1.Volume{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "tls-delete-state",
					Finalizers: []string{zfscsiv1.VolumeFinalizer},
				},
				Spec: zfscsiv1.VolumeSpec{
					Pool: "tank", Type: zfscsiv1.VolumeTypeBlock,
					OwnerNode: "storage-a", VolumeID: "csi:tank:block:tls-delete-state",
					NFSTLSEnabled: true,
				},
				Status: zfscsiv1.VolumeStatus{State: zfscsiv1.VolumeStateDeleting},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := newTestDeps(t)
			p, err := naming.ParseVolID(tc.volume.Spec.VolumeID)
			if err != nil {
				t.Fatal(err)
			}
			dataset := backendPathForVolume(tc.volume, p)
			if err := d.zfsb.Create(context.Background(), zfs.CreateOptions{Name: dataset, Kind: p.Kind, Capacity: 1 << 30}); err != nil {
				t.Fatal(err)
			}
			if err := d.Client.Create(t.Context(), tc.volume); err != nil {
				t.Fatal(err)
			}
			if tc.deleteAfterCreate {
				if err := d.Client.Delete(t.Context(), tc.volume); err != nil {
					t.Fatal(err)
				}
			}

			r := d.reconciler()
			r.NFSTLSEnabled = false // No runtime and no Secret must not pin deletion.
			if _, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: tc.volume.Name}}); err != nil {
				t.Fatalf("Reconcile() deletion error = %v", err)
			}
			if exists, err := d.zfsb.Exists(t.Context(), dataset); err != nil || exists {
				t.Fatalf("delete did not destroy dataset: exists=%t err=%v", exists, err)
			}
			stored := &zfscsiv1.Volume{}
			if err := d.Client.Get(t.Context(), apimachinerytypes.NamespacedName{Name: tc.volume.Name}, stored); err == nil && hasFinalizer(stored.Finalizers, zfscsiv1.VolumeFinalizer) {
				t.Fatal("delete reconciliation left volume finalizer behind")
			}
		})
	}
}

func TestReconcileNFSTLSRequiresControllerIssuedLeafBeforeSharing(t *testing.T) {
	d := newTestDeps(t)
	vol := &zfscsiv1.Volume{
		ObjectMeta: metav1.ObjectMeta{Name: "tls-ready-volume"},
		Spec: zfscsiv1.VolumeSpec{
			Pool: "tank", PoolGUID: "1", Type: zfscsiv1.VolumeTypeFilesystem,
			OwnerNode: "storage-a", VolumeID: "csi:tank:filesystem:tls-ready-volume",
			NFSTLSEnabled: true, NFSExportCIDRs: []string{"10.0.0.0/8"},
		},
	}
	if err := d.Client.Create(t.Context(), vol); err != nil {
		t.Fatal(err)
	}
	r := d.reconciler()
	r.NFSTLSEnabled = true
	if _, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: vol.Name}}); err == nil {
		t.Fatal("Reconcile() succeeded without controller-issued TLS material")
	}
	leaf := &corev1.Secret{}
	leafName, err := tlsca.ServerSecretName("storage-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Client.Get(t.Context(), apimachinerytypes.NamespacedName{Namespace: "zfs-csi-system", Name: leafName}, leaf); err == nil {
		t.Fatal("storage reconcile created controller-owned TLS leaf")
	}
	if exists, err := d.zfsb.Exists(t.Context(), "tank/csi/filesystem/tls-ready-volume"); err != nil || exists {
		t.Fatalf("TLS-gated reconcile materialized dataset: exists=%t err=%v", exists, err)
	}
}
