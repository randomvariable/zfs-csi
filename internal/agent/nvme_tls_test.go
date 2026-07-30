// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apimachinerytypes "k8s.io/apimachinery/pkg/types"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/psk"
	"github.com/randomvariable/zfs-csi/internal/transport"
	"github.com/randomvariable/zfs-csi/internal/zfs"
)

type recordingNVMeTLSPSKProvisioner struct {
	calls        []struct{ hostNQN, subsysNQN string }
	removalCalls []struct{ hostNQN, subsysNQN string }
	err          error
	removes      int
	removeErr    error
}

func (p *recordingNVMeTLSPSKProvisioner) RemoveConfigured(_ psk.Interchange, hostNQN, subsysNQN string) error {
	p.removes++
	p.removalCalls = append(p.removalCalls, struct{ hostNQN, subsysNQN string }{hostNQN, subsysNQN})
	return p.removeErr
}

type exactSecretReader struct {
	crclient.Reader
	keys []apimachinerytypes.NamespacedName
}

func (r *exactSecretReader) Get(ctx context.Context, key crclient.ObjectKey, obj crclient.Object, opts ...crclient.GetOption) error {
	r.keys = append(r.keys, key)
	return r.Reader.Get(ctx, key, obj, opts...)
}

type recordingSecretDeleteClient struct {
	crclient.Client
	deleted []apimachinerytypes.NamespacedName
	err     error
}

func (c *recordingSecretDeleteClient) Delete(ctx context.Context, obj crclient.Object, opts ...crclient.DeleteOption) error {
	if secret, ok := obj.(*corev1.Secret); ok {
		c.deleted = append(c.deleted, crclient.ObjectKeyFromObject(secret))
		if c.err != nil {
			return c.err
		}
	}
	return c.Client.Delete(ctx, obj, opts...)
}

func (p *recordingNVMeTLSPSKProvisioner) InsertConfigured(_ psk.Interchange, hostNQN, subsysNQN string) error {
	p.calls = append(p.calls, struct{ hostNQN, subsysNQN string }{hostNQN, subsysNQN})
	return p.err
}

func configuredNVMeTLSPSK(t *testing.T) []byte {
	t.Helper()
	ic, err := psk.Generate(bytes.NewReader(bytes.Repeat([]byte{7}, 32)), psk.HMACSHA256)
	if err != nil {
		t.Fatal(err)
	}
	formatted, err := ic.Format()
	if err != nil {
		t.Fatal(err)
	}
	return []byte(formatted)
}

func TestReconcileNVMeTLSPSKBeforeExport(t *testing.T) {
	d := newTestDeps(t)
	d.export.returnedPortal = "192.168.192.7:4421"
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "zfs-csi-system", Name: "vol-psk"},
		Type:       corev1.SecretTypeOpaque,
		Immutable:  boolPtrAgent(true),
		Data:       map[string][]byte{nvmeTLSPSKSecretDataKey: configuredNVMeTLSPSK(t)},
	}
	if err := d.Client.Create(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
	vol := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "tls-block"}, Spec: zfscsiv1.VolumeSpec{
		Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, Capacity: 1 << 30,
		VolumeID: "csi:tank:block:tls-block", Transport: zfscsiv1.TransportNVMeTCP,
		NVMeTLSEnabled: true, NVMeTLSPSKSecretName: secret.Name,
	}, Status: zfscsiv1.VolumeStatus{MappedInitiators: []zfscsiv1.MappedInitiator{
		{NodeName: "worker-a", InitiatorID: "worker-a"},
		{NodeName: "worker-b", InitiatorID: "worker-b"},
	}}}
	if err := d.Client.Create(t.Context(), vol); err != nil {
		t.Fatal(err)
	}
	provisioner := &recordingNVMeTLSPSKProvisioner{}
	r := d.reconciler()
	r.NVMeTLSPSK = provisioner
	reader := &exactSecretReader{Reader: d.Client}
	r.NVMeTLSSecretReader = reader
	if _, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: vol.Name}}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}
	if len(provisioner.calls) != 2 {
		t.Fatalf("PSK provision calls = %d, want 2", len(provisioner.calls))
	}
	for i, want := range []string{"worker-a", "worker-b"} {
		call := provisioner.calls[i]
		if call.hostNQN != transport.HostNQN(want) || call.subsysNQN == "" {
			t.Fatalf("PSK identities = host %q subsystem %q", call.hostNQN, call.subsysNQN)
		}
	}
	if len(d.export.exports) != 1 {
		t.Fatal("target was not exported after PSK provision")
	}
	if len(reader.keys) != 1 || reader.keys[0] != (apimachinerytypes.NamespacedName{Namespace: "zfs-csi-system", Name: secret.Name}) {
		t.Fatalf("Secret reads = %#v", reader.keys)
	}
	got := getVol(t, d, vol.Name)
	if got.Status.Portal != "192.168.192.7:4421" || got.Status.PortalHost != "192.168.192.7" || got.Status.PortalPort != 4421 {
		t.Fatalf("exported endpoint = %q %q:%d, want TLS portal and structured endpoint", got.Status.Portal, got.Status.PortalHost, got.Status.PortalPort)
	}

	patch := crclient.MergeFrom(got.DeepCopy())
	got.Status.PortalHost = "192.168.192.7"
	got.Status.PortalPort = 4420
	if err := d.Status().Patch(t.Context(), got, patch); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: vol.Name}}); err != nil {
		t.Fatalf("ensure Reconcile() = %v", err)
	}
	got = getVol(t, d, vol.Name)
	if got.Status.PortalHost != "192.168.192.7" || got.Status.PortalPort != 4421 {
		t.Fatalf("repaired structured endpoint = %q:%d, want 192.168.192.7:4421", got.Status.PortalHost, got.Status.PortalPort)
	}
}

func TestReconcileNVMeTLSPSKFailureDoesNotExport(t *testing.T) {
	for _, tc := range []struct {
		name      string
		secret    *corev1.Secret
		insertErr error
	}{
		{name: "missing secret"},
		{name: "malformed secret", secret: &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "zfs-csi-system", Name: "vol-psk"}, Type: corev1.SecretTypeOpaque, Immutable: boolPtrAgent(true), Data: map[string][]byte{nvmeTLSPSKSecretDataKey: []byte("not-a-psk")}}},
		{name: "insert error", secret: &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "zfs-csi-system", Name: "vol-psk"}, Type: corev1.SecretTypeOpaque, Immutable: boolPtrAgent(true), Data: map[string][]byte{nvmeTLSPSKSecretDataKey: configuredNVMeTLSPSK(t)}}, insertErr: errors.New("keyring unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := newTestDeps(t)
			if tc.secret != nil {
				if err := d.Client.Create(t.Context(), tc.secret); err != nil {
					t.Fatal(err)
				}
			}
			vol := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "tls-fail"}, Spec: zfscsiv1.VolumeSpec{Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, Capacity: 1 << 30, VolumeID: "csi:tank:block:tls-fail", Transport: zfscsiv1.TransportNVMeTCP, NVMeTLSEnabled: true, NVMeTLSPSKSecretName: "vol-psk"}, Status: zfscsiv1.VolumeStatus{MappedInitiators: []zfscsiv1.MappedInitiator{{NodeName: "worker-a", InitiatorID: "worker-a"}}}}
			if err := d.Client.Create(t.Context(), vol); err != nil {
				t.Fatal(err)
			}
			r := d.reconciler()
			r.NVMeTLSPSK = &recordingNVMeTLSPSKProvisioner{err: tc.insertErr}
			r.NVMeTLSSecretReader = d.Client
			_, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: vol.Name}})
			if err == nil {
				t.Fatal("Reconcile() succeeded")
			}
			if strings.Contains(err.Error(), "not-a-psk") {
				t.Fatal("PSK material leaked into reconciliation error")
			}
			if len(d.export.exports) != 0 {
				t.Fatal("PSK failure exposed NVMe target")
			}
		})
	}
}

func TestReconcileNonTLSDoesNotRequirePSK(t *testing.T) {
	d := newTestDeps(t)
	vol := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{Name: "plain-block"}, Spec: zfscsiv1.VolumeSpec{Pool: "tank", Type: zfscsiv1.VolumeTypeBlock, Capacity: 1 << 30, VolumeID: "csi:tank:block:plain-block", Transport: zfscsiv1.TransportNVMeTCP}}
	if err := d.Client.Create(t.Context(), vol); err != nil {
		t.Fatal(err)
	}
	if _, err := d.reconciler().Reconcile(t.Context(), reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: vol.Name}}); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}
	if len(d.export.exports) != 1 {
		t.Fatal("non-TLS volume did not export")
	}
}

func TestReconcileDeletingNVMeTLSVolumeBypassesPSKReadiness(t *testing.T) {
	d := newTestDeps(t)
	vol := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{
		Name: "tls-delete", Finalizers: []string{zfscsiv1.VolumeFinalizer},
	}, Spec: zfscsiv1.VolumeSpec{
		Pool: "tank", Type: zfscsiv1.VolumeTypeBlock,
		VolumeID: "csi:tank:block:tls-delete", Transport: zfscsiv1.TransportNVMeTCP,
		NVMeTLSEnabled: true, NVMeTLSPSKSecretName: "missing-psk",
	}}
	if err := d.Client.Create(t.Context(), vol); err != nil {
		t.Fatal(err)
	}
	if err := d.Client.Delete(t.Context(), vol); err != nil {
		t.Fatal(err)
	}
	d.export.exports["nqn.2026-01.io.randomvariable:zfs-csi:storage-a:00000000-0000-0000-0000-000000000001:block:tls-delete-revoke"] = true
	d.export.mapped["nqn.2026-01.io.randomvariable:zfs-csi:storage-a:00000000-0000-0000-0000-000000000001:block:tls-delete-revoke"] = map[string]bool{}
	provisioner := &recordingNVMeTLSPSKProvisioner{}
	r := d.reconciler()
	r.NVMeTLSPSK = provisioner
	r.NVMeTLSSecretReader = d.Client
	if _, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: vol.Name}}); err != nil {
		t.Fatalf("delete Reconcile() = %v", err)
	}
	if len(provisioner.calls) != 0 {
		t.Fatal("deletion attempted to provision TLS PSK")
	}
	if provisioner.removes != 0 {
		t.Fatal("deletion attempted PSK revocation before terminal target teardown")
	}
}

func TestReconcileDeletingNVMeTLSVolumeDeletesPSKOnlyAfterDestroy(t *testing.T) {
	for _, tc := range []struct {
		name          string
		policy        zfscsiv1.VolumeDeletionPolicy
		backend       func(*testDeps) zfs.Backend
		secretExists  bool
		deleteErr     error
		wantErr       bool
		wantDeletes   int
		wantFinalizer bool
	}{
		{
			name:         "success deletes secret",
			secretExists: true,
			wantDeletes:  1,
		},
		{
			name:         "retain preserves secret",
			policy:       zfscsiv1.VolumeDeletionPolicyRetain,
			secretExists: true,
		},
		{
			name: "destroy failure preserves secret",
			backend: func(d *testDeps) zfs.Backend {
				return &failDestroyZFS{Backend: d.zfsb, err: errors.New("destroy failed")}
			},
			secretExists:  true,
			wantErr:       true,
			wantFinalizer: true,
		},
		{
			name:          "secret delete failure retries with finalizer",
			secretExists:  true,
			deleteErr:     errors.New("delete denied"),
			wantErr:       true,
			wantDeletes:   1,
			wantFinalizer: true,
		},
		{
			name:        "missing secret is idempotent",
			wantDeletes: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := newTestDeps(t)
			secretName := "delete-psk"
			if tc.secretExists {
				if err := d.Client.Create(t.Context(), &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Namespace: "zfs-csi-system", Name: secretName},
				}); err != nil {
					t.Fatal(err)
				}
			}
			vol := &zfscsiv1.Volume{
				ObjectMeta: metav1.ObjectMeta{Name: "tls-secret-delete", Finalizers: []string{zfscsiv1.VolumeFinalizer}},
				Spec: zfscsiv1.VolumeSpec{
					Pool: "tank", Type: zfscsiv1.VolumeTypeFilesystem,
					VolumeID: "csi:tank:filesystem:tls-secret-delete", DeletionPolicy: tc.policy,
					NVMeTLSEnabled: true, NVMeTLSPSKSecretName: secretName,
				},
			}
			if err := d.Client.Create(t.Context(), vol); err != nil {
				t.Fatal(err)
			}
			if err := d.Client.Delete(t.Context(), vol); err != nil {
				t.Fatal(err)
			}

			r := d.reconciler()
			if tc.backend != nil {
				d.setReconcilerBackend(r, tc.backend(d))
			}
			client := &recordingSecretDeleteClient{Client: d.Client, err: tc.deleteErr}
			r.Client = client
			_, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: crclient.ObjectKeyFromObject(vol)})
			if (err != nil) != tc.wantErr {
				t.Fatalf("Reconcile() error = %v, want error %t", err, tc.wantErr)
			}
			if len(client.deleted) != tc.wantDeletes {
				t.Fatalf("Secret deletes = %d, want %d", len(client.deleted), tc.wantDeletes)
			}
			secret := &corev1.Secret{}
			err = d.Client.Get(t.Context(), apimachinerytypes.NamespacedName{Namespace: "zfs-csi-system", Name: secretName}, secret)
			if tc.secretExists && tc.wantDeletes == 1 && tc.deleteErr == nil && !tc.wantFinalizer {
				if !apierrors.IsNotFound(err) {
					t.Fatalf("Secret get error = %v, want NotFound", err)
				}
			} else if tc.secretExists && err != nil {
				t.Fatalf("Secret get error = %v, want preserved", err)
			}
			got := &zfscsiv1.Volume{}
			err = d.Client.Get(t.Context(), crclient.ObjectKeyFromObject(vol), got)
			if tc.wantFinalizer {
				if err != nil || !hasFinalizer(got.Finalizers, zfscsiv1.VolumeFinalizer) {
					t.Fatalf("Volume finalizer retained = %t, get error = %v", err == nil && hasFinalizer(got.Finalizers, zfscsiv1.VolumeFinalizer), err)
				}
			}
		})
	}
}

func TestReconcileDeletingNVMeTLSVolumeRevokesAfterTargetTeardown(t *testing.T) {
	d := newTestDeps(t)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "zfs-csi-system", Name: "delete-psk"},
		Type:       corev1.SecretTypeOpaque,
		Immutable:  boolPtrAgent(true),
		Data:       map[string][]byte{nvmeTLSPSKSecretDataKey: configuredNVMeTLSPSK(t)},
	}
	if err := d.Client.Create(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
	vol := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{
		Name: "tls-delete-revoke", Finalizers: []string{zfscsiv1.VolumeFinalizer}, Annotations: map[string]string{zfscsiv1.ForceDeleteAnnotation: "true"},
	}, Spec: zfscsiv1.VolumeSpec{
		Pool: "tank", Type: zfscsiv1.VolumeTypeBlock,
		VolumeID: "csi:tank:block:tls-delete-revoke", Transport: zfscsiv1.TransportNVMeTCP,
		NVMeTLSEnabled: true, NVMeTLSPSKSecretName: secret.Name,
	}, Status: zfscsiv1.VolumeStatus{MappedInitiators: []zfscsiv1.MappedInitiator{{NodeName: "worker-a", InitiatorID: "worker-a"}}}}
	if err := d.Client.Create(t.Context(), vol); err != nil {
		t.Fatal(err)
	}
	if err := d.Client.Delete(t.Context(), vol); err != nil {
		t.Fatal(err)
	}
	provisioner := &recordingNVMeTLSPSKProvisioner{}
	r := d.reconciler()
	r.NVMeTLSPSK = provisioner
	r.NVMeTLSSecretReader = d.Client
	if _, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: vol.Name}}); err != nil {
		t.Fatalf("delete Reconcile() = %v", err)
	}
	if provisioner.removes != 1 {
		t.Fatalf("PSK revocations = %d, want 1", provisioner.removes)
	}
	if len(provisioner.removalCalls) != 1 || provisioner.removalCalls[0].hostNQN != transport.HostNQN("worker-a") || provisioner.removalCalls[0].subsysNQN == "" {
		t.Fatalf("PSK removal identities = %#v", provisioner.removalCalls)
	}
}

func TestReconcileDeletingNVMeTLSVolumeDoesNotRevokeWhenUnexportFails(t *testing.T) {
	d := newTestDeps(t)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "zfs-csi-system", Name: "unexport-failure-psk"},
		Type:       corev1.SecretTypeOpaque,
		Immutable:  boolPtrAgent(true),
		Data:       map[string][]byte{nvmeTLSPSKSecretDataKey: configuredNVMeTLSPSK(t)},
	}
	if err := d.Client.Create(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
	dataset := "tank/csi/block/tls-delete-unexport-failure"
	if err := d.zfsb.Create(t.Context(), zfs.CreateOptions{
		Name: dataset, Kind: zfs.KindBlock, Capacity: 1 << 30,
	}); err != nil {
		t.Fatal(err)
	}
	vol := &zfscsiv1.Volume{ObjectMeta: metav1.ObjectMeta{
		Name: "tls-delete-unexport-failure", Finalizers: []string{zfscsiv1.VolumeFinalizer}, Annotations: map[string]string{zfscsiv1.ForceDeleteAnnotation: "true"},
	}, Spec: zfscsiv1.VolumeSpec{
		Pool: "tank", Type: zfscsiv1.VolumeTypeBlock,
		VolumeID: "csi:tank:block:tls-delete-unexport-failure", Transport: zfscsiv1.TransportNVMeTCP,
		NVMeTLSEnabled: true, NVMeTLSPSKSecretName: secret.Name,
	}, Status: zfscsiv1.VolumeStatus{MappedInitiators: []zfscsiv1.MappedInitiator{{NodeName: "worker-a", InitiatorID: "worker-a"}}}}
	if err := d.Client.Create(t.Context(), vol); err != nil {
		t.Fatal(err)
	}
	if err := d.Client.Delete(t.Context(), vol); err != nil {
		t.Fatal(err)
	}
	d.export.unexportErr = errors.New("target teardown failed")
	provisioner := &recordingNVMeTLSPSKProvisioner{}
	r := d.reconciler()
	r.NVMeTLSPSK = provisioner
	r.NVMeTLSSecretReader = d.Client
	if _, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: vol.Name}}); err == nil {
		t.Fatal("delete Reconcile() succeeded after target teardown failed")
	}
	if provisioner.removes != 0 {
		t.Fatal("PSK revoked despite failed target teardown")
	}
	if exists, err := d.zfsb.Exists(t.Context(), dataset); err != nil || !exists {
		t.Fatalf("dataset exists after failed unexport = %t, %v; want retained", exists, err)
	}
	current := &zfscsiv1.Volume{}
	if err := d.Get(t.Context(), apimachinerytypes.NamespacedName{Name: vol.Name}, current); err != nil {
		t.Fatalf("get deleting volume: %v", err)
	}
	if !hasFinalizer(current.Finalizers, zfscsiv1.VolumeFinalizer) {
		t.Fatal("failed unexport removed volume finalizer")
	}
	if current.Status.State == zfscsiv1.VolumeStateDestroyed {
		t.Fatal("failed unexport de-adopted the volume")
	}

	d.export.unexportErr = nil
	if _, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: apimachinerytypes.NamespacedName{Name: vol.Name}}); err != nil {
		t.Fatalf("retry delete Reconcile() = %v", err)
	}
	if provisioner.removes != 1 {
		t.Fatalf("PSK revocations after successful retry = %d, want 1", provisioner.removes)
	}
	if exists, err := d.zfsb.Exists(t.Context(), dataset); err != nil || exists {
		t.Fatalf("dataset exists after successful retry = %t, %v; want destroyed", exists, err)
	}
}

func boolPtrAgent(v bool) *bool { return &v }
