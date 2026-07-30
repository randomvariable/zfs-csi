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

package tlsca

import (
	"bytes"
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apimachinerytypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func fakeClient() crclient.Client {
	return fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()
}

func TestEnsureCAIdempotent(t *testing.T) {
	ctx := context.Background()
	c := fakeClient()

	ca1, err := EnsureCA(ctx, c, "zfs")
	if err != nil {
		t.Fatalf("EnsureCA #1: %v", err)
	}
	ca2, err := EnsureCA(ctx, c, "zfs")
	if err != nil {
		t.Fatalf("EnsureCA #2: %v", err)
	}
	// Second call must REUSE the persisted CA, not mint a new one.
	if string(ca1.CertPEM) != string(ca2.CertPEM) {
		t.Fatal("EnsureCA re-minted the CA instead of reusing the Secret")
	}

	// Exactly one CA Secret exists.
	sec := &corev1.Secret{}
	if err := c.Get(ctx, apimachinerytypes.NamespacedName{Name: CASecretName, Namespace: "zfs"}, sec); err != nil {
		t.Fatalf("get ca secret: %v", err)
	}
	if len(sec.Data[dataCACert]) == 0 {
		t.Fatal("ca secret missing ca.crt")
	}
}

func TestServerSecretNameRequiresDNSSubdomainOwner(t *testing.T) {
	for _, owner := range []string{"Storage-A", "-storage", "storage-", "storage..a"} {
		if _, err := ServerSecretName(owner); err == nil {
			t.Errorf("ServerSecretName(%q) succeeded", owner)
		}
	}
	name, err := ServerSecretName("storage-a")
	if err != nil || name != "zfs-csi-tls-server-storage-a" {
		t.Fatalf("ServerSecretName() = %q, %v", name, err)
	}
	name, err = ServerSecretName("ip-10-0-92-202.ec2.internal")
	if err != nil || name != "zfs-csi-tls-server-ip-10-0-92-202.ec2.internal" {
		t.Fatalf("ServerSecretName(Node) = %q, %v", name, err)
	}
}

func TestEnsureNodeLeafMintsAndReuses(t *testing.T) {
	ctx := context.Background()
	c := fakeClient()
	ca, err := EnsureCA(ctx, c, "zfs")
	if err != nil {
		t.Fatal(err)
	}

	if err := EnsureNodeLeaf(ctx, c, "zfs", "storage-a", "10.0.0.5", ca); err != nil {
		t.Fatalf("EnsureNodeLeaf mint: %v", err)
	}
	sec := &corev1.Secret{}
	name, _ := ServerSecretName("storage-a")
	if err := c.Get(ctx, apimachinerytypes.NamespacedName{Name: name, Namespace: "zfs"}, sec); err != nil {
		t.Fatalf("get server secret: %v", err)
	}
	first := string(sec.Data[dataTLSCert])
	if !LeafValidFor(sec.Data[dataTLSCert], "10.0.0.5", 0) {
		t.Fatal("minted leaf is not valid for the portal IP")
	}

	// Second call with the SAME portal must reuse (no re-mint).
	if err := EnsureNodeLeaf(ctx, c, "zfs", "storage-a", "10.0.0.5", ca); err != nil {
		t.Fatalf("EnsureNodeLeaf reuse: %v", err)
	}
	_ = c.Get(ctx, apimachinerytypes.NamespacedName{Name: name, Namespace: "zfs"}, sec)
	if string(sec.Data[dataTLSCert]) != first {
		t.Fatal("EnsureNodeLeaf re-minted despite an unchanged portal")
	}
}

func TestEnsureNodeLeafRemintsOnIPChange(t *testing.T) {
	ctx := context.Background()
	c := fakeClient()
	ca, err := EnsureCA(ctx, c, "zfs")
	if err != nil {
		t.Fatal(err)
	}
	if err := EnsureNodeLeaf(ctx, c, "zfs", "storage-a", "10.0.0.5", ca); err != nil {
		t.Fatal(err)
	}
	sec := &corev1.Secret{}
	name, _ := ServerSecretName("storage-a")
	_ = c.Get(ctx, apimachinerytypes.NamespacedName{Name: name, Namespace: "zfs"}, sec)
	old := string(sec.Data[dataTLSCert])

	// Node IP changed (ephemeral CAPA node) -> must re-mint for the new portal.
	if err := EnsureNodeLeaf(ctx, c, "zfs", "storage-a", "10.0.0.9", ca); err != nil {
		t.Fatal(err)
	}
	_ = c.Get(ctx, apimachinerytypes.NamespacedName{Name: name, Namespace: "zfs"}, sec)
	if string(sec.Data[dataTLSCert]) == old {
		t.Fatal("leaf was not re-minted after the portal IP changed")
	}
	if !LeafValidFor(sec.Data[dataTLSCert], "10.0.0.9", 0) {
		t.Fatal("re-minted leaf is not valid for the new portal IP")
	}
	if LeafValidFor(sec.Data[dataTLSCert], "10.0.0.5", 0) {
		t.Fatal("re-minted leaf must not still certify the old IP")
	}
}

func TestEnsureNodeLeafKeepsDistinctOwnerLeaves(t *testing.T) {
	ctx := context.Background()
	c := fakeClient()
	ca, err := EnsureCA(ctx, c, "zfs")
	if err != nil {
		t.Fatal(err)
	}
	for owner, endpoint := range map[string]string{"storage-a": "10.0.0.5", "storage-b": "10.0.0.9"} {
		if err := EnsureNodeLeaf(ctx, c, "zfs", owner, endpoint, ca); err != nil {
			t.Fatalf("EnsureNodeLeaf(%q): %v", owner, err)
		}
	}
	for owner, endpoint := range map[string]string{"storage-a": "10.0.0.5", "storage-b": "10.0.0.9"} {
		name, err := ServerSecretName(owner)
		if err != nil {
			t.Fatal(err)
		}
		sec := &corev1.Secret{}
		if err := c.Get(ctx, apimachinerytypes.NamespacedName{Name: name, Namespace: "zfs"}, sec); err != nil {
			t.Fatalf("get %q: %v", name, err)
		}
		if !LeafValidFor(sec.Data[dataTLSCert], endpoint, 0) {
			t.Fatalf("owner %q leaf does not certify %q", owner, endpoint)
		}
	}
}

func TestEnsureCAReusesPreexistingSecret(t *testing.T) {
	ctx := context.Background()
	c := fakeClient()
	// Pre-seed a CA, then confirm EnsureCA adopts it verbatim.
	ca, err := NewCA("preexisting")
	if err != nil {
		t.Fatal(err)
	}
	seed := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: CASecretName, Namespace: "zfs"},
		Data: map[string][]byte{
			dataTLSCert: ca.CertPEM,
			dataTLSKey:  ca.KeyPEM,
			dataCACert:  ca.CertPEM,
		},
	}
	if err := c.Create(ctx, seed); err != nil {
		t.Fatal(err)
	}
	got, err := EnsureCA(ctx, c, "zfs")
	if err != nil {
		t.Fatal(err)
	}
	if string(got.CertPEM) != string(ca.CertPEM) {
		t.Fatal("EnsureCA did not adopt the pre-existing CA Secret")
	}
}

func TestEnsurePublicCAContainsNoPrivateKey(t *testing.T) {
	ctx := context.Background()
	c := fakeClient()
	ca, err := EnsureCA(ctx, c, "zfs")
	if err != nil {
		t.Fatal(err)
	}
	if err := EnsurePublicCA(ctx, c, "zfs", ca); err != nil {
		t.Fatal(err)
	}
	sec := &corev1.Secret{}
	if err := c.Get(ctx, apimachinerytypes.NamespacedName{Name: CAPublicSecretName, Namespace: "zfs"}, sec); err != nil {
		t.Fatal(err)
	}
	if len(sec.Data[dataCACert]) == 0 || len(sec.Data[dataTLSKey]) != 0 || len(sec.Data) != 1 {
		t.Fatalf("public CA Secret data = %v, want only ca.crt", sec.Data)
	}
}

func TestEnsureCAInSigningNamespaceMigratesLegacyCAWithoutRotation(t *testing.T) {
	ctx := context.Background()
	c := fakeClient()
	legacy, err := EnsureCA(ctx, c, "zfs-csi")
	if err != nil {
		t.Fatal(err)
	}
	got, err := EnsureCAInSigningNamespace(ctx, c, "zfs-csi-signing", "zfs-csi")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.CertPEM, legacy.CertPEM) || !bytes.Equal(got.KeyPEM, legacy.KeyPEM) {
		t.Fatal("migration changed active CA")
	}
	private := &corev1.Secret{}
	if err := c.Get(ctx, apimachinerytypes.NamespacedName{Namespace: "zfs-csi-signing", Name: CASecretName}, private); err != nil {
		t.Fatalf("get signing CA: %v", err)
	}
	if len(private.Data[dataTLSKey]) == 0 {
		t.Fatal("signing CA lacks private key")
	}
	if err := c.Get(ctx, apimachinerytypes.NamespacedName{Namespace: "zfs-csi", Name: CASecretName}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Fatalf("legacy private CA remains in runtime namespace: %v", err)
	}
}

func TestValidateSigningNamespace(t *testing.T) {
	for _, tc := range []struct {
		driver, signing string
		valid           bool
	}{
		{"zfs-csi", "zfs-csi-signing", true},
		{"zfs-csi", "zfs-csi", false},
		{"zfs-csi", "", false},
		{"zfs-csi", "Bad", false},
	} {
		err := ValidateSigningNamespace(tc.driver, tc.signing)
		if (err == nil) != tc.valid {
			t.Errorf("ValidateSigningNamespace(%q, %q) error=%v", tc.driver, tc.signing, err)
		}
	}
}
