// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

package podcertificatesigner

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"testing"
	"time"

	certificatesv1beta1 "k8s.io/api/certificates/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/randomvariable/zfs-csi/internal/tlsca"
)

func TestReconcileIssuesConstrainedLeafToAttestedNode(t *testing.T) {
	pcr, key := validRequest(t)
	ca := mustCA(t)
	r := testReconciler(t, pcr, ca)
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: pcr.Namespace, Name: pcr.Name}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := &certificatesv1beta1.PodCertificateRequest{}
	if err := r.Get(t.Context(), types.NamespacedName{Namespace: pcr.Namespace, Name: pcr.Name}, got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.Conditions) != 1 || got.Status.Conditions[0].Type != certificatesv1beta1.PodCertificateRequestConditionTypeIssued {
		t.Fatalf("conditions = %#v", got.Status.Conditions)
	}
	leafBlock, rest := pem.Decode([]byte(got.Status.CertificateChain))
	rootBlock, trailing := pem.Decode(rest)
	if leafBlock == nil || rootBlock == nil || len(trailing) != 0 || leafBlock.Type != "CERTIFICATE" || rootBlock.Type != "CERTIFICATE" {
		t.Fatalf("certificateChain is not leaf-to-root PEM")
	}
	leaf, err := x509.ParseCertificate(leafBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(leaf.ExtKeyUsage) != 1 || leaf.ExtKeyUsage[0] != x509.ExtKeyUsageClientAuth || leaf.IsCA || leaf.Subject.CommonName != "worker-a" || len(leaf.DNSNames) != 1 || leaf.DNSNames[0] != "worker-a" {
		t.Fatalf("leaf profile = %#v", leaf)
	}
	if !leaf.PublicKey.(*ecdsa.PublicKey).Equal(&key.PublicKey) {
		t.Fatal("leaf key does not match stubPKCS10Request")
	}
	if got.Status.NotBefore == nil || got.Status.NotAfter == nil || got.Status.BeginRefreshAt == nil || !got.Status.NotBefore.Time.Equal(leaf.NotBefore) || !got.Status.NotAfter.Time.Equal(leaf.NotAfter) {
		t.Fatalf("status timestamps do not match leaf")
	}
	requested := time.Duration(*pcr.Spec.MaxExpirationSeconds) * time.Second
	if span := leaf.NotAfter.Sub(leaf.NotBefore); span != requested {
		t.Fatalf("certificate validity span = %s, want exactly %s", span, requested)
	}
	issuedAt := r.Now()
	lowerBound := issuedAt.Add(-5 * time.Minute)
	upperBound := issuedAt.Add(5 * time.Minute)
	if !got.Status.NotBefore.Time.After(lowerBound) || !got.Status.NotBefore.Time.Before(upperBound) {
		t.Fatalf("notBefore = %s, want strictly inside apiserver window (%s, %s)", got.Status.NotBefore.Time, lowerBound, upperBound)
	}
	if want := issuedAt.Add(-certificateBackdate); !got.Status.NotBefore.Time.Equal(want) {
		t.Fatalf("notBefore = %s, want conservative backdate %s", got.Status.NotBefore.Time, want)
	}
	wantRefreshAt := leaf.NotBefore.Add(requested * 2 / 3)
	if !got.Status.BeginRefreshAt.Time.Equal(wantRefreshAt) {
		t.Fatalf("beginRefreshAt = %s, want %s", got.Status.BeginRefreshAt.Time, wantRefreshAt)
	}
	if !got.Status.BeginRefreshAt.Time.After(got.Status.NotBefore.Time) || !got.Status.BeginRefreshAt.Time.Before(got.Status.NotAfter.Time) {
		t.Fatalf("beginRefreshAt = %s, want strictly between notBefore %s and notAfter %s", got.Status.BeginRefreshAt.Time, got.Status.NotBefore.Time, got.Status.NotAfter.Time)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca.Cert)
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, DNSName: "worker-a", KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, CurrentTime: leaf.NotBefore.Add(time.Minute)}); err != nil {
		t.Fatalf("verify issued leaf: %v", err)
	}
}

func TestReconcileIssuesSameProfileFromDeprecatedEncoding(t *testing.T) {
	pcr, key := validDeprecatedRequest(t)
	ca := mustCA(t)
	r := testReconciler(t, pcr, ca)
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: pcr.Namespace, Name: pcr.Name}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got := &certificatesv1beta1.PodCertificateRequest{}
	if err := r.Get(t.Context(), types.NamespacedName{Namespace: pcr.Namespace, Name: pcr.Name}, got); err != nil {
		t.Fatal(err)
	}
	leafBlock, _ := pem.Decode([]byte(got.Status.CertificateChain))
	if leafBlock == nil {
		t.Fatal("certificateChain lacks leaf certificate")
	}
	leaf, err := x509.ParseCertificate(leafBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if !leaf.PublicKey.(*ecdsa.PublicKey).Equal(&key.PublicKey) {
		t.Fatal("leaf key does not match deprecated pkixPublicKey")
	}
}

func TestRequestPublicKeyKnownAnswerEncodings(t *testing.T) {
	stubRequest, stubKey := validRequest(t)
	legacyRequest, legacyKey := validDeprecatedRequest(t)
	for name, tc := range map[string]struct {
		request *certificatesv1beta1.PodCertificateRequest
		key     *ecdsa.PrivateKey
	}{
		"stub PKCS10":     {request: stubRequest, key: stubKey},
		"deprecated pair": {request: legacyRequest, key: legacyKey},
	} {
		t.Run(name, func(t *testing.T) {
			r := &Reconciler{DriverNamespace: "zfs-csi-system"}
			got, denial := r.validateRequest(tc.request)
			if denial != nil {
				t.Fatalf("validateRequest denied valid encoding: %#v", denial)
			}
			if !got.Equal(&tc.key.PublicKey) {
				t.Fatal("extracted public key mismatch")
			}
		})
	}
}

func TestRequestPublicKeyRejectsConflictsAndMalformedDeprecatedEncoding(t *testing.T) {
	tests := map[string]func(*certificatesv1beta1.PodCertificateRequest){
		"conflicting encodings": func(p *certificatesv1beta1.PodCertificateRequest) {
			setDeprecatedEncoding(t, p, mustP256Key(t))
			p.Spec.StubPKCS10Request = csrFor(t, mustP256Key(t), "")
		},
		"missing proof": func(p *certificatesv1beta1.PodCertificateRequest) {
			setDeprecatedEncoding(t, p, mustP256Key(t))
			p.Spec.ProofOfPossession = nil
		},
		"missing public key": func(p *certificatesv1beta1.PodCertificateRequest) {
			setDeprecatedEncoding(t, p, mustP256Key(t))
			p.Spec.PKIXPublicKey = nil
		},
		"malformed public key": func(p *certificatesv1beta1.PodCertificateRequest) {
			p.Spec.StubPKCS10Request = nil
			p.Spec.PKIXPublicKey = []byte("bad")
			p.Spec.ProofOfPossession = []byte("bad")
		},
		"wrong proof": func(p *certificatesv1beta1.PodCertificateRequest) {
			setDeprecatedEncoding(t, p, mustP256Key(t))
			p.Spec.ProofOfPossession[len(p.Spec.ProofOfPossession)-1] ^= 1
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			pcr, _ := validRequest(t)
			mutate(pcr)
			if _, denial := requestPublicKey(pcr); denial == nil || denial.reason != "MalformedRequest" {
				t.Fatalf("requestPublicKey denial = %#v, want MalformedRequest", denial)
			}
		})
	}
}

func TestRequestedLifetimeClampsToDriverPolicy(t *testing.T) {
	pcr, _ := validRequest(t)
	requested := int32(91 * 24 * 60 * 60)
	pcr.Spec.MaxExpirationSeconds = &requested
	r := &Reconciler{DriverNamespace: "zfs-csi-system"}
	if _, denial := r.validateRequest(pcr); denial != nil {
		t.Fatalf("legitimate 91-day request denied: %#v", denial)
	}
	if got := requestedLifetime(pcr); got != 24*time.Hour {
		t.Fatalf("requestedLifetime = %s, want 24h policy clamp", got)
	}
}

func TestValidateRequestStrictDenials(t *testing.T) {
	tests := map[string]struct {
		mutate func(*certificatesv1beta1.PodCertificateRequest)
		want   string
	}{
		"identity":               {func(p *certificatesv1beta1.PodCertificateRequest) { p.Spec.NodeUID = "" }, "InvalidIdentity"},
		"malformed pod identity": {func(p *certificatesv1beta1.PodCertificateRequest) { p.Spec.PodName = "bad/name" }, "InvalidIdentity"},
		"annotations": {func(p *certificatesv1beta1.PodCertificateRequest) {
			p.Spec.UnverifiedUserAnnotations = map[string]string{"example.test/a": "b"}
		}, "InvalidUnverifiedUserAnnotations"},
		"malformed CSR": {func(p *certificatesv1beta1.PodCertificateRequest) { p.Spec.StubPKCS10Request = []byte("bad") }, "MalformedRequest"},
		"CSR subject": {func(p *certificatesv1beta1.PodCertificateRequest) {
			p.Spec.StubPKCS10Request = csrFor(t, mustP256Key(t), "unsafe")
		}, "MalformedRequest"},
		"unsupported key": {func(p *certificatesv1beta1.PodCertificateRequest) {
			key, err := rsa.GenerateKey(rand.Reader, 3072)
			if err != nil {
				t.Fatal(err)
			}
			p.Spec.StubPKCS10Request = csrFor(t, key, "")
		}, "UnsupportedKeyType"},
		"short lifetime": {func(p *certificatesv1beta1.PodCertificateRequest) { v := int32(3599); p.Spec.MaxExpirationSeconds = &v }, "InvalidLifetime"},
		"long lifetime": {func(p *certificatesv1beta1.PodCertificateRequest) {
			v := int32(91*24*60*60 + 1)
			p.Spec.MaxExpirationSeconds = &v
		}, "InvalidLifetime"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			pcr, _ := validRequest(t)
			tt.mutate(pcr)
			r := &Reconciler{DriverNamespace: "zfs-csi-system"}
			if _, denial := r.validateRequest(pcr); denial == nil || denial.reason != tt.want {
				t.Fatalf("validateRequest denial = %#v, want reason %q", denial, tt.want)
			}
		})
	}
}

func TestValidateRequestRejectsUnauthorizedRequester(t *testing.T) {
	tests := map[string]func(*certificatesv1beta1.PodCertificateRequest){
		"namespace":       func(p *certificatesv1beta1.PodCertificateRequest) { p.Namespace = "other-system" },
		"service account": func(p *certificatesv1beta1.PodCertificateRequest) { p.Spec.ServiceAccountName = "other-node" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			pcr, _ := validRequest(t)
			mutate(pcr)
			r := &Reconciler{DriverNamespace: "zfs-csi-system"}
			_, denial := r.validateRequest(pcr)
			if denial == nil || denial.reason != "UnauthorizedRequester" {
				t.Fatalf("validateRequest denial = %#v", denial)
			}
		})
	}
}

func TestReconcileDeniesBeforeReadingPrivateCA(t *testing.T) {
	pcr, _ := validRequest(t)
	pcr.Spec.UnverifiedUserAnnotations = map[string]string{"example.test/unsafe": "true"}
	r := testReconciler(t, pcr, nil)
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: pcr.Namespace, Name: pcr.Name}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got := &certificatesv1beta1.PodCertificateRequest{}
	if err := r.Get(t.Context(), types.NamespacedName{Namespace: pcr.Namespace, Name: pcr.Name}, got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.Conditions) != 1 || got.Status.Conditions[0].Type != certificatesv1beta1.PodCertificateRequestConditionTypeDenied || got.Status.Conditions[0].Reason != certificatesv1beta1.PodCertificateRequestConditionInvalidUserConfig || got.Status.CertificateChain != "" {
		t.Fatalf("denied status = %#v", got.Status)
	}
}

func TestReconcileIgnoresUnsupportedSigner(t *testing.T) {
	pcr, _ := validRequest(t)
	pcr.Spec.SignerName = "other.example.test/nfs-client"
	r := testReconciler(t, pcr, nil)
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: pcr.Namespace, Name: pcr.Name}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got := &certificatesv1beta1.PodCertificateRequest{}
	if err := r.Get(t.Context(), types.NamespacedName{Namespace: pcr.Namespace, Name: pcr.Name}, got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.Conditions) != 0 || got.Status.CertificateChain != "" {
		t.Fatalf("unsupported signer status changed: %#v", got.Status)
	}
}

func TestEnsureAuthorityUsesDedicatedUncachedClient(t *testing.T) {
	ca := mustCA(t)
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	authorityClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: tlsca.CASecretName, Namespace: "zfs-csi-signing"},
		Data:       map[string][]byte{corev1.TLSCertKey: ca.CertPEM, corev1.TLSPrivateKeyKey: ca.KeyPEM},
	}).Build()
	r := &Reconciler{
		AuthorityClient:  authorityClient,
		SigningNamespace: "zfs-csi-signing",
		DriverNamespace:  "zfs-csi-system",
	}
	if err := r.EnsureAuthority(t.Context(), map[string]string{"storage-a": "10.0.0.5"}); err != nil {
		t.Fatalf("EnsureAuthority: %v", err)
	}
	for _, name := range []string{tlsca.CAPublicSecretName, "zfs-csi-tls-server-storage-a"} {
		if err := authorityClient.Get(t.Context(), types.NamespacedName{Namespace: "zfs-csi-system", Name: name}, &corev1.Secret{}); err != nil {
			t.Fatalf("authority Secret %q: %v", name, err)
		}
	}
}

func TestAuthorityRunnablePeriodicallyRenewsServerLeaf(t *testing.T) {
	ca := mustCA(t)
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	authorityClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: tlsca.CASecretName, Namespace: "zfs-csi-signing"},
		Data:       map[string][]byte{corev1.TLSCertKey: ca.CertPEM, corev1.TLSPrivateKeyKey: ca.KeyPEM},
	}, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "zfs-csi-system"}}).Build()
	r := &Reconciler{APIReader: authorityClient, AuthorityClient: authorityClient, SigningNamespace: "zfs-csi-signing", DriverNamespace: "zfs-csi-system"}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	runnable := &AuthorityRunnable{Reconciler: r, Owners: map[string]string{"storage-a": "10.0.0.5"}, Ready: make(chan struct{}), Interval: time.Millisecond}
	go func() { done <- runnable.Start(ctx) }()
	<-runnable.Ready
	leaf := &corev1.Secret{}
	key := types.NamespacedName{Namespace: "zfs-csi-system", Name: "zfs-csi-tls-server-storage-a"}
	if err := authorityClient.Get(t.Context(), key, leaf); err != nil {
		t.Fatal(err)
	}
	leaf.Data[corev1.TLSCertKey] = []byte("invalid")
	if err := authorityClient.Update(t.Context(), leaf); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		if err := authorityClient.Get(t.Context(), key, leaf); err == nil && string(leaf.Data[corev1.TLSCertKey]) != "invalid" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("periodic authority ensure did not repair server leaf")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func validRequest(t *testing.T) (*certificatesv1beta1.PodCertificateRequest, *ecdsa.PrivateKey) {
	t.Helper()
	key := mustP256Key(t)
	lifetime := int32(3600)
	return &certificatesv1beta1.PodCertificateRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "request", Namespace: "zfs-csi-system", Generation: 1},
		Spec: certificatesv1beta1.PodCertificateRequestSpec{
			SignerName: SignerName, PodName: "zfs-csi-node-a", PodUID: "pod-uid",
			ServiceAccountName: "zfs-csi-node", ServiceAccountUID: "sa-uid",
			NodeName: "worker-a", NodeUID: "node-uid", MaxExpirationSeconds: &lifetime,
			StubPKCS10Request: csrFor(t, key, ""),
		},
	}, key
}

func validDeprecatedRequest(t *testing.T) (*certificatesv1beta1.PodCertificateRequest, *ecdsa.PrivateKey) {
	t.Helper()
	pcr, _ := validRequest(t)
	key := mustP256Key(t)
	setDeprecatedEncoding(t, pcr, key)
	return pcr, key
}

func setDeprecatedEncoding(t *testing.T, pcr *certificatesv1beta1.PodCertificateRequest, key *ecdsa.PrivateKey) {
	t.Helper()
	pkixPublicKey, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(pcr.Spec.PodUID))
	proof, err := ecdsa.SignASN1(rand.Reader, key, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	pcr.Spec.StubPKCS10Request = nil
	pcr.Spec.PKIXPublicKey = pkixPublicKey
	pcr.Spec.ProofOfPossession = proof
}

func csrFor(t *testing.T, key any, commonName string) []byte {
	t.Helper()
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{Subject: pkixName(commonName)}, key)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

func pkixName(commonName string) pkix.Name { return pkix.Name{CommonName: commonName} }

func mustP256Key(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func mustCA(t *testing.T) *tlsca.CA {
	t.Helper()
	ca, err := tlsca.NewCA("test")
	if err != nil {
		t.Fatal(err)
	}
	return ca
}

func testReconciler(t *testing.T, pcr *certificatesv1beta1.PodCertificateRequest, ca *tlsca.CA) *Reconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := certificatesv1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	objects := []runtime.Object{pcr}
	if ca != nil {
		objects = append(objects, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: tlsca.CASecretName, Namespace: "zfs-csi-signing"}, Data: map[string][]byte{corev1.TLSCertKey: ca.CertPEM, corev1.TLSPrivateKeyKey: ca.KeyPEM}})
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).WithStatusSubresource(pcr).Build()
	now := time.Now().UTC().Truncate(time.Second)
	return &Reconciler{Client: c, APIReader: c, SigningNamespace: "zfs-csi-signing", DriverNamespace: "zfs-csi-system", Now: func() time.Time { return now }}
}
