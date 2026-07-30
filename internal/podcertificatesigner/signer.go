// Copyright 2026 Naadir Jeewa
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// SPDX-License-Identifier: Apache-2.0

// Package podcertificatesigner implements the narrow PodCertificateRequest
// authority used for NFS client mTLS identities.
package podcertificatesigner

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"time"

	certificatesv1beta1 "k8s.io/api/certificates/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/randomvariable/zfs-csi/internal/tlsca"
)

const (
	// SignerName is intentionally outside kubernetes.io and names one constrained
	// leaf profile: attested node identity with ClientAuth only.
	SignerName = "zfs.csi.randomvariable.co.uk/nfs-client"

	defaultLifetime = 24 * time.Hour
	// Keep NotBefore comfortably inside the API server's exclusive five-minute
	// clock-skew window while tolerating small signer/kube-apiserver skew.
	certificateBackdate = 2 * time.Minute
)

// Reconciler signs only PCR status. Kubernetes admission attests immutable pod,
// service-account and node identity fields and validates proof of possession.
// The signer repeats proof validation so unit and non-apiserver clients fail safe.
type Reconciler struct {
	client.Client
	APIReader        client.Reader
	AuthorityClient  tlsca.SecretClient
	SigningNamespace string
	DriverNamespace  string
	Now              func() time.Time
}

// AuthorityRunnable initializes persistent authority artifacts before the
// signer manager reports readiness. Runtime workloads wait for these Secrets.
type AuthorityRunnable struct {
	Reconciler *Reconciler
	Owners     map[string]string
	Ready      chan struct{}
	Interval   time.Duration
}

func (r *AuthorityRunnable) Start(ctx context.Context) error {
	if err := r.ensureWhenDriverNamespaceExists(ctx); err != nil {
		return err
	}
	close(r.Ready)
	interval := r.Interval
	if interval <= 0 {
		interval = 6 * time.Hour
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := r.Reconciler.EnsureAuthority(ctx, r.Owners); err != nil {
				ctrl.LoggerFrom(ctx).Error(err, "periodic TLS authority ensure failed")
			}
		}
	}
}

func (r *AuthorityRunnable) ensureWhenDriverNamespaceExists(ctx context.Context) error {
	interval := r.Interval
	if interval <= 0 || interval > time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		namespace := &corev1.Namespace{}
		reader := r.Reconciler.APIReader
		if reader == nil {
			reader = r.Reconciler.Client
		}
		err := reader.Get(ctx, types.NamespacedName{Name: r.Reconciler.DriverNamespace}, namespace)
		switch {
		case err == nil:
			return r.Reconciler.EnsureAuthority(ctx, r.Owners)
		case !apierrors.IsNotFound(err):
			return fmt.Errorf("get driver namespace %q: %w", r.Reconciler.DriverNamespace, err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (r *AuthorityRunnable) NeedLeaderElection() bool { return true }

func (r *AuthorityRunnable) ReadyCheck(_ *http.Request) error {
	select {
	case <-r.Ready:
		return nil
	default:
		return errors.New("TLS authority is not initialized")
	}
}

// SetupWithManager registers the v1beta1 PCR controller.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&certificatesv1beta1.PodCertificateRequest{}).
		Complete(r)
}

// Reconcile validates one request before loading signing material, then writes
// either terminal denial or issued status. It never mutates spec or metadata.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	pcr := &certificatesv1beta1.PodCertificateRequest{}
	if err := r.Get(ctx, req.NamespacedName, pcr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if terminal(pcr) {
		return ctrl.Result{}, nil
	}
	// Ignore other signer names. RBAC limits status writes to SignerName, but
	// filtering here avoids treating another signer's requests as malformed.
	if pcr.Spec.SignerName != SignerName {
		return ctrl.Result{}, nil
	}
	publicKey, denial := r.validateRequest(pcr)
	if denial != nil {
		return ctrl.Result{}, r.deny(ctx, pcr, denial.reason, denial.message)
	}

	ca, err := r.loadCA(ctx)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("load private signing CA: %w", err)
	}
	now := r.now().UTC().Truncate(time.Second)
	lifetime := requestedLifetime(pcr)
	leafPEM, leaf, err := ca.SignClientCertificate(string(pcr.Spec.NodeName), publicKey, now.Add(-certificateBackdate), lifetime)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("sign NFS client certificate: %w", err)
	}
	chain := append(append([]byte(nil), leafPEM...), ca.CertPEM...)
	refreshAt := leaf.NotBefore.Add((leaf.NotAfter.Sub(leaf.NotBefore) * 2) / 3)
	pcr.Status.CertificateChain = string(chain)
	pcr.Status.NotBefore = &metav1.Time{Time: leaf.NotBefore}
	pcr.Status.BeginRefreshAt = &metav1.Time{Time: refreshAt}
	pcr.Status.NotAfter = &metav1.Time{Time: leaf.NotAfter}
	pcr.Status.Conditions = []metav1.Condition{{
		Type:               certificatesv1beta1.PodCertificateRequestConditionTypeIssued,
		Status:             metav1.ConditionTrue,
		Reason:             "Issued",
		Message:            "issued constrained NFS ClientAuth certificate",
		ObservedGeneration: pcr.Generation,
		LastTransitionTime: metav1.NewTime(now),
	}}
	if err := r.Status().Update(ctx, pcr); err != nil {
		if latestTerminal, getErr := r.requestIsTerminal(ctx, req.NamespacedName); getErr == nil && latestTerminal {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("update issued PodCertificateRequest status: %w", err)
	}
	return ctrl.Result{}, nil
}

func (r *Reconciler) requestIsTerminal(ctx context.Context, key types.NamespacedName) (bool, error) {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	latest := &certificatesv1beta1.PodCertificateRequest{}
	if err := reader.Get(ctx, key, latest); err != nil {
		return false, err
	}
	return terminal(latest), nil
}

// EnsureAuthority preserves public CA and server-leaf Secret runtime behavior
// while keeping every private-key operation in signer process.
func (r *Reconciler) EnsureAuthority(ctx context.Context, owners map[string]string) error {
	authorityClient := r.AuthorityClient
	if authorityClient == nil {
		authorityClient = r.Client
	}
	ca, err := tlsca.EnsureCAInSigningNamespace(ctx, authorityClient, r.SigningNamespace, r.DriverNamespace)
	if err != nil {
		return fmt.Errorf("ensure private signing CA: %w", err)
	}
	if err := tlsca.EnsurePublicCA(ctx, authorityClient, r.DriverNamespace, ca); err != nil {
		return fmt.Errorf("ensure public CA: %w", err)
	}
	for owner, endpoint := range owners {
		if err := tlsca.EnsureNodeLeaf(ctx, authorityClient, r.DriverNamespace, owner, endpoint, ca); err != nil {
			return fmt.Errorf("ensure server leaf for %q: %w", owner, err)
		}
	}
	return nil
}

func (r *Reconciler) loadCA(ctx context.Context) (*tlsca.CA, error) {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	caSecret := &corev1.Secret{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: r.SigningNamespace, Name: tlsca.CASecretName}, caSecret); err != nil {
		return nil, fmt.Errorf("read private signing CA: %w", err)
	}
	ca, err := tlsca.LoadCA(caSecret.Data[corev1.TLSCertKey], caSecret.Data[corev1.TLSPrivateKeyKey])
	if err != nil {
		return nil, fmt.Errorf("load private signing CA: %w", err)
	}
	return ca, nil
}

type requestDenial struct {
	reason  string
	message string
}

func (r *Reconciler) validateRequest(pcr *certificatesv1beta1.PodCertificateRequest) (*ecdsa.PublicKey, *requestDenial) {
	if pcr.Namespace != r.DriverNamespace || pcr.Spec.ServiceAccountName != "zfs-csi-node" {
		return nil, denyRequest("UnauthorizedRequester", fmt.Sprintf("request must target namespace %q and service account %q", r.DriverNamespace, "zfs-csi-node"))
	}
	if len(validation.IsDNS1123Subdomain(pcr.Namespace)) != 0 || pcr.Spec.PodName == "" || pcr.Spec.PodUID == "" || pcr.Spec.ServiceAccountName == "" || pcr.Spec.ServiceAccountUID == "" || pcr.Spec.NodeName == "" || pcr.Spec.NodeUID == "" {
		return nil, denyRequest("InvalidIdentity", "complete attested pod, service-account, and node identity is required")
	}
	if len(validation.IsDNS1123Subdomain(pcr.Spec.PodName)) != 0 || len(validation.IsDNS1123Subdomain(pcr.Spec.ServiceAccountName)) != 0 {
		return nil, denyRequest("InvalidIdentity", "podName and serviceAccountName must be DNS1123 subdomains")
	}
	if len(validation.IsDNS1123Subdomain(string(pcr.Spec.NodeName))) != 0 {
		return nil, denyRequest("InvalidIdentity", "nodeName must be a DNS1123 subdomain")
	}
	if len(pcr.Spec.UnverifiedUserAnnotations) != 0 {
		return nil, denyRequest(certificatesv1beta1.PodCertificateRequestConditionInvalidUserConfig, "unverifiedUserAnnotations are not supported")
	}
	if pcr.Spec.MaxExpirationSeconds == nil || *pcr.Spec.MaxExpirationSeconds < 3600 || *pcr.Spec.MaxExpirationSeconds > 91*24*60*60 {
		return nil, denyRequest("InvalidLifetime", "maxExpirationSeconds must be between 3600 and 7862400")
	}
	return requestPublicKey(pcr)
}

func denyRequest(reason, message string) *requestDenial {
	return &requestDenial{reason: reason, message: message}
}

func requestPublicKey(pcr *certificatesv1beta1.PodCertificateRequest) (*ecdsa.PublicKey, *requestDenial) {
	spec := &pcr.Spec
	if len(spec.StubPKCS10Request) != 0 {
		if len(spec.PKIXPublicKey) != 0 || len(spec.ProofOfPossession) != 0 {
			return nil, denyRequest("MalformedRequest", "stubPKCS10Request cannot be combined with deprecated key fields")
		}
		csr, err := x509.ParseCertificateRequest(spec.StubPKCS10Request)
		if err != nil || csr.CheckSignature() != nil || !emptyCSR(csr) {
			return nil, denyRequest("MalformedRequest", "stubPKCS10Request must be a signed empty CSR")
		}
		return supportedPublicKey(csr.PublicKey)
	}
	if len(spec.PKIXPublicKey) == 0 || len(spec.ProofOfPossession) == 0 {
		return nil, denyRequest("MalformedRequest", "deprecated encoding requires both pkixPublicKey and proofOfPossession")
	}
	publicKey, err := x509.ParsePKIXPublicKey(spec.PKIXPublicKey)
	if err != nil {
		return nil, denyRequest("MalformedRequest", "pkixPublicKey is not valid PKIX public-key data")
	}
	key, denial := supportedPublicKey(publicKey)
	if denial != nil {
		return nil, denial
	}
	digest := sha256.Sum256([]byte(pcr.Spec.PodUID))
	if !ecdsa.VerifyASN1(key, digest[:], spec.ProofOfPossession) {
		return nil, denyRequest("MalformedRequest", "proofOfPossession does not verify against podUID and pkixPublicKey")
	}
	return key, nil
}

func supportedPublicKey(publicKey any) (*ecdsa.PublicKey, *requestDenial) {
	key, ok := publicKey.(*ecdsa.PublicKey)
	if !ok || key.Curve != elliptic.P256() {
		return nil, denyRequest(certificatesv1beta1.PodCertificateRequestConditionUnsupportedKeyType, "only ECDSAP256 is supported")
	}
	return key, nil
}

func emptyCSR(csr *x509.CertificateRequest) bool {
	return csr.Subject.String() == "" && len(csr.DNSNames) == 0 && len(csr.EmailAddresses) == 0 && len(csr.IPAddresses) == 0 && len(csr.URIs) == 0 && len(csr.Extensions) == 0 && len(csr.ExtraExtensions) == 0
}

func requestedLifetime(pcr *certificatesv1beta1.PodCertificateRequest) time.Duration {
	lifetime := time.Duration(*pcr.Spec.MaxExpirationSeconds) * time.Second
	if lifetime > defaultLifetime {
		return defaultLifetime
	}
	return lifetime
}

func terminal(pcr *certificatesv1beta1.PodCertificateRequest) bool {
	if pcr.Status.CertificateChain != "" {
		return true
	}
	for _, condition := range pcr.Status.Conditions {
		if condition.Status == metav1.ConditionTrue && (condition.Type == certificatesv1beta1.PodCertificateRequestConditionTypeIssued || condition.Type == certificatesv1beta1.PodCertificateRequestConditionTypeDenied || condition.Type == certificatesv1beta1.PodCertificateRequestConditionTypeFailed) {
			return true
		}
	}
	return false
}

func (r *Reconciler) deny(ctx context.Context, pcr *certificatesv1beta1.PodCertificateRequest, reason, message string) error {
	pcr.Status = certificatesv1beta1.PodCertificateRequestStatus{Conditions: []metav1.Condition{{
		Type:               certificatesv1beta1.PodCertificateRequestConditionTypeDenied,
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: pcr.Generation,
		LastTransitionTime: metav1.NewTime(r.now()),
	}}}
	if err := r.Status().Update(ctx, pcr); err != nil {
		return fmt.Errorf("update denied PodCertificateRequest status: %w", err)
	}
	return nil
}

func (r *Reconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// AddToScheme registers certificates.k8s.io/v1beta1 for signer managers.
func AddToScheme(scheme *runtime.Scheme) error {
	return certificatesv1beta1.AddToScheme(scheme)
}
