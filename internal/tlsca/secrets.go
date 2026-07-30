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
	"crypto/ecdsa"
	"crypto/x509"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apimachinerytypes "k8s.io/apimachinery/pkg/types"
	validation "k8s.io/apimachinery/pkg/util/validation"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// SecretClient is the narrow uncached Kubernetes API surface required to keep
// TLS authority state correct. A future cert-manager issuer can implement this
// contract without changing the filesystem provisioning path.
type SecretClient interface {
	Get(context.Context, apimachinerytypes.NamespacedName, crclient.Object, ...crclient.GetOption) error
	Create(context.Context, crclient.Object, ...crclient.CreateOption) error
	Update(context.Context, crclient.Object, ...crclient.UpdateOption) error
	Delete(context.Context, crclient.Object, ...crclient.DeleteOption) error
}

// Secret names + data keys the chart's tlshd sidecars mount. Kept here so the Go
// side and the chart agree on one source of truth.
const (
	// CASecretName holds the internal CA cert (+ key). The chart mounts it into
	// controller-owned namespace only. It is never mounted on nodes.
	CASecretName = "zfs-csi-tls-ca"
	// CAPublicSecretName contains only the CA certificate for node bootstrap and
	// server tlshd trust stores. Keeping this separate prevents accidental
	// key-bearing Secret mounts in node manifests.
	CAPublicSecretName = "zfs-csi-tls-ca-public"
	// serverSecretPrefix identifies storage-owner-specific NFS TLS leaf Secrets.
	serverSecretPrefix = "zfs-csi-tls-server-"
	// Data keys (tls.crt/tls.key mirror kubernetes.io/tls convention; ca.crt is
	// the CA bundle tlshd trusts).
	dataCACert  = "ca.crt"
	dataTLSCert = "tls.crt"
	dataTLSKey  = "tls.key"

	// leafRenewBefore re-mints a node leaf once it is within this window of
	// expiry (belt-and-braces alongside the per-startup IP-keyed re-mint).
	leafRenewBefore = 14 * 24 * time.Hour
)

// ServerSecretName returns the leaf Secret name for one storage owner. Legacy
// single-owner installs also use this suffixed form; the old shared leaf name
// is unsafe when multiple owners expose different endpoints.
func ServerSecretName(owner string) (string, error) {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return "", fmt.Errorf("tlsca: storage owner required for server leaf")
	}
	// Owner names are stable DNS subdomains shared with Kubernetes Node identity.
	// Do not lowercase:
	// silently changing an identity would select a different leaf Secret.
	if problems := validation.IsDNS1123Subdomain(owner); len(problems) != 0 {
		return "", fmt.Errorf("tlsca: invalid storage owner")
	}
	name := serverSecretPrefix + owner
	if len(name) > 253 || strings.HasSuffix(name, "-") {
		return "", fmt.Errorf("tlsca: invalid storage owner")
	}
	return name, nil
}

// EnsureCA loads the internal CA from its Secret, minting + persisting a new one
// if absent. Idempotent: safe to call on every controller startup / leader
// change. Returns the CA so the same process can sign leaves if desired.
func EnsureCA(ctx context.Context, c SecretClient, namespace string) (*CA, error) {
	key := apimachinerytypes.NamespacedName{Name: CASecretName, Namespace: namespace}
	sec := &corev1.Secret{}
	if err := c.Get(ctx, key, sec); err == nil {
		ca, lerr := LoadCA(sec.Data[dataTLSCert], sec.Data[dataTLSKey])
		if lerr == nil {
			return ca, nil
		}
		return nil, fmt.Errorf("load ca secret: %w", lerr)
	} else if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("get ca secret: %w", err)
	}

	ca, err := NewCA("zfs-csi NFS TLS CA")
	if err != nil {
		return nil, err
	}
	sec = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CASecretName,
			Namespace: namespace,
			Labels:    map[string]string{"app.kubernetes.io/managed-by": "zfs-csi", "zfs-csi/tls": "ca"},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			dataTLSCert: ca.CertPEM,
			dataTLSKey:  ca.KeyPEM,
			dataCACert:  ca.CertPEM, // bundle == the single self-signed cert
		},
	}
	if err := c.Create(ctx, sec); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// Lost a race; load the winner.
			sec = &corev1.Secret{}
			if gerr := c.Get(ctx, key, sec); gerr != nil {
				return nil, fmt.Errorf("load raced CA secret: %w", gerr)
			}
			loaded, lerr := LoadCA(sec.Data[dataTLSCert], sec.Data[dataTLSKey])
			if lerr != nil {
				return nil, fmt.Errorf("load raced CA secret: %w", lerr)
			}

			return loaded, nil
		}

		return nil, fmt.Errorf("create ca secret: %w", err)
	}

	return ca, nil
}

// EnsureCAInSigningNamespace loads the private CA from signingNamespace. On an
// upgrade from the original single-namespace layout it atomically copies a
// valid legacy CA before issuing any leaf, preserving every existing mTLS
// identity. Runtime node and storage identities never call this function.
func EnsureCAInSigningNamespace(ctx context.Context, c SecretClient, signingNamespace, legacyNamespace string) (*CA, error) {
	if err := ValidateSigningNamespace(legacyNamespace, signingNamespace); err != nil {
		return nil, err
	}

	target := apimachinerytypes.NamespacedName{Name: CASecretName, Namespace: signingNamespace}
	targetSecret := &corev1.Secret{}
	targetExists := false
	var targetCA *CA
	if err := c.Get(ctx, target, targetSecret); err == nil {
		var loadErr error
		targetCA, loadErr = LoadCA(targetSecret.Data[dataTLSCert], targetSecret.Data[dataTLSKey])
		if loadErr != nil {
			return nil, fmt.Errorf("load signing CA secret: %w", loadErr)
		}
		targetExists = true
	} else if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("get signing CA secret: %w", err)
	}

	legacy := &corev1.Secret{}
	legacyKey := apimachinerytypes.NamespacedName{Name: CASecretName, Namespace: legacyNamespace}
	if err := c.Get(ctx, legacyKey, legacy); err == nil {
		ca, loadErr := LoadCA(legacy.Data[dataTLSCert], legacy.Data[dataTLSKey])
		if loadErr != nil {
			return nil, fmt.Errorf("load legacy CA secret: %w", loadErr)
		}
		if targetExists {
			if !bytes.Equal(targetSecret.Data[dataTLSCert], legacy.Data[dataTLSCert]) || !bytes.Equal(targetSecret.Data[dataTLSKey], legacy.Data[dataTLSKey]) {
				return nil, fmt.Errorf("signing and legacy CA secrets differ; refusing to replace active trust root")
			}
			if err := c.Delete(ctx, legacy); err != nil && !apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("remove migrated legacy CA secret: %w", err)
			}
			return targetCA, nil
		}
		copy := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: CASecretName, Namespace: signingNamespace, Labels: map[string]string{"app.kubernetes.io/managed-by": "zfs-csi", "zfs-csi/tls": "ca"}},
			Type:       corev1.SecretTypeOpaque,
			Data:       map[string][]byte{dataTLSCert: ca.CertPEM, dataTLSKey: ca.KeyPEM, dataCACert: ca.CertPEM},
		}
		createErr := c.Create(ctx, copy)
		if createErr != nil && !apierrors.IsAlreadyExists(createErr) {
			return nil, fmt.Errorf("copy legacy CA secret: %w", createErr)
		}
		if createErr == nil {
			if err := c.Delete(ctx, legacy); err != nil && !apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("remove migrated legacy CA secret: %w", err)
			}
			return ca, nil
		}
		if err := c.Get(ctx, target, targetSecret); err != nil {
			return nil, fmt.Errorf("load raced signing CA secret: %w", err)
		}
		winner, loadErr := LoadCA(targetSecret.Data[dataTLSCert], targetSecret.Data[dataTLSKey])
		if loadErr != nil {
			return nil, fmt.Errorf("load raced signing CA secret: %w", loadErr)
		}
		if !bytes.Equal(targetSecret.Data[dataTLSCert], legacy.Data[dataTLSCert]) || !bytes.Equal(targetSecret.Data[dataTLSKey], legacy.Data[dataTLSKey]) {
			return nil, fmt.Errorf("raced signing and legacy CA secrets differ; refusing to replace active trust root")
		}
		if err := c.Delete(ctx, legacy); err != nil && !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("remove migrated legacy CA secret: %w", err)
		}
		return winner, nil
	} else if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("get legacy CA secret: %w", err)
	}

	if targetExists {
		return targetCA, nil
	}
	return EnsureCA(ctx, c, signingNamespace)
}

// ValidateSigningNamespace prevents the private signing key from being placed
// in the runtime driver's namespace, where dynamic NVMe PSK reads are broader.
func ValidateSigningNamespace(driverNamespace, signingNamespace string) error {
	if len(validation.IsDNS1123Subdomain(driverNamespace)) != 0 || len(validation.IsDNS1123Subdomain(signingNamespace)) != 0 {
		return fmt.Errorf("tlsca: signing and driver namespaces must be DNS1123 subdomains")
	}
	if driverNamespace == signingNamespace {
		return fmt.Errorf("tlsca: signing namespace must differ from driver namespace")
	}
	return nil
}

// EnsurePublicCA writes a certificate-only copy of the CA for consumers. It
// intentionally has no tls.key key and can therefore be projected into node
// pods without placing signing material in their runtime filesystem.
func EnsurePublicCA(ctx context.Context, c SecretClient, namespace string, ca *CA) error {
	key := apimachinerytypes.NamespacedName{Name: CAPublicSecretName, Namespace: namespace}
	sec := &corev1.Secret{}
	err := c.Get(ctx, key, sec)
	data := map[string][]byte{dataCACert: ca.CertPEM}
	if apierrors.IsNotFound(err) {
		sec = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: CAPublicSecretName, Namespace: namespace, Labels: map[string]string{"app.kubernetes.io/managed-by": "zfs-csi", "zfs-csi/tls": "ca-public"}},
			Type:       corev1.SecretTypeOpaque,
			Data:       data,
		}
		if err := c.Create(ctx, sec); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("create public CA secret: %w", err)
			}
			if err := c.Get(ctx, key, sec); err != nil {
				return fmt.Errorf("load raced public CA secret: %w", err)
			}
			if string(sec.Data[dataCACert]) != string(ca.CertPEM) || len(sec.Data) != 1 {
				return fmt.Errorf("raced public CA secret does not match current CA")
			}
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("get public CA secret: %w", err)
	}
	if string(sec.Data[dataCACert]) == string(ca.CertPEM) && len(sec.Data) == 1 {
		return nil
	}
	sec.Data = data
	if err := c.Update(ctx, sec); err != nil {
		return fmt.Errorf("update public CA secret: %w", err)
	}
	return nil
}

// EnsureNodeLeaf ensures this storage node's NFS server leaf Secret exists and
// certifies portalHost, minting + persisting a fresh leaf when the Secret is
// absent, the cert no longer matches the portal (node IP changed), or it is near
// expiry. Idempotent per startup. Requires the CA (from EnsureCA).
func EnsureNodeLeaf(ctx context.Context, c SecretClient, namespace, owner, portalHost string, ca *CA) error {
	if portalHost == "" {
		return fmt.Errorf("tlsca: portal host required for node leaf")
	}
	secretName, err := ServerSecretName(owner)
	if err != nil {
		return err
	}
	key := apimachinerytypes.NamespacedName{Name: secretName, Namespace: namespace}
	sec := &corev1.Secret{}
	err = c.Get(ctx, key, sec)
	switch {
	case err == nil:
		if leafSecretValidFor(sec, portalHost, ca) {
			return nil // current leaf already good for this portal
		}
		// Re-mint into the existing Secret.
		leaf, merr := ca.SignLeaf(portalHost)
		if merr != nil {
			return merr
		}
		sec.Data = leafSecretData(leaf, ca)
		if uerr := c.Update(ctx, sec); uerr != nil {
			return fmt.Errorf("update server leaf secret: %w", uerr)
		}

		return nil
	case apierrors.IsNotFound(err):
		leaf, merr := ca.SignLeaf(portalHost)
		if merr != nil {
			return merr
		}
		sec = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: namespace,
				Labels:    map[string]string{"app.kubernetes.io/managed-by": "zfs-csi", "zfs-csi/tls": "server"},
			},
			Type: corev1.SecretTypeTLS,
			Data: leafSecretData(leaf, ca),
		}
		if cerr := c.Create(ctx, sec); cerr != nil {
			if !apierrors.IsAlreadyExists(cerr) {
				return fmt.Errorf("create server leaf secret: %w", cerr)
			}
			// A concurrent storage agent won. Re-read and verify the winner instead
			// of treating an unknown certificate as ready.
			if gerr := c.Get(ctx, key, sec); gerr != nil {
				return fmt.Errorf("load raced server leaf secret: %w", gerr)
			}
			if !leafSecretValidFor(sec, portalHost, ca) {
				return fmt.Errorf("raced server leaf secret is invalid for endpoint %q", portalHost)
			}
		}

		return nil
	default:
		return fmt.Errorf("get server leaf secret: %w", err)
	}
}

func leafSecretData(leaf *KeyPair, ca *CA) map[string][]byte {
	return map[string][]byte{
		dataTLSCert: leaf.CertPEM,
		dataTLSKey:  leaf.KeyPEM,
		dataCACert:  ca.CertPEM,
	}
}

func leafSecretValidFor(sec *corev1.Secret, portalHost string, ca *CA) bool {
	if string(sec.Data[dataCACert]) != string(ca.CertPEM) {
		return false
	}
	leaf, err := parseCertPEM(sec.Data[dataTLSCert])
	if err != nil {
		return false
	}
	key, err := parseECKeyPEM(sec.Data[dataTLSKey])
	if err != nil {
		return false
	}
	if !publicKeysEqual(&key.PublicKey, leaf.PublicKey) {
		return false
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	_, err = leaf.Verify(x509.VerifyOptions{
		Roots:       pool,
		DNSName:     portalHost,
		KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		CurrentTime: time.Now(),
	})

	return err == nil && time.Now().Add(leafRenewBefore).Before(leaf.NotAfter)
}

// ServerLeafValid verifies a server-auth leaf using only public CA material.
// Storage agents use it at runtime so they never need to read the CA signing
// key. The signer authority owns all leaf issuance and renewal.
func ServerLeafValid(certPEM, keyPEM, caPEM []byte, endpoint string, renewBefore time.Duration) bool {
	caCert, err := parseCertPEM(caPEM)
	if err != nil || !caCert.IsCA {
		return false
	}
	leaf, err := parseCertPEM(certPEM)
	if err != nil {
		return false
	}
	key, err := parseECKeyPEM(keyPEM)
	if err != nil || !publicKeysEqual(&key.PublicKey, leaf.PublicKey) {
		return false
	}
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	_, err = leaf.Verify(x509.VerifyOptions{
		Roots:       pool,
		DNSName:     endpoint,
		KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		CurrentTime: time.Now(),
	})
	return err == nil && time.Now().Add(renewBefore).Before(leaf.NotAfter)
}

func publicKeysEqual(a, b any) bool {
	aKey, aOK := a.(*ecdsa.PublicKey)
	bKey, bOK := b.(*ecdsa.PublicKey)
	return aOK && bOK && aKey.Equal(bKey)
}
