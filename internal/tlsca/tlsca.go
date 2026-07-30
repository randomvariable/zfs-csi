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

// Package tlsca provides the minimal internal certificate authority used for
// mutually authenticated NFS-over-TLS (RFC 9289). Signer authority mints a
// self-signed CA and server-auth leaves; PodCertificateRequests supply
// node-bound client-auth public keys.
//
// This is deliberately a small, dependency-free x509 core (no cert-manager, no
// SPIFFE) so the driver installs on any cluster. The CASource seam lets a
// cert-manager-backed issuer replace it later without touching the data path
// (oracle Decision 2). PEM in, PEM out — no k8s or kernel here, so the whole
// package is unit-testable.
package tlsca

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	// caValidity is the self-signed CA lifetime. Long, because rotating the CA
	// re-trusts every node; leaves are short and rotated at agent startup.
	caValidity = 10 * 365 * 24 * time.Hour
	// leafValidity is a node leaf lifetime. Short-ish, but the agent re-mints on
	// every startup keyed to the node's current portal address, so ephemeral
	// CAPA nodes with changing IPs always present a currently-valid cert.
	leafValidity = 90 * 24 * time.Hour

	pemTypeCert = "CERTIFICATE"
	pemTypeKey  = "EC PRIVATE KEY"
)

// CA is a parsed certificate authority: the signing cert + its private key, plus
// the PEM encodings for persistence into a Secret.
type CA struct {
	Cert    *x509.Certificate
	Key     *ecdsa.PrivateKey
	CertPEM []byte
	KeyPEM  []byte
}

// KeyPair is a leaf certificate + key in PEM form (what tlshd loads).
type KeyPair struct {
	CertPEM []byte
	KeyPEM  []byte
}

// NewCA generates a fresh self-signed CA. commonName labels it (e.g.
// "zfs-csi NFS TLS CA").
func NewCA(commonName string) (*CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("tlsca: generate ca key: %w", err)
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              now.Add(caValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true, // CA signs leaves only, no intermediates
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("tlsca: self-sign ca: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("tlsca: parse ca: %w", err)
	}
	keyPEM, err := encodeECKey(key)
	if err != nil {
		return nil, err
	}

	return &CA{
		Cert:    cert,
		Key:     key,
		CertPEM: pem.EncodeToMemory(&pem.Block{Type: pemTypeCert, Bytes: der}),
		KeyPEM:  keyPEM,
	}, nil
}

// LoadCA reconstructs a CA from its persisted cert + key PEM (Secret round-trip).
func LoadCA(certPEM, keyPEM []byte) (*CA, error) {
	cert, err := parseCertPEM(certPEM)
	if err != nil {
		return nil, err
	}
	if !cert.IsCA {
		return nil, fmt.Errorf("tlsca: loaded cert is not a CA")
	}
	key, err := parseECKeyPEM(keyPEM)
	if err != nil {
		return nil, err
	}

	return &CA{Cert: cert, Key: key, CertPEM: certPEM, KeyPEM: keyPEM}, nil
}

// SignLeaf mints a server leaf certificate for the given portal host. If host
// parses as an IP it is placed in an IP SAN (tlshd validates the dialed IP
// against IP SANs, not DNS SANs); otherwise it is a DNS SAN. commonName is
// cosmetic (SAN is authoritative for modern validation).
func (ca *CA) SignLeaf(host string) (*KeyPair, error) {
	return ca.signLeaf(host, x509.ExtKeyUsageServerAuth)
}

// SignClientCertificate signs an externally generated public key as a
// client-authentication-only leaf for one attested Kubernetes node identity.
// Callers must derive identity and key from a trusted API, not user CSR fields.
func (ca *CA) SignClientCertificate(node string, publicKey crypto.PublicKey, notBefore time.Time, lifetime time.Duration) ([]byte, *x509.Certificate, error) {
	if err := validateDNSIdentity(node); err != nil {
		return nil, nil, err
	}
	if publicKey == nil {
		return nil, nil, fmt.Errorf("tlsca: client public key required")
	}
	if lifetime < time.Hour || lifetime > 91*24*time.Hour {
		return nil, nil, fmt.Errorf("tlsca: client certificate lifetime must be between 1 hour and 91 days")
	}
	if notBefore.IsZero() {
		return nil, nil, fmt.Errorf("tlsca: client certificate notBefore required")
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, nil, err
	}
	if notBefore.Before(ca.Cert.NotBefore) {
		notBefore = ca.Cert.NotBefore
	}
	notAfter := notBefore.Add(lifetime)
	if notAfter.After(ca.Cert.NotAfter) {
		notAfter = ca.Cert.NotAfter
	}
	if notAfter.Sub(notBefore) < time.Hour {
		return nil, nil, fmt.Errorf("tlsca: CA expires before minimum client certificate lifetime")
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: node},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{node},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, publicKey, ca.Key)
	if err != nil {
		return nil, nil, fmt.Errorf("tlsca: sign client certificate: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, fmt.Errorf("tlsca: parse signed client certificate: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: pemTypeCert, Bytes: der}), cert, nil
}

func (ca *CA) signLeaf(host string, usage x509.ExtKeyUsage) (*KeyPair, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("tlsca: generate leaf key: %w", err)
	}
	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: host},
		NotBefore:    now.Add(-5 * time.Minute),
		NotAfter:     now.Add(leafValidity),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
	}
	if ip := net.ParseIP(host); ip != nil {
		tmpl.IPAddresses = []net.IP{ip}
	} else {
		tmpl.DNSNames = []string{host}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, &key.PublicKey, ca.Key)
	if err != nil {
		return nil, fmt.Errorf("tlsca: sign leaf: %w", err)
	}
	keyPEM, err := encodeECKey(key)
	if err != nil {
		return nil, err
	}

	return &KeyPair{
		CertPEM: pem.EncodeToMemory(&pem.Block{Type: pemTypeCert, Bytes: der}),
		KeyPEM:  keyPEM,
	}, nil
}

func validateDNSIdentity(identity string) error {
	if identity == "" {
		return fmt.Errorf("tlsca: client identity must be a DNS name")
	}
	if problems := validation.IsDNS1123Subdomain(identity); len(problems) != 0 {
		return fmt.Errorf("tlsca: invalid client identity %q", identity)
	}
	return nil
}

// LeafValidFor reports whether a persisted leaf cert is still valid for host
// (correct SAN + not near expiry). Used by the agent to decide whether to re-mint
// on startup — a portal IP change or approaching expiry triggers a fresh leaf.
func LeafValidFor(certPEM []byte, host string, renewBefore time.Duration) bool {
	cert, err := parseCertPEM(certPEM)
	if err != nil {
		return false
	}
	if time.Now().Add(renewBefore).After(cert.NotAfter) {
		return false
	}
	if err := cert.VerifyHostname(host); err != nil {
		return false
	}

	return true
}

func randomSerial() (*big.Int, error) {
	// 128-bit random serial (RFC 5280 recommends >= 64-bit unpredictable).
	max := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, max)
	if err != nil {
		return nil, fmt.Errorf("tlsca: serial: %w", err)
	}

	return serial, nil
}

func encodeECKey(key *ecdsa.PrivateKey) ([]byte, error) {
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("tlsca: marshal key: %w", err)
	}

	return pem.EncodeToMemory(&pem.Block{Type: pemTypeKey, Bytes: der}), nil
}

func parseCertPEM(certPEM []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil || block.Type != pemTypeCert {
		return nil, fmt.Errorf("tlsca: no CERTIFICATE PEM block")
	}

	return x509.ParseCertificate(block.Bytes)
}

func parseECKeyPEM(keyPEM []byte) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode(keyPEM)
	if block == nil || block.Type != pemTypeKey {
		return nil, fmt.Errorf("tlsca: no EC PRIVATE KEY PEM block")
	}

	return x509.ParseECPrivateKey(block.Bytes)
}
