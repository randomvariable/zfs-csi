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
	"crypto/x509"
	"net"
	"testing"
	"time"
)

func TestNewCARoundTrip(t *testing.T) {
	ca, err := NewCA("zfs-csi NFS TLS CA")
	if err != nil {
		t.Fatalf("NewCA: %v", err)
	}
	if !ca.Cert.IsCA {
		t.Fatal("cert is not a CA")
	}
	// Reload from PEM and confirm it still signs.
	reloaded, err := LoadCA(ca.CertPEM, ca.KeyPEM)
	if err != nil {
		t.Fatalf("LoadCA: %v", err)
	}
	if reloaded.Cert.Subject.CommonName != "zfs-csi NFS TLS CA" {
		t.Fatalf("CN = %q", reloaded.Cert.Subject.CommonName)
	}
}

func TestSignLeafIPSANValidatesAgainstDialedIP(t *testing.T) {
	ca, err := NewCA("ca")
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := ca.SignLeaf("10.0.0.5")
	if err != nil {
		t.Fatalf("SignLeaf: %v", err)
	}

	cert, err := parseCertPEM(leaf.CertPEM)
	if err != nil {
		t.Fatal(err)
	}
	// The dialed IP must be an IP SAN, not a DNS SAN.
	if len(cert.IPAddresses) != 1 || !cert.IPAddresses[0].Equal(net.ParseIP("10.0.0.5")) {
		t.Fatalf("IP SANs = %v, want [10.0.0.5]", cert.IPAddresses)
	}
	if len(cert.DNSNames) != 0 {
		t.Fatalf("DNS SANs = %v, want none for an IP portal", cert.DNSNames)
	}

	// Full chain verification as tlshd would do: leaf signed by CA, dialed by IP.
	roots := x509.NewCertPool()
	roots.AddCert(ca.Cert)
	if _, err := cert.Verify(x509.VerifyOptions{
		Roots:     roots,
		DNSName:   "10.0.0.5", // Verify treats a numeric DNSName as an IP match
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		t.Fatalf("verify IP leaf: %v", err)
	}

	// A DIFFERENT IP must fail (proves SAN binding).
	if err := cert.VerifyHostname("10.0.0.6"); err == nil {
		t.Fatal("cert must not validate for a different IP")
	}
}

func TestSignLeafDNSSAN(t *testing.T) {
	ca, err := NewCA("ca")
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := ca.SignLeaf("storage.internal")
	if err != nil {
		t.Fatal(err)
	}
	cert, err := parseCertPEM(leaf.CertPEM)
	if err != nil {
		t.Fatal(err)
	}
	if len(cert.DNSNames) != 1 || cert.DNSNames[0] != "storage.internal" {
		t.Fatalf("DNS SANs = %v", cert.DNSNames)
	}
	if len(cert.IPAddresses) != 0 {
		t.Fatalf("IP SANs = %v, want none for a DNS portal", cert.IPAddresses)
	}
}

func TestLeafValidFor(t *testing.T) {
	ca, err := NewCA("ca")
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := ca.SignLeaf("10.0.0.5")
	if err != nil {
		t.Fatal(err)
	}

	// Valid now for the right host.
	if !LeafValidFor(leaf.CertPEM, "10.0.0.5", time.Hour) {
		t.Fatal("freshly minted leaf should be valid for its host")
	}
	// Invalid for a different host (IP changed -> must re-mint).
	if LeafValidFor(leaf.CertPEM, "10.0.0.6", time.Hour) {
		t.Fatal("leaf must be invalid for a different host")
	}
	// Invalid when renewBefore exceeds the whole validity (approaching expiry).
	if LeafValidFor(leaf.CertPEM, "10.0.0.5", 100*365*24*time.Hour) {
		t.Fatal("leaf should be considered stale when renewBefore exceeds lifetime")
	}
	// Garbage PEM is invalid, not a panic.
	if LeafValidFor([]byte("not a cert"), "10.0.0.5", time.Hour) {
		t.Fatal("garbage PEM must be invalid")
	}
}
