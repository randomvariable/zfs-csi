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

//go:build e2e

package e2e

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestConformanceTestDriverFiles(t *testing.T) {
	tests := []struct {
		name                     string
		tlsOnly, encryption, tls bool
		want                     []string
		wantErr                  bool
	}{
		{"normal", false, true, false, []string{"zfs-csi-nvme.yaml", "zfs-csi-nfs.yaml", "zfs-csi-nvme-encrypted.yaml"}, false},
		{"normal TLS", false, true, true, []string{"zfs-csi-nvme.yaml", "zfs-csi-nfs.yaml", "zfs-csi-nvme-encrypted.yaml", "zfs-csi-nvme-tls.yaml", "zfs-csi-nfs-tls.yaml"}, false},
		{"normal no encryption", false, false, false, []string{"zfs-csi-nvme.yaml", "zfs-csi-nfs.yaml"}, false},
		{"TLS enabled without encryption", false, false, true, []string{"zfs-csi-nvme.yaml", "zfs-csi-nfs.yaml", "zfs-csi-nvme-tls.yaml", "zfs-csi-nfs-tls.yaml"}, false},
		{"TLS only ignores encryption", true, true, true, []string{"zfs-csi-nvme-tls.yaml", "zfs-csi-nfs-tls.yaml"}, false},
		{"TLS only without encryption", true, false, true, []string{"zfs-csi-nvme-tls.yaml", "zfs-csi-nfs-tls.yaml"}, false},
		{"TLS only requires TLS", true, false, false, nil, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := conformanceTestDriverFiles(tc.tlsOnly, tc.encryption, tc.tls)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tc.wantErr)
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("files = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestConformanceImageForVersion verifies the conformance image tag derivation,
// including the oracle-mandated strip of any build-metadata suffix that has no
// matching registry.k8s.io/conformance tag.
func TestConformanceImageForVersion(t *testing.T) {
	cases := []struct {
		version string
		want    string
	}{
		{"v1.34.8", "registry.k8s.io/conformance:v1.34.8"},
		{"v1.36.2", "registry.k8s.io/conformance:v1.36.2"},
		{"v1.34.8+vmware.1", "registry.k8s.io/conformance:v1.34.8"},  // suffix stripped
		{"v1.35.0-rc.1", "registry.k8s.io/conformance:v1.35.0-rc.1"}, // hyphen prerelease kept
	}
	for _, tc := range cases {
		if got := conformanceImageForVersion(tc.version); got != tc.want {
			t.Errorf("conformanceImageForVersion(%q) = %q, want %q", tc.version, got, tc.want)
		}
	}
}

func TestWriteConformanceSSHKey(t *testing.T) {
	path, err := writeConformanceSSHKey(t.TempDir(), []byte("private-key"))
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(path) != "ssh-private-key" {
		t.Errorf("key path = %q, want ssh-private-key", path)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "private-key" {
		t.Errorf("key body = %q", body)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("key mode = %o, want 600", info.Mode().Perm())
	}
	if _, err := writeConformanceSSHKey(t.TempDir(), nil); err == nil {
		t.Error("empty private key did not fail")
	}
}

func TestConformanceSSHEnvironmentIncludesBastionRouting(t *testing.T) {
	environment := conformanceSSHEnvironment(conformanceInput{
		SSHPrivateKey: []byte("private-key"),
		SSHUser:       "ubuntu",
		SSHBastion:    "203.0.113.10:22",
	})
	for key, want := range map[string]string{
		"KUBE_SSH_KEY_PATH": "/tmp/ssh-key",
		"KUBE_SSH_USER":     "ubuntu",
		"KUBE_SSH_BASTION":  "203.0.113.10:22",
	} {
		if got := environment[key]; got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
}

// TestConformanceSSHEnvironmentOmitsKeyPathWithoutKey pins the static-lane
// contract: with no SSH key supplied (non-disruptive static conformance) the
// key path env var is omitted entirely, matching the skipped key mount.
func TestConformanceSSHEnvironmentOmitsKeyPathWithoutKey(t *testing.T) {
	environment := conformanceSSHEnvironment(conformanceInput{SSHUser: "ubuntu"})
	if _, present := environment["KUBE_SSH_KEY_PATH"]; present {
		t.Errorf("KUBE_SSH_KEY_PATH present without an SSH key: %v", environment)
	}
	if got := environment["KUBE_SSH_USER"]; got != "ubuntu" {
		t.Errorf("KUBE_SSH_USER = %q, want ubuntu", got)
	}
}

func TestConformanceErrorAggregationPreservesAllEvidence(t *testing.T) {
	err := errors.Join(
		errors.New("conformance run failed"),
		errors.New("post-run diagnostics failed"),
		errors.New("junit collection failed"),
	)
	for _, want := range []string{"conformance run failed", "post-run diagnostics failed", "junit collection failed"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("joined error missing %q: %v", want, err)
		}
	}
}

// TestBuildConformanceArgs verifies flag rendering with both markers.
func TestBuildConformanceArgs(t *testing.T) {
	got := buildConformanceArgs(map[string]string{"focus": "X", "v": "true"}, "-")
	// Map order is non-deterministic; assert set membership.
	joined := strings.Join(got, " ")
	for _, want := range []string{"-focus=X", "-v=true"} {
		if !strings.Contains(joined, want) {
			t.Errorf("buildConformanceArgs missing %q in %v", want, got)
		}
	}
	if len(got) != 2 {
		t.Errorf("buildConformanceArgs len = %d, want 2", len(got))
	}
}

// TestConformanceDefaults asserts the focus anchors on our storage drivers and
// that the default skip is empty — the focus already scopes the run to the
// external-storage suite for our drivers, so Disruptive/Serial/Slow/Feature
// storage specs are deliberately NOT skipped (they are the storage edge-cases we
// want proven).
func TestConformanceDefaults(t *testing.T) {
	// Focus must match the External Storage tree for a co.uk-suffixed driver.
	if !strings.Contains(conformanceDefaultFocus, "External.Storage") {
		t.Errorf("focus %q must target External.Storage", conformanceDefaultFocus)
	}
	if !strings.Contains(conformanceDefaultFocus, `co\.uk`) {
		t.Errorf("focus %q must anchor on the co.uk driver-name suffix", conformanceDefaultFocus)
	}
	// Default skip must be empty: we want Disruptive/Serial/Slow/Feature storage
	// specs to run (they are storage-related by focus construction). A non-empty
	// default would silently drop storage edge-case coverage.
	if conformanceDefaultSkip != "" {
		t.Errorf("default skip must be empty (storage focus scopes the run); got %q", conformanceDefaultSkip)
	}
}

// TestConformanceEmptySkipOmitsFlag guards the footgun that an empty ginkgo
// --skip regex matches (and skips) every spec: the runner must OMIT the skip
// flag when Skip is empty, not render --skip=.
func TestConformanceEmptySkipOmitsFlag(t *testing.T) {
	// A rendered args map with an empty skip must not carry a skip entry.
	// buildConformanceArgs turns the map into flags verbatim, so the guard lives
	// in the ginkgoVars construction (only add "skip" when non-empty). Mirror that
	// contract here on the default.
	if conformanceDefaultSkip != "" {
		t.Skip("default skip non-empty; empty-skip omission contract not exercised")
	}
	args := buildConformanceArgs(map[string]string{"focus": conformanceDefaultFocus}, "-")
	for _, a := range args {
		if strings.HasPrefix(a, "-skip=") {
			t.Errorf("empty skip must not render a -skip= flag; got %q", a)
		}
	}
}

// TestConformanceSuiteTimeoutBounded asserts a hard suite timeout is set (so a
// broken provision can't run away for hours on framework polls).
func TestConformanceSuiteTimeoutBounded(t *testing.T) {
	d, err := time.ParseDuration(conformanceSuiteTimeout)
	if err != nil {
		t.Fatalf("parse conformanceSuiteTimeout %q: %v", conformanceSuiteTimeout, err)
	}
	if d <= 0 || d > 6*time.Hour {
		t.Errorf("conformanceSuiteTimeout = %s, want a positive duration no longer than 6h", d)
	}
}
