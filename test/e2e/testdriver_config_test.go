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
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
)

// testDriverDefinition is the subset of the external-storage testdriver
// definition needed to guard access-mode semantics. The upstream loader runs in
// the version-matched conformance image; this keeps the regression test local.
type testDriverDefinition struct {
	VolumeAttributesClass struct {
		FromExistingClassName string `yaml:"FromExistingClassName"`
	} `yaml:"VolumeAttributesClass"`
	DriverInfo struct {
		PreprovisionedPV    *bool           `yaml:"PreprovisionedPV"`
		Capabilities        map[string]bool `yaml:"Capabilities"`
		RequiredAccessModes []string        `yaml:"RequiredAccessModes"`
		StressTestOptions   struct {
			NumPods     int `yaml:"NumPods"`
			NumRestarts int `yaml:"NumRestarts"`
		} `yaml:"StressTestOptions"`
	} `yaml:"DriverInfo"`
}

func TestTestDriverAccessModeDefaults(t *testing.T) {
	tests := []struct {
		name                    string
		path                    string
		wantRequiredAccessModes []string
		wantReadWriteOncePod    bool
		wantVAC                 string
	}{
		{
			name:                    "nvme",
			path:                    "data/testdriver/zfs-csi-nvme.yaml",
			wantRequiredAccessModes: []string{"ReadWriteOnce"},
			wantReadWriteOncePod:    true,
			wantVAC:                 volumeAttributesClassName,
		},
		{
			name:                    "nvme encrypted",
			path:                    "data/testdriver/zfs-csi-nvme-encrypted.yaml",
			wantRequiredAccessModes: []string{"ReadWriteOnce"},
			wantReadWriteOncePod:    true,
			wantVAC:                 volumeAttributesClassName,
		},
		{
			name:                    "nfs",
			path:                    "data/testdriver/zfs-csi-nfs.yaml",
			wantRequiredAccessModes: []string{"ReadWriteMany", "ReadWriteOnce"},
			wantReadWriteOncePod:    false,
			wantVAC:                 volumeAttributesClassName,
		},
		{
			name:                    "nvme tls",
			path:                    "data/testdriver/zfs-csi-nvme-tls.yaml",
			wantRequiredAccessModes: []string{"ReadWriteOnce"},
			wantReadWriteOncePod:    true,
			wantVAC:                 volumeAttributesClassName,
		},
		{
			name:                    "nfs tls",
			path:                    "data/testdriver/zfs-csi-nfs-tls.yaml",
			wantRequiredAccessModes: []string{"ReadWriteMany", "ReadWriteOnce"},
			wantReadWriteOncePod:    false,
			wantVAC:                 volumeAttributesClassName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			definition := loadTestDriverDefinition(t, tt.path)
			if definition.DriverInfo.Capabilities["volumeLimits"] {
				t.Error("Capabilities.volumeLimits = true, want false for selective node deployments")
			}
			if got := definition.DriverInfo.RequiredAccessModes; !reflect.DeepEqual(got, tt.wantRequiredAccessModes) {
				t.Errorf("RequiredAccessModes = %v, want %v", got, tt.wantRequiredAccessModes)
			}
			if got := definition.DriverInfo.Capabilities["readWriteOncePod"]; got != tt.wantReadWriteOncePod {
				t.Errorf("Capabilities.readWriteOncePod = %t, want %t", got, tt.wantReadWriteOncePod)
			}
			if !definition.DriverInfo.Capabilities["capacity"] {
				t.Error("Capabilities.capacity = false, want true")
			}
			if tt.name == "nvme tls" && (!definition.DriverInfo.Capabilities["pvcDataSource"] || !definition.DriverInfo.Capabilities["snapshotDataSource"]) {
				t.Error("TLS NVMe testdriver must advertise clone and snapshot support")
			}
			if definition.DriverInfo.PreprovisionedPV == nil || *definition.DriverInfo.PreprovisionedPV {
				t.Errorf("DriverInfo.PreprovisionedPV = %v, want explicit false", definition.DriverInfo.PreprovisionedPV)
			}
			if got := definition.VolumeAttributesClass.FromExistingClassName; got != tt.wantVAC {
				t.Errorf("VolumeAttributesClass.FromExistingClassName = %q, want %q", got, tt.wantVAC)
			}
			if got := definition.DriverInfo.StressTestOptions.NumPods; got != 4 {
				t.Errorf("StressTestOptions.NumPods = %d, want 4", got)
			}
			if got := definition.DriverInfo.StressTestOptions.NumRestarts; got != 3 {
				t.Errorf("StressTestOptions.NumRestarts = %d, want 3", got)
			}
			if hasReadWriteOncePod(definition.DriverInfo.RequiredAccessModes) && len(definition.DriverInfo.RequiredAccessModes) != 1 {
				t.Errorf("ReadWriteOncePod must be the only RequiredAccessModes entry, got %v", definition.DriverInfo.RequiredAccessModes)
			}
		})
	}
}

func loadTestDriverDefinition(t *testing.T, path string) testDriverDefinition {
	t.Helper()
	root := repositoryRootForTestDriver(t, path)
	body, err := os.ReadFile(filepath.Join(root, "test", "e2e", path))
	if err != nil {
		t.Fatalf("read testdriver definition: %v", err)
	}

	var definition testDriverDefinition
	if err := yaml.Unmarshal(body, &definition); err != nil {
		t.Fatalf("parse testdriver definition: %v", err)
	}
	return definition
}

// repositoryRootForTestDriver works for both `go test` package execution and
// harness-launched binaries built with -trimpath, where runtime.Caller cannot
// locate the source tree. A candidate root must contain both go.mod and the
// repository-owned fixture, preventing an unrelated parent module from winning.
func repositoryRootForTestDriver(t *testing.T, fixture string) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	for {
		fixturePath := filepath.Join(dir, "test", "e2e", fixture)
		if _, modErr := os.Stat(filepath.Join(dir, "go.mod")); modErr == nil {
			if _, fixtureErr := os.Stat(fixturePath); fixtureErr == nil {
				return dir
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatalf("find repository root containing test/e2e/%s", fixture)
	return ""
}

func hasReadWriteOncePod(modes []string) bool {
	for _, mode := range modes {
		if mode == "ReadWriteOncePod" {
			return true
		}
	}
	return false
}
