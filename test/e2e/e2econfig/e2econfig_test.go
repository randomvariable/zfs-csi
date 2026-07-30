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

package e2econfig

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const testOpenSSHPrivateKey = `-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACDqAIs1TYnYr5sXL0b5W9OwJar39C4xeKy7rtTGYvPmCQAAAJg748s4O+PL
OAAAAAtzc2gtZWQyNTUxOQAAACDqAIs1TYnYr5sXL0b5W9OwJar39C4xeKy7rtTGYvPmCQ
AAAEABZ0OrGFIZluBWUyX+aCdRT3Ge4DnjgiwCaZ4nypHNn+oAizVNidivmxcvRvlb07Al
qvf0LjF4rLuu1MZi8+YJAAAAE2UyZS1jb25maWctdGVzdC1rZXkBAg==
-----END OPENSSH PRIVATE KEY-----
`

const testEncryptedOpenSSHPrivateKey = `-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAACmFlczI1Ni1jdHIAAAAGYmNyeXB0AAAAGAAAABBIPV4eeV
Lrq+mg7NHl/3uJAAAAGAAAAAEAAAAzAAAAC3NzaC1lZDI1NTE5AAAAIDduvpfuXJAZM6lE
y2p3GuUhYNChQina9YawEe3gw+rIAAAAoJ26Hef+KqLH/6TAGF1/1meHobjEEMcYlDMwep
f1jQwzPC55qK56wj1/Aq305QlM11kD39Nw+zzbOJ6YPjNiB+GtBtmqojjZJ7mKM5RFfQzZ
5zK4TDX6pumZWb3KW6De87T4V/rMVWK4lz0RF4izvAXlJw1l3dALwdzB3jWnlf5NlFk18l
vHh8PtYhgPhr2VddNnO90MunA5Yg/X8k//Kbg=
-----END OPENSSH PRIVATE KEY-----
`

// TestProviderSeamDefaultsPinKubeVirtLane is the non-mutating parity guard for
// the provider seam (memory: CAPA/KubeVirt lane switch). Before the seam, the
// lifecycle test hardcoded InfrastructureProvider:"kubevirt", Flavor:"zfs-csi",
// and the storage helper hardcoded the data disk as
// /dev/disk/by-id/virtio-tank0. The seam moved all three behind config knobs;
// this test pins that the DEFAULTS reproduce those exact literals, so the
// existing KubeVirt lane is byte-for-byte unchanged when none of
// E2E_INFRASTRUCTURE_PROVIDER / E2E_FLAVOR / E2E_DATA_DISK_BY_ID are set.
//
// config.Init() binds the global pflag.CommandLine + viper.AutomaticEnv, so an
// ambient E2E_INFRASTRUCTURE_PROVIDER (e.g. from a concurrent AWS CI job in
// the same shell) would leak in and flip these defaults. t.Setenv forces the
// three keys to empty so the test is hermetic regardless of the caller's env.
//
// If any default drifts, this fails before a run silently picks the wrong lane.
func TestProviderSeamDefaultsPinKubeVirtLane(t *testing.T) {
	// config.Init() binds global pflag.CommandLine + AutomaticEnv, so scrub the
	// three seam env vars to guarantee the defaults are what's asserted here,
	// not whatever the caller's shell happened to export.
	t.Setenv("E2E_INFRASTRUCTURE_PROVIDER", "")
	t.Setenv("E2E_FLAVOR", "")
	t.Setenv("E2E_DATA_DISK_BY_ID", "")
	t.Setenv("E2E_KUBERNETES_VERSION", "")
	t.Setenv("E2E_NFS_EXPORT_CIDRS", "")
	t.Setenv("E2E_TRANSPORT_TLS", "")
	t.Setenv("E2E_CONFORMANCE_TLS_ONLY", "")
	if err := Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}

	t.Run("infrastructure provider is kubevirt", func(t *testing.T) {
		if got := InfrastructureProvider(); got != "kubevirt" {
			t.Fatalf("InfrastructureProvider default = %q, want %q (hardcoded pre-seam)", got, "kubevirt")
		}
	})
	t.Run("flavor is zfs-csi", func(t *testing.T) {
		if got := Flavor(); got != "zfs-csi" {
			t.Fatalf("Flavor default = %q, want %q (hardcoded pre-seam)", got, "zfs-csi")
		}
	})
	t.Run("data disk by-id is the virtio-tank0 path", func(t *testing.T) {
		const want = "/dev/disk/by-id/virtio-tank0"
		if got := DataDiskByID(); got != want {
			t.Fatalf("DataDiskByID default = %q, want %q (hardcoded pre-seam)", got, want)
		}
	})
	t.Run("kubernetes version is the pre-seam pinned v1.36.2", func(t *testing.T) {
		// Pre-seam this was a hardcoded const KubernetesVersion = "v1.36.2".
		// The KubeVirt-lane default must still resolve to exactly that.
		const want = "v1.36.2"
		if got := KubernetesVersion(); got != want {
			t.Fatalf("KubernetesVersion default = %q, want %q (hardcoded pre-seam)", got, want)
		}
	})
	t.Run("KubeVirt supplies its fixture NFS CIDR", func(t *testing.T) {
		if got := NFSExportCIDRs(); !slices.Equal(got, []string{"192.0.2.0/24"}) {
			t.Fatalf("KubeVirt NFSExportCIDRs = %q", got)
		}
	})
	t.Run("transport TLS is disabled by default", func(t *testing.T) {
		if TransportTLSEnabled() {
			t.Fatal("TransportTLSEnabled default = true, want false")
		}
	})
	t.Run("conformance TLS-only is disabled by default", func(t *testing.T) {
		if ConformanceTLSOnly() {
			t.Fatal("ConformanceTLSOnly default = true, want false")
		}
	})
}

func TestTransportTLSEnabledOverride(t *testing.T) {
	t.Setenv("E2E_TRANSPORT_TLS", "1")
	if err := Init(); err != nil {
		t.Fatal(err)
	}
	if !TransportTLSEnabled() {
		t.Fatal("TransportTLSEnabled override = false, want true")
	}
}

func TestConformanceTLSOnlyOverrideAndChildEnv(t *testing.T) {
	t.Setenv("E2E_CONFORMANCE_TLS_ONLY", "1")
	t.Setenv("E2E_TRANSPORT_TLS", "1")
	if err := Init(); err != nil {
		t.Fatal(err)
	}
	if !ConformanceTLSOnly() {
		t.Fatal("ConformanceTLSOnly override = false, want true")
	}
	if !slices.Contains(ChildEnv(), "E2E_CONFORMANCE_TLS_ONLY=1") {
		t.Fatalf("ChildEnv missing TLS-only conformance flag: %v", ChildEnv())
	}
}

func TestChildEnvOmitsDisabledBoolOverrides(t *testing.T) {
	for _, value := range []string{"", "0"} {
		t.Run("value="+value, func(t *testing.T) {
			for _, key := range []string{"E2E_CONFORMANCE_TLS_ONLY", "E2E_POD_CERTIFICATE_ACCEPTANCE"} {
				t.Setenv(key, value)
			}
			if err := Init(); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"E2E_CONFORMANCE_TLS_ONLY", "E2E_POD_CERTIFICATE_ACCEPTANCE"} {
				for _, envValue := range ChildEnv() {
					if strings.HasPrefix(envValue, key+"=") {
						t.Fatalf("ChildEnv emitted disabled bool %q: %v", key, ChildEnv())
					}
				}
			}
		})
	}
}

func TestConformanceTLSOnlyRequiresTransportTLS(t *testing.T) {
	t.Setenv("E2E_RUN_ID", "config-test")
	t.Setenv("E2E_CONFIG", filepath.Join("..", "e2e-config.yaml"))
	t.Setenv("E2E_INFRASTRUCTURE_PROVIDER", "kubevirt")
	t.Setenv("E2E_STORAGE_OWNERS", "")
	t.Setenv("E2E_CONSUMER_DOMAINS", "")
	t.Setenv("E2E_INFRASTRUCTURE_CONFIG", "")
	t.Setenv("E2E_CONFORMANCE_TLS_ONLY", "1")
	t.Setenv("E2E_TRANSPORT_TLS", "0")
	if err := Init(); err != nil {
		t.Fatal(err)
	}
	if err := Validate(); err == nil || !strings.Contains(err.Error(), "E2E_CONFORMANCE_TLS_ONLY=1 requires E2E_TRANSPORT_TLS=1") {
		t.Fatalf("Validate error = %v, want TLS-only transport requirement", err)
	}
}

func TestConformanceTLSOnlyCleanupOnlyBypassesTransportValidation(t *testing.T) {
	t.Setenv("E2E_RUN_ID", "config-test")
	t.Setenv("E2E_CONFIG", filepath.Join("..", "e2e-config.yaml"))
	t.Setenv("E2E_INFRASTRUCTURE_PROVIDER", "kubevirt")
	t.Setenv("E2E_STORAGE_OWNERS", "")
	t.Setenv("E2E_CONSUMER_DOMAINS", "")
	t.Setenv("E2E_INFRASTRUCTURE_CONFIG", "")
	t.Setenv("E2E_CLEANUP_ONLY", "1")
	t.Setenv("E2E_CONFORMANCE_TLS_ONLY", "1")
	t.Setenv("E2E_TRANSPORT_TLS", "0")
	if err := Init(); err != nil {
		t.Fatal(err)
	}
	if err := Validate(); err != nil {
		t.Fatalf("Validate cleanup-only: %v", err)
	}
}

func TestPodCertificateAcceptanceOverride(t *testing.T) {
	t.Setenv("E2E_POD_CERTIFICATE_ACCEPTANCE", "1")
	if err := Init(); err != nil {
		t.Fatal(err)
	}
	if !PodCertificateAcceptanceEnabled() {
		t.Fatal("E2E_POD_CERTIFICATE_ACCEPTANCE=1 did not enable acceptance")
	}
	if !slices.Contains(ChildEnv(), "E2E_POD_CERTIFICATE_ACCEPTANCE=1") {
		t.Fatalf("ChildEnv missing PodCertificate acceptance flag: %v", ChildEnv())
	}
}

// TestAWSLanePinsKubernetesVersion guards the AWS lane's version default: the
// custom image-builder AMI (mage e2e:imageBuildAWS) bakes v1.36.2, so the aws
// provider defaults to that (matching the KubeVirt lane). Previously the AWS
// lane was capped at v1.34.8 (newest CAPA-published AMI); the custom AMI lifts
// that cap. An explicit override still wins.
func TestAWSLanePinsKubernetesVersion(t *testing.T) {
	t.Setenv("E2E_FLAVOR", "")
	t.Setenv("E2E_DATA_DISK_BY_ID", "")
	t.Setenv("E2E_KUBERNETES_VERSION", "")
	t.Setenv("E2E_NFS_EXPORT_CIDRS", "")
	t.Setenv("E2E_INFRASTRUCTURE_PROVIDER", "aws")
	t.Setenv("E2E_SSH_USER", "")
	if err := Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Run("aws default is v1.36.2", func(t *testing.T) {
		if got := KubernetesVersion(); got != "v1.36.2" {
			t.Fatalf("aws KubernetesVersion default = %q, want %q (custom AMI bakes it)", got, "v1.36.2")
		}
	})
	t.Run("AWS supplies its VPC NFS CIDR", func(t *testing.T) {
		if got := NFSExportCIDRs(); !slices.Equal(got, []string{"10.0.0.0/16"}) {
			t.Fatalf("AWS NFSExportCIDRs = %q", got)
		}
	})
	t.Run("AWS conformance SSH user defaults to ubuntu", func(t *testing.T) {
		if got := SSHUser(); got != "ubuntu" {
			t.Fatalf("AWS SSHUser = %q, want ubuntu", got)
		}
	})
	t.Run("explicit NFS CIDR override wins", func(t *testing.T) {
		t.Setenv("E2E_NFS_EXPORT_CIDRS", "10.99.0.0/16")
		if err := Init(); err != nil {
			t.Fatalf("Init: %v", err)
		}
		if got := NFSExportCIDRs(); !slices.Equal(got, []string{"10.99.0.0/16"}) {
			t.Fatalf("NFSExportCIDRs override = %q", got)
		}
	})
	t.Run("explicit override wins", func(t *testing.T) {
		t.Setenv("E2E_KUBERNETES_VERSION", "v1.35.0")
		if err := Init(); err != nil {
			t.Fatalf("Init: %v", err)
		}
		if got := KubernetesVersion(); got != "v1.35.0" {
			t.Fatalf("override KubernetesVersion = %q, want %q", got, "v1.35.0")
		}
	})
}

func TestStorageOwnersLegacyCompatibility(t *testing.T) {
	t.Setenv("E2E_INFRASTRUCTURE_PROVIDER", "kubevirt")
	t.Setenv("E2E_DATA_DISK_BY_ID", "")
	t.Setenv("E2E_STORAGE_OWNERS", "")
	t.Setenv("E2E_CONSUMER_DOMAINS", "")
	if err := Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}

	owners, err := StorageOwners()
	if err != nil {
		t.Fatalf("StorageOwners: %v", err)
	}
	if len(owners) != 1 {
		t.Fatalf("StorageOwners count = %d, want legacy singleton", len(owners))
	}
	owner := owners[0]
	if owner.Name != "storage" || owner.MachineSuffix != "storage" || owner.PoolDeviceID != "/dev/disk/by-id/virtio-tank0" {
		t.Fatalf("legacy owner = %#v", owner)
	}
	if got, want := owner.NodeSelector[defaultStorageOwnerLabelKey], "storage"; got != want {
		t.Fatalf("legacy selector = %q, want %q", got, want)
	}
	if domains, err := ConsumerDomains(); err != nil || !slices.Equal(domains, []string{"workers"}) {
		t.Fatalf("legacy ConsumerDomains = %q, %v", domains, err)
	}
}

func TestInfrastructureConfigLegacyAndTwoOwnerFixtures(t *testing.T) {
	for _, fixture := range []struct {
		provider string
		mode     string
		count    int
	}{
		{provider: "kubevirt", mode: "legacy", count: 1},
		{provider: "kubevirt", mode: "two-owner", count: 2},
		{provider: "aws", mode: "legacy", count: 1},
		{provider: "aws", mode: "two-owner", count: 2},
	} {
		t.Run(fixture.provider+"/"+fixture.mode, func(t *testing.T) {
			t.Setenv(Env[InfrastructureProviderKey], fixture.provider)
			t.Setenv(Env[InfrastructureConfigKey], filepath.Join("..", "data", "infrastructure-"+fixture.provider, fixture.mode+".yaml"))
			t.Setenv(Env[StorageOwnersKey], "")
			if err := Init(); err != nil {
				t.Fatal(err)
			}
			owners, err := StorageOwners()
			if err != nil || len(owners) != fixture.count {
				t.Fatalf("StorageOwners = %#v, %v; want %d", owners, err, fixture.count)
			}
			workers, err := ConsumerWorkers()
			if err != nil || len(workers) != 1 {
				t.Fatalf("ConsumerWorkers = %#v, %v; want 1", workers, err)
			}
			for _, worker := range workers {
				if len(worker.NodeSelector) == 0 {
					t.Fatalf("worker %q has no deterministic node selector", worker.Name)
				}
			}
		})
	}
}

func TestStaticConsumerWorkersRequireStableNodeNames(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "static.yaml")
	config := `spec:
  provider: static
  flavor: zfs-csi
  consumerWorkers:
    - name: workers-a
      nodeNames: [worker-a, worker-b]
      replicas: 2
      networkDomain: fabric-a
`
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(Env[InfrastructureProviderKey], "static")
	t.Setenv(Env[InfrastructureConfigKey], configPath)
	if err := Init(); err != nil {
		t.Fatal(err)
	}
	workers, err := ConsumerWorkers()
	if err != nil || len(workers) != 1 || !slices.Equal(workers[0].NodeNames, []string{"worker-a", "worker-b"}) {
		t.Fatalf("ConsumerWorkers = %#v, %v", workers, err)
	}

	for name, replacement := range map[string]string{
		"missing node names":   "      nodeNames: []\n",
		"wrong replica count":  "      nodeNames: [worker-a]\n      replicas: 2\n",
		"duplicate node names": "      nodeNames: [worker-a, worker-a]\n",
	} {
		t.Run(name, func(t *testing.T) {
			body := strings.Replace(config, "      nodeNames: [worker-a, worker-b]\n", replacement, 1)
			path := filepath.Join(t.TempDir(), "invalid.yaml")
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv(Env[InfrastructureConfigKey], path)
			if err := Init(); err != nil {
				t.Fatal(err)
			}
			if _, err := ConsumerWorkers(); err == nil {
				t.Fatal("ConsumerWorkers accepted invalid static node identity")
			}
		})
	}
}

func TestValidateExplicitInfrastructureConfigRequiresConsumerReachability(t *testing.T) {
	fixture := filepath.Join("..", "data", "infrastructure-kubevirt", "two-owner.yaml")
	body, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	body = []byte(strings.Replace(string(body), "networkDomain: fabric-a\n      endpoints:\n        nfs: {ipv4: 10.19.1.21", "networkDomain: fabric-b\n      endpoints:\n        nfs: {ipv4: 10.19.1.21", 1))
	path := filepath.Join(t.TempDir(), "missing-reachability.yaml")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(Env[RunIDKey], "config-test")
	t.Setenv(Env[ConfigKey], filepath.Join("..", "e2e-config.yaml"))
	t.Setenv(Env[InfrastructureProviderKey], "kubevirt")
	t.Setenv(Env[InfrastructureConfigKey], path)
	t.Setenv(Env[StorageOwnersKey], "")
	if err := Init(); err != nil {
		t.Fatal(err)
	}
	err = Validate()
	if err == nil || !strings.Contains(err.Error(), `network domain "fabric-b" has no configured consumer reachability`) {
		t.Fatalf("Validate() error = %v, want missing consumer reachability", err)
	}
}

func TestStaticProviderSubstrateAcceptsLegacyAndTwoOwnersAndRejectsThree(t *testing.T) {
	for _, provider := range []string{"kubevirt", "aws"} {
		for _, mode := range []string{"legacy", "two-owner"} {
			t.Run(provider+"/"+mode, func(t *testing.T) {
				t.Setenv(Env[InfrastructureProviderKey], provider)
				t.Setenv(Env[InfrastructureConfigKey], filepath.Join("..", "data", "infrastructure-"+provider, mode+".yaml"))
				if err := Init(); err != nil {
					t.Fatal(err)
				}
				if err := ValidateStaticProviderSubstrate(); err != nil {
					t.Fatalf("%s substrate: %v", mode, err)
				}
			})
		}
	}

	fixture := filepath.Join("..", "data", "infrastructure-kubevirt", "two-owner.yaml")
	body, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	body = []byte(strings.Replace(string(body), "  consumerWorkers:", `    - name: storage-c
      machineDeploymentSuffix: storage-c
      nodeSelector: {zfs.csi.randomvariable.co.uk/storage-owner: storage-c}
      pool: {name: tank, diskID: tank-c, deviceID: /dev/disk/by-id/virtio-tank-c}
      networkDomain: fabric-a
      endpoints:
        nfs: {ipv4: 10.19.1.22, port: 2049}
        nvme: {ipv4: 10.19.1.22, port: 4420}
  consumerWorkers:`, 1))
	path := filepath.Join(t.TempDir(), "three-owner.yaml")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(Env[InfrastructureProviderKey], "kubevirt")
	t.Setenv(Env[InfrastructureConfigKey], path)
	if err := Init(); err != nil {
		t.Fatal(err)
	}
	err = ValidateStaticProviderSubstrate()
	if err == nil || !strings.Contains(err.Error(), "requests 3 owners") {
		t.Fatalf("ValidateStaticProviderSubstrate() error = %v, want unsupported owner count", err)
	}
}

func TestStaticProviderSubstrateRejectsUnknownRenderedSelectorBeforeLifecycle(t *testing.T) {
	fixture := filepath.Join("..", "data", "infrastructure-kubevirt", "two-owner.yaml")
	body, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	body = []byte(strings.Replace(string(body), "machineDeploymentSuffix: storage-b", "machineDeploymentSuffix: storage-c", 1))
	path := filepath.Join(t.TempDir(), "unknown-owner.yaml")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(Env[InfrastructureProviderKey], "kubevirt")
	t.Setenv(Env[InfrastructureConfigKey], path)
	if err := Init(); err != nil {
		t.Fatal(err)
	}
	err = ValidateStaticProviderSubstrate()
	if err == nil || !strings.Contains(err.Error(), "does not provision storage owner") {
		t.Fatalf("ValidateStaticProviderSubstrate() error = %v, want missing rendered owner", err)
	}
}

func TestCleanupOnlyStillRejectsUnsupportedStaticProviderSubstrate(t *testing.T) {
	fixture := filepath.Join("..", "data", "infrastructure-kubevirt", "two-owner.yaml")
	body, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	body = []byte(strings.Replace(string(body), "machineDeploymentSuffix: md-0", "machineDeploymentSuffix: workers-unknown", 1))
	path := filepath.Join(t.TempDir(), "unsupported-cleanup.yaml")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(Env[CleanupOnlyKey], "1")
	t.Setenv(Env[InfrastructureProviderKey], "kubevirt")
	t.Setenv(Env[InfrastructureConfigKey], path)
	if err := Init(); err != nil {
		t.Fatal(err)
	}
	if !IsCleanupOnly() {
		t.Fatal("cleanup-only config was not enabled")
	}
	err = ValidateStaticProviderSubstrate()
	if err == nil || !strings.Contains(err.Error(), "does not match rendered substrate") {
		t.Fatalf("ValidateStaticProviderSubstrate() error = %v, want cleanup-time substrate rejection", err)
	}
}

func TestCleanupOnlyAllowsLegacyStaticProviderSubstrate(t *testing.T) {
	for _, configPath := range []string{
		"",
		filepath.Join("..", "data", "infrastructure-kubevirt", "legacy.yaml"),
	} {
		t.Run(configPath, func(t *testing.T) {
			t.Setenv(Env[CleanupOnlyKey], "1")
			t.Setenv(Env[InfrastructureProviderKey], "kubevirt")
			t.Setenv(Env[InfrastructureConfigKey], configPath)
			if err := Init(); err != nil {
				t.Fatal(err)
			}
			if err := ValidateStaticProviderSubstrate(); err != nil {
				t.Fatalf("legacy cleanup substrate: %v", err)
			}
		})
	}
}

func TestStorageOwnersTwoOwnerContract(t *testing.T) {
	t.Setenv("E2E_RUN_ID", "config-test")
	t.Setenv("E2E_CONFIG", filepath.Join("..", "e2e-config.yaml"))
	t.Setenv("E2E_INFRASTRUCTURE_PROVIDER", "kubevirt")
	t.Setenv("E2E_STORAGE_OWNERS", strings.Join([]string{
		"storage-a,storage-a,/dev/disk/by-id/virtio-tank-a,fabric-a,10.19.1.20,10.19.1.20",
		"storage-b,storage-b,/dev/disk/by-id/virtio-tank-b,fabric-b,10.19.1.21,10.19.1.21",
	}, ";"))
	t.Setenv("E2E_CONSUMER_DOMAINS", "fabric-a,fabric-b")
	if err := Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}

	owners, err := StorageOwners()
	if err != nil {
		t.Fatalf("StorageOwners: %v", err)
	}
	if len(owners) != 2 {
		t.Fatalf("StorageOwners count = %d, want 2", len(owners))
	}
	if owners[0].Name != "storage-a" || owners[1].Name != "storage-b" {
		t.Fatalf("owner names = %q, %q", owners[0].Name, owners[1].Name)
	}
	if owners[0].NFSPort != 2049 || owners[0].NVMePort != 4420 || owners[0].StorageTaint != "zfs.csi.randomvariable.co.uk/storage=true:NoSchedule" {
		t.Fatalf("owner defaults = %#v", owners[0])
	}
	if err := Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	child := ChildEnv()
	if !slices.Contains(child, "E2E_CONSUMER_DOMAINS=fabric-a,fabric-b") {
		t.Fatalf("ChildEnv missing consumer domains: %v", child)
	}
}

func TestStorageOwnersAllowSharedNetworkDomain(t *testing.T) {
	t.Setenv("E2E_RUN_ID", "config-test")
	t.Setenv("E2E_CONFIG", filepath.Join("..", "e2e-config.yaml"))
	t.Setenv("E2E_INFRASTRUCTURE_PROVIDER", "kubevirt")
	t.Setenv("E2E_STORAGE_OWNERS", strings.Join([]string{
		"storage-a,storage-a,/dev/disk/by-id/virtio-tank-a,shared,10.19.1.20,10.19.1.20",
		"storage-b,storage-b,/dev/disk/by-id/virtio-tank-b,shared,10.19.1.21,10.19.1.21",
	}, ";"))
	t.Setenv("E2E_CONSUMER_DOMAINS", "shared")
	if err := Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	owners, err := StorageOwners()
	if err != nil {
		t.Fatalf("StorageOwners: %v", err)
	}
	if len(owners) != 2 || owners[0].NetworkDomain != "shared" || owners[1].NetworkDomain != "shared" {
		t.Fatalf("shared-domain owners = %#v", owners)
	}
}

func TestConsumerGroupsMayShareNetworkDomain(t *testing.T) {
	t.Setenv("E2E_CONSUMER_DOMAINS", "shared,shared")
	if err := Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	domains, err := ConsumerDomains()
	if err != nil {
		t.Fatalf("ConsumerDomains: %v", err)
	}
	if !slices.Equal(domains, []string{"shared", "shared"}) {
		t.Fatalf("ConsumerDomains = %v, want repeated shared reachability grouping", domains)
	}
}

func TestStorageOwnersDoNotHardCodeOwnerCount(t *testing.T) {
	for _, tc := range []struct {
		name    string
		owners  []string
		domains string
	}{
		{
			name: "one owner",
			owners: []string{
				"storage-a,storage-a,/dev/disk/by-id/virtio-tank-a,fabric-a,10.19.1.20,fd19:1::20",
			},
			domains: "fabric-a",
		},
		{
			name: "three owners",
			owners: []string{
				"storage-a,storage-a,/dev/disk/by-id/virtio-tank-a,fabric-a,10.19.1.20,fd19:1::20",
				"storage-b,storage-b,/dev/disk/by-id/virtio-tank-b,fabric-b,10.19.1.21,fd19:1::21",
				"storage-c,storage-c,/dev/disk/by-id/virtio-tank-c,fabric-c,10.19.1.22,fd19:1::22",
			},
			domains: "fabric-a,fabric-b,fabric-c",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("E2E_RUN_ID", "config-test")
			t.Setenv("E2E_CONFIG", filepath.Join("..", "e2e-config.yaml"))
			t.Setenv("E2E_INFRASTRUCTURE_PROVIDER", "kubevirt")
			t.Setenv("E2E_STORAGE_OWNERS", strings.Join(tc.owners, ";"))
			t.Setenv("E2E_CONSUMER_DOMAINS", tc.domains)
			if err := Init(); err != nil {
				t.Fatalf("Init: %v", err)
			}
			if err := Validate(); err != nil {
				t.Fatalf("Validate: %v", err)
			}
			owners, err := StorageOwners()
			if err != nil {
				t.Fatalf("StorageOwners: %v", err)
			}
			if len(owners) != len(tc.owners) {
				t.Fatalf("StorageOwners count = %d, want %d", len(owners), len(tc.owners))
			}
		})
	}
}

func TestSyntheticInfrastructureConfigSupportsThreeOwners(t *testing.T) {
	fixture := filepath.Join("..", "data", "infrastructure-kubevirt", "two-owner.yaml")
	body, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	body = []byte(strings.Replace(string(body), "  consumerWorkers:", `    - name: storage-c
      machineDeploymentSuffix: storage-c
      nodeSelector: {zfs.csi.randomvariable.co.uk/storage-owner: storage-c}
      pool: {name: tank, diskID: tank-c, deviceID: /dev/disk/by-id/virtio-tank-c}
      networkDomain: fabric-a
      endpoints:
        nfs: {ipv4: 10.19.1.22, port: 2049}
        nvme: {ipv4: 10.19.1.22, port: 4420}
  consumerWorkers:`, 1))
	path := filepath.Join(t.TempDir(), "synthetic-three-owner.yaml")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(Env[InfrastructureProviderKey], "kubevirt")
	t.Setenv(Env[InfrastructureConfigKey], path)
	if err := Init(); err != nil {
		t.Fatal(err)
	}
	owners, err := StorageOwners()
	if err != nil || len(owners) != 3 {
		t.Fatalf("StorageOwners = %#v, %v; want 3", owners, err)
	}
	workers, err := ConsumerWorkers()
	if err != nil || len(workers) != 1 {
		t.Fatalf("ConsumerWorkers = %#v, %v; want 1", workers, err)
	}
}

func TestStorageOwnersRejectDuplicatesAndMalformedInput(t *testing.T) {
	base := []string{
		"storage-a,storage-a,/dev/disk/by-id/virtio-tank-a,fabric-a,10.19.1.20,10.19.1.20",
		"storage-b,storage-b,/dev/disk/by-id/virtio-tank-b,fabric-b,10.19.1.21,10.19.1.21",
	}
	tests := []struct {
		name    string
		owners  string
		domains string
	}{
		{name: "duplicate names", owners: strings.Join([]string{base[0], strings.Replace(base[1], "storage-b,", "storage-a,", 1)}, ";"), domains: "fabric-a,fabric-b"},
		{name: "duplicate selectors", owners: strings.Join([]string{base[0], strings.Replace(base[1], "storage-b,/dev", "storage-a,/dev", 1)}, ";"), domains: "fabric-a,fabric-b"},
		{name: "duplicate devices", owners: strings.Join([]string{base[0], strings.Replace(base[1], "virtio-tank-b", "virtio-tank-a", 1)}, ";"), domains: "fabric-a,fabric-b"},
		{name: "missing consumer domain", owners: strings.Join(base, ";"), domains: "fabric-a"},
		{name: "non by-id device", owners: strings.Replace(strings.Join(base, ";"), "/dev/disk/by-id/virtio-tank-b", "/dev/vdb", 1), domains: "fabric-a,fabric-b"},
		{name: "wildcard by-id device", owners: strings.Replace(strings.Join(base, ";"), "virtio-tank-b", "virtio-tank-*", 1), domains: "fabric-a,fabric-b"},
		{name: "ambiguous EBS by-id prefix", owners: strings.Replace(strings.Join(base, ";"), "/dev/disk/by-id/virtio-tank-b", "/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_", 1), domains: "fabric-a,fabric-b"},
		{name: "missing endpoint", owners: strings.Replace(strings.Join(base, ";"), ",10.19.1.21,10.19.1.21", ",10.19.1.21,", 1), domains: "fabric-a,fabric-b"},
		{name: "duplicate NFS endpoint", owners: strings.Join([]string{base[0], strings.Replace(base[1], "10.19.1.21,10.19.1.21", "10.19.1.20,10.19.1.21", 1)}, ";"), domains: "fabric-a,fabric-b"},
		{name: "duplicate NVMe endpoint", owners: strings.Join([]string{
			"storage-a,storage-a,/dev/disk/by-id/virtio-tank-a,fabric-a,10.19.1.20,fd19:1::20",
			"storage-b,storage-b,/dev/disk/by-id/virtio-tank-b,fabric-b,10.19.1.21,fd19:0001:0:0:0:0:0:20",
		}, ";"), domains: "fabric-a,fabric-b"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("E2E_RUN_ID", "config-test")
			t.Setenv("E2E_CONFIG", filepath.Join("..", "e2e-config.yaml"))
			t.Setenv("E2E_INFRASTRUCTURE_PROVIDER", "kubevirt")
			t.Setenv("E2E_STORAGE_OWNERS", tc.owners)
			t.Setenv("E2E_CONSUMER_DOMAINS", tc.domains)
			if err := Init(); err != nil {
				t.Fatalf("Init: %v", err)
			}
			if err := Validate(); err == nil {
				t.Fatal("Validate succeeded for invalid owner contract")
			}
		})
	}
}

// TestEncryptionEnabledDefaultsOn pins the encryption knob's default-true
// semantics: unset -> true (OpenBao + encrypted SC + encrypted testdriver run by
// default), E2E_ENCRYPTION=0 -> false. The default-true bool is easy to get
// wrong in the ChildEnv bridge (a skipped false would let the child re-default
// to true), so this also asserts ChildEnv emits E2E_ENCRYPTION=0 explicitly when
// disabled and omits it when enabled (the default the child already has).
func TestEncryptionEnabledDefaultsOn(t *testing.T) {
	t.Setenv("E2E_INFRASTRUCTURE_PROVIDER", "")
	t.Setenv("E2E_FLAVOR", "")
	t.Setenv("E2E_DATA_DISK_BY_ID", "")
	t.Setenv("E2E_KUBERNETES_VERSION", "")

	t.Run("default is on", func(t *testing.T) {
		t.Setenv("E2E_ENCRYPTION", "")
		if err := Init(); err != nil {
			t.Fatalf("Init: %v", err)
		}
		if !EncryptionEnabled() {
			t.Fatal("EncryptionEnabled default = false, want true (encryption tested by default)")
		}
		// Enabled matches the child's own default, so ChildEnv omits it.
		if slices.ContainsFunc(ChildEnv(), func(kv string) bool { return kv == "E2E_ENCRYPTION=1" || kv == "E2E_ENCRYPTION=0" }) {
			// Emitting "1" is harmless; emitting "0" here would be a bug. Assert
			// specifically that it is not disabled.
			for _, kv := range ChildEnv() {
				if kv == "E2E_ENCRYPTION=0" {
					t.Fatalf("ChildEnv wrongly emitted %q when encryption is enabled", kv)
				}
			}
		}
	})

	t.Run("E2E_ENCRYPTION=0 disables and ChildEnv propagates it", func(t *testing.T) {
		t.Setenv("E2E_ENCRYPTION", "0")
		if err := Init(); err != nil {
			t.Fatalf("Init: %v", err)
		}
		if EncryptionEnabled() {
			t.Fatal("EncryptionEnabled with E2E_ENCRYPTION=0 = true, want false")
		}
		if !slices.Contains(ChildEnv(), "E2E_ENCRYPTION=0") {
			t.Fatalf("ChildEnv must emit E2E_ENCRYPTION=0 when disabled (else child re-defaults to true); got %v", ChildEnv())
		}
	})
}

func TestConformanceRequiresReadableParseableSSHPrivateKey(t *testing.T) {
	t.Setenv("E2E_RUN_ID", "config-test")
	t.Setenv("E2E_CONFIG", filepath.Join("..", "e2e-config.yaml"))
	t.Setenv("E2E_INFRASTRUCTURE_PROVIDER", "aws")
	t.Setenv("E2E_RUN_CONFORMANCE", "1")
	t.Setenv("E2E_SSH_PRIVATE_KEY_PATH", "")
	if err := Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}

	t.Run("missing path", func(t *testing.T) {
		t.Setenv("E2E_CLEANUP_ONLY", "0")
		if err := Validate(); err == nil {
			t.Fatal("Validate succeeded without E2E_SSH_PRIVATE_KEY_PATH")
		}
	})

	t.Run("cleanup-only bypasses key validation", func(t *testing.T) {
		t.Setenv("E2E_CLEANUP_ONLY", "1")
		t.Setenv("E2E_SSH_PRIVATE_KEY_PATH", filepath.Join(t.TempDir(), "missing-key"))
		if err := Init(); err != nil {
			t.Fatalf("Init: %v", err)
		}
		if err := Validate(); err != nil {
			t.Fatalf("Validate in cleanup-only mode: %v", err)
		}
	})

	t.Run("unreadable path", func(t *testing.T) {
		t.Setenv("E2E_CLEANUP_ONLY", "0")
		path := filepath.Join(t.TempDir(), "missing-key")
		t.Setenv("E2E_SSH_PRIVATE_KEY_PATH", path)
		if err := Init(); err != nil {
			t.Fatalf("Init: %v", err)
		}
		if err := Validate(); err == nil {
			t.Fatal("Validate succeeded with an unreadable SSH key path")
		}
	})

	t.Run("invalid key", func(t *testing.T) {
		t.Setenv("E2E_CLEANUP_ONLY", "0")
		path := filepath.Join(t.TempDir(), "invalid-key")
		if err := os.WriteFile(path, []byte("not a private key"), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("E2E_SSH_PRIVATE_KEY_PATH", path)
		if err := Init(); err != nil {
			t.Fatalf("Init: %v", err)
		}
		if err := Validate(); err == nil {
			t.Fatal("Validate succeeded with a non-parseable SSH key")
		}
	})

	t.Run("empty key", func(t *testing.T) {
		t.Setenv("E2E_CLEANUP_ONLY", "0")
		path := filepath.Join(t.TempDir(), "empty-key")
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("E2E_SSH_PRIVATE_KEY_PATH", path)
		if err := Init(); err != nil {
			t.Fatalf("Init: %v", err)
		}
		if err := Validate(); err == nil {
			t.Fatal("Validate succeeded with an empty SSH key")
		}
	})

	t.Run("encrypted key is actionable", func(t *testing.T) {
		t.Setenv("E2E_CLEANUP_ONLY", "0")
		path := filepath.Join(t.TempDir(), "encrypted-key")
		if err := os.WriteFile(path, []byte(testEncryptedOpenSSHPrivateKey), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("E2E_SSH_PRIVATE_KEY_PATH", path)
		if err := Init(); err != nil {
			t.Fatalf("Init: %v", err)
		}
		err := Validate()
		if err == nil || !strings.Contains(err.Error(), "encrypted private keys are unsupported") {
			t.Fatalf("Validate error = %v, want unsupported encrypted-key guidance", err)
		}
	})

	t.Run("valid OpenSSH ed25519 key is propagated", func(t *testing.T) {
		t.Setenv("E2E_CLEANUP_ONLY", "0")
		path := filepath.Join(t.TempDir(), "ssh-key")
		if err := os.WriteFile(path, []byte(testOpenSSHPrivateKey), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("E2E_SSH_PRIVATE_KEY_PATH", path)
		if err := Init(); err != nil {
			t.Fatalf("Init: %v", err)
		}
		if err := Validate(); err != nil {
			t.Fatalf("Validate: %v", err)
		}
		if got := SSHPrivateKeyPath(); got != path {
			t.Fatalf("SSHPrivateKeyPath = %q, want %q", got, path)
		}
		if !slices.Contains(ChildEnv(), "E2E_SSH_PRIVATE_KEY_PATH="+path) {
			t.Fatalf("ChildEnv did not propagate SSH key path: %v", ChildEnv())
		}
	})
}
