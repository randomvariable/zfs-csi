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

// Package e2econfig is the single source of truth for zfs-csi E2E
// configuration knobs. It is imported by both the Magefile (magefiles/) and the
// Ginkgo test suite (test/e2e) so that flags, env-var bindings, defaults and
// help text are defined exactly once.
//
// Each knob is exposed as a pflag (with help text) whose name is also its
// viper key. github.com/randomvariable/mage-common/config binds pflags to viper
// and enables AutomaticEnv with a "-" -> "_" / "." -> "_" replacer, so a flag
// like "e2e-run-id" resolves the env var E2E_RUN_ID automatically. This means a
// knob can be set three ways, in increasing precedence: default -> env var ->
// --flag.
//
// Usage (mage target):
//
//	func (E2e) Foo(ctx context.Context) error {
//	    if err := e2econfig.Init(); err != nil { return err }
//	    runID := e2econfig.RunID()
//	    ...
//	}
//
// Usage (test suite):
//
//	func TestE2E(t *testing.T) {
//	    e2econfig.Register(pflag.CommandLine)
//	    if err := e2econfig.Init(); err != nil { t.Fatal(err) }
//	    RegisterFailHandler(Fail)
//	    RunSpecs(t, "...")
//	}
//
// Bridging mage -> `go test` subprocess: a mage target reads knobs via the
// accessors below, then passes them into the child process environment using
// ChildEnv() so the child's own Init()+AutomaticEnv picks them up without the
// caller having to know each env-var name.
package e2econfig

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/randomvariable/mage-common/config"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"golang.org/x/crypto/ssh"
	"sigs.k8s.io/yaml"
)

// Flag (== viper key) names. Each flag also resolves its upper-snake env var
// (e.g. RunID -> E2E_RUN_ID) via mage-common/config's AutomaticEnv replacer.
// Key constants are the pflag names AND viper keys for each knob. Accessors
// below read them via viper.GetString/GetBool.
const (
	RunIDKey          = "e2e-run-id"
	ConfigKey         = "e2e-config"
	HostKubeconfigKey = "e2e-host-kubeconfig"
	CleanupOnlyKey    = "e2e-cleanup-only"
	SkipCleanupKey    = "e2e-skip-cleanup"
	StoragePoolKey    = "e2e-zpool"
	DriverImageKey    = "e2e-driver-image"
	PreflightImageKey = "e2e-preflight-image"
	ChartRefKey       = "e2e-chart-ref"
	KubeconfigOutKey  = "e2e-kubeconfig"
	// Provider-seam knobs (memory: CAPA/KubeVirt lane switch). Defaults select
	// the existing KubeVirt lane, so the lane stays identical when these are
	// unset. The AWS lane sets E2E_INFRASTRUCTURE_PROVIDER=aws + E2E_FLAVOR=
	// zfs-csi-aws + E2E_DATA_DISK_BY_ID=<ebs/nvme by-id>.
	InfrastructureProviderKey = "e2e-infrastructure-provider"
	FlavorKey                 = "e2e-flavor"
	DataDiskByIDKey           = "e2e-data-disk-by-id"
	DataDiskByIDBKey          = "e2e-data-disk-by-id-b"
	StorageOwnersKey          = "e2e-storage-owners"
	ConsumerDomainsKey        = "e2e-consumer-domains"
	InfrastructureConfigKey   = "e2e-infrastructure-config"
	// KubernetesVersionKey overrides the workload Kubernetes version. Unset, the
	// version defaults per provider (see KubernetesVersion): both the KubeVirt
	// and AWS lanes pin v1.36.2 (KubeVirt from the QEMU golden image, AWS from
	// the custom image-builder AMI). An explicit value overrides either lane.
	KubernetesVersionKey = "e2e-kubernetes-version"
	// NFSExportCIDRsKey overrides the provider fixture CIDRs the chart sets on the
	// zfs-tank-nfs StorageClass (sharenfs rw=@<cidr>).
	NFSExportCIDRsKey = "e2e-nfs-export-cidrs"
	// RunConformanceKey gates the (long, ~30-60m) external-storage conformance
	// spec. Opt-in: unset, the conformance It skips. Set E2E_RUN_CONFORMANCE=1 to
	// run the upstream suite against both testdrivers after the smokes pass.
	RunConformanceKey = "e2e-run-conformance"
	// SSHPrivateKeyPathKey identifies the local private key used by the
	// conformance skeleton provider to SSH into workload nodes. The key remains
	// on the launch host and is never persisted in the management cluster.
	SSHPrivateKeyPathKey = "e2e-ssh-private-key-path"
	// SSHUserKey is the workload-node and CAPA bastion login used by the
	// conformance skeleton provider. AWS Ubuntu images use ubuntu.
	SSHUserKey = "e2e-ssh-user"
	// ConformanceFocusKey / ConformanceSkipKey override the ginkgo focus/skip
	// regexes (defaults in conformance.go select External.Storage.*co.uk and skip
	// Disruptive/Serial/Slow/Feature). ConformanceImageKey overrides the derived
	// registry.k8s.io/conformance:vX image (air-gapped / mirror registries).
	ConformanceFocusKey = "e2e-conformance-focus"
	ConformanceSkipKey  = "e2e-conformance-skip"
	ConformanceImageKey = "e2e-conformance-image"
	// ConformanceDryRunKey adds ginkgo --dry-run: the mandatory ~1-2m pre-flight
	// that compiles focus/skip, loads+registers both testdrivers (surfacing YAML
	// errors), and prints "Will run N of M" WITHOUT touching the cluster. Run
	// once before a real (60-120m) conformance run.
	ConformanceDryRunKey = "e2e-conformance-dry-run"
	// EncryptionEnabledKey gates whether the lane deploys OpenBao (dev-mode
	// Transit) and installs the chart with per-volume ZFS native encryption
	// enabled. When on, the chart renders the zfs-tank-nvme-encrypted
	// StorageClass and the conformance suite additionally exercises an encrypted
	// testdriver. Defaults on; set E2E_ENCRYPTION=0 to skip OpenBao + the
	// encrypted SC (e.g. a fast smoke-only run).
	EncryptionEnabledKey = "e2e-encryption"
	// TransportTLSEnabledKey gates TLS StorageClasses, smoke probes, and TLS
	// conformance drivers. It defaults off so ordinary E2E behavior is unchanged.
	TransportTLSEnabledKey = "e2e-transport-tls"
	ConformanceTLSOnlyKey  = "e2e-conformance-tls-only"
	// PodCertificateAcceptanceKey gates the bounded AWS acceptance that inspects
	// kubelet-created PCRs and tlshd trust configuration. It is separate from the
	// ordinary TLS smoke because rotation waits can consume several hours.
	PodCertificateAcceptanceKey = "e2e-pod-certificate-acceptance"
	// WorkloadKubeconfigKey points the static provider at the kubeconfig of the
	// PRE-EXISTING workload cluster the suite runs against. Required when
	// E2E_INFRASTRUCTURE_PROVIDER=static; ignored by the CAPI-provisioning lanes
	// (kubevirt/aws), which obtain the workload kubeconfig from the framework.
	WorkloadKubeconfigKey = "e2e-workload-kubeconfig"
	// NonBlockingTaintsKey overrides the conformance suite's
	// --non-blocking-taints list (comma-separated taint keys the node-readiness
	// preflight must ignore). Unset keeps the current in-tree default.
	NonBlockingTaintsKey = "e2e-non-blocking-taints"
	// AllowedNotReadyNodesKey sets the conformance suite's
	// --allowed-not-ready-nodes count. Unset defaults per provider: 1 for the
	// static (shared, pre-existing cluster) lane where an unrelated NotReady
	// node must not block the run, 0 otherwise (unchanged behaviour).
	AllowedNotReadyNodesKey = "e2e-allowed-not-ready-nodes"
	// ConformanceDisruptiveKey opts a static-provider conformance run into the
	// [Disruptive]/[Serial] specs. Default off: on a shared pre-existing cluster
	// the static lane skips those specs (see ConformanceSkip) and the SSH key
	// requirement is relaxed. Setting E2E_CONFORMANCE_DISRUPTIVE=1 restores both.
	ConformanceDisruptiveKey = "e2e-conformance-disruptive"
	// SkipSnapshotBundleKey skips force-applying the vendored
	// external-snapshotter bundle when the cluster already serves the
	// VolumeSnapshot CRDs (shared clusters own their snapshot-controller).
	// Implied by the static provider; the VolumeSnapshotClass is still ensured.
	SkipSnapshotBundleKey = "e2e-skip-snapshot-bundle"
	// StorageClassOverridesKey renames chart-installed StorageClasses so a run
	// on a shared cluster cannot collide with pre-existing classes of the same
	// name. Comma-separated chartKey=name pairs (e.g. tankNVMe=e2e-tank-nvme).
	// The harness passes matching --set storageClasses.<key>.name overrides to
	// helm and rewrites testdriver FromExistingClassName references to match.
	StorageClassOverridesKey = "e2e-storage-class-overrides"
)

// Env maps each viper key to its canonical env-var name. Used by ChildEnv() to
// bridge mage-side values into a child `go test` process, and kept here so the
// env-var contract is documented in one place.
var Env = map[string]string{
	RunIDKey:                    "E2E_RUN_ID",
	ConfigKey:                   "E2E_CONFIG",
	HostKubeconfigKey:           "E2E_HOST_KUBECONFIG",
	CleanupOnlyKey:              "E2E_CLEANUP_ONLY",
	SkipCleanupKey:              "E2E_SKIP_CLEANUP",
	StoragePoolKey:              "E2E_ZPOOL",
	DriverImageKey:              "E2E_DRIVER_IMAGE",
	PreflightImageKey:           "E2E_PREFLIGHT_IMAGE",
	ChartRefKey:                 "E2E_CHART_REF",
	KubeconfigOutKey:            "E2E_KUBECONFIG",
	InfrastructureProviderKey:   "E2E_INFRASTRUCTURE_PROVIDER",
	FlavorKey:                   "E2E_FLAVOR",
	DataDiskByIDKey:             "E2E_DATA_DISK_BY_ID",
	DataDiskByIDBKey:            "E2E_DATA_DISK_BY_ID_B",
	StorageOwnersKey:            "E2E_STORAGE_OWNERS",
	ConsumerDomainsKey:          "E2E_CONSUMER_DOMAINS",
	InfrastructureConfigKey:     "E2E_INFRASTRUCTURE_CONFIG",
	KubernetesVersionKey:        "E2E_KUBERNETES_VERSION",
	NFSExportCIDRsKey:           "E2E_NFS_EXPORT_CIDRS",
	EncryptionEnabledKey:        "E2E_ENCRYPTION",
	TransportTLSEnabledKey:      "E2E_TRANSPORT_TLS",
	ConformanceTLSOnlyKey:       "E2E_CONFORMANCE_TLS_ONLY",
	PodCertificateAcceptanceKey: "E2E_POD_CERTIFICATE_ACCEPTANCE",
	RunConformanceKey:           "E2E_RUN_CONFORMANCE",
	SSHPrivateKeyPathKey:        "E2E_SSH_PRIVATE_KEY_PATH",
	SSHUserKey:                  "E2E_SSH_USER",
	ConformanceFocusKey:         "E2E_CONFORMANCE_FOCUS",
	ConformanceSkipKey:          "E2E_CONFORMANCE_SKIP",
	ConformanceImageKey:         "E2E_CONFORMANCE_IMAGE",
	ConformanceDryRunKey:        "E2E_CONFORMANCE_DRY_RUN",
	WorkloadKubeconfigKey:       "E2E_WORKLOAD_KUBECONFIG",
	NonBlockingTaintsKey:        "E2E_NON_BLOCKING_TAINTS",
	AllowedNotReadyNodesKey:     "E2E_ALLOWED_NOT_READY_NODES",
	ConformanceDisruptiveKey:    "E2E_CONFORMANCE_DISRUPTIVE",
	SkipSnapshotBundleKey:       "E2E_SKIP_SNAPSHOT_BUNDLE",
	StorageClassOverridesKey:    "E2E_STORAGE_CLASS_OVERRIDES",
}

// Register defines all E2E flags on fs with help text and defaults. Call once
// (typically in init() or TestMain) before config.Init(). Defining a flag that
// already exists is an error, so callers must not double-register.
func Register(fs *pflag.FlagSet) {
	fs.String(RunIDKey, "",
		"E2E run ID. Per-run resources live in cluster zfs-csi-e2e-<run-id>. "+
			"Auto-generated by `mage e2e:up` and pinned to test/e2e/_artifacts/e2e-run.json if unset. [env E2E_RUN_ID]")
	fs.String(ConfigKey, "",
		"Path to the CAPI e2e-config.yaml. Defaults to test/e2e/e2e-config.yaml. [env E2E_CONFIG]")
	fs.String(HostKubeconfigKey, "",
		"Path to the management cluster kubeconfig used by clusterctl/the test framework. "+
			"Defaults to KUBECONFIG/~/.kube/config. [env E2E_HOST_KUBECONFIG]")
	fs.Bool(CleanupOnlyKey, false,
		"Run only the teardown phase (destroy the per-run cluster). Equivalent to `mage e2e:down`. [env E2E_CLEANUP_ONLY]")
	fs.Bool(SkipCleanupKey, false,
		"Provision + run tests but leave the cluster standing on exit. [env E2E_SKIP_CLEANUP]")
	fs.String(StoragePoolKey, "tank",
		"Name of the ZFS pool the storage node provisions and the driver serves. [env E2E_ZPOOL]")
	fs.String(DriverImageKey, "",
		"Container image (with libzfs) for the zfs-csi driver, e.g. harbor.../zfs-csi@sha256:... . "+
			"Required for the driver-install + PVC smoke specs. [env E2E_DRIVER_IMAGE]")
	fs.String(PreflightImageKey, defaultPreflightImage,
		"Image used by the storage-node libzfs provenance preflight pod. [env E2E_PREFLIGHT_IMAGE]")
	fs.String(ChartRefKey, defaultChartRef,
		"Helm chart reference for the driver install: an OCI ref / repo-chart in CI, or the in-repo path for local runs. [env E2E_CHART_REF]")
	fs.String(KubeconfigOutKey, "",
		"If set, `mage e2e:kubeconfig` writes the workload kubeconfig here instead of stdout. [env E2E_KUBECONFIG]")
	// Provider seam. Defaults keep the existing KubeVirt lane unchanged.
	fs.String(InfrastructureProviderKey, defaultInfrastructureProvider,
		"Infrastructure provider for the lifecycle lane (kubevirt|aws|static). "+
			"Default kubevirt selects the in-tree KubeVirt-on-Ceph lane; aws selects the CAPA EC2 lane; "+
			"static runs against a PRE-EXISTING cluster reached via E2E_WORKLOAD_KUBECONFIG (no CAPI provisioning, no cluster teardown). [env E2E_INFRASTRUCTURE_PROVIDER]")
	fs.String(FlavorKey, defaultFlavor,
		"clusterctl flavor (cluster-template-<flavor>.yaml). Default zfs-csi (KubeVirt); "+
			"aws lane uses zfs-csi-aws. [env E2E_FLAVOR]")
	fs.String(DataDiskByIDKey, defaultDataDiskByID,
		"Block device path (by-id) for the storage-node ZFS pool disk. "+
			"Default /dev/disk/by-id/virtio-tank0 (KubeVirt virtio-serial data disk); "+
			"aws lane uses the EBS/NVMe by-id of the dedicated gp3 volume. [env E2E_DATA_DISK_BY_ID]")
	fs.String(DataDiskByIDBKey, "",
		"Optional legacy-compatible second owner pool device by-id. Unset means no second owner. [env E2E_DATA_DISK_BY_ID_B]")
	fs.String(StorageOwnersKey, "",
		"Semicolon-separated owner substrate contract. Each owner is name,selector,device,domain,nfs-host,nvme-host; "+
			"unset synthesizes the legacy single storage owner. Pool GUIDs are intentionally discovered live later. [env E2E_STORAGE_OWNERS]")
	fs.String(ConsumerDomainsKey, "",
		"Comma-separated consumer topology domains. Unset preserves the legacy workers domain. [env E2E_CONSUMER_DOMAINS]")
	fs.String(InfrastructureConfigKey, "",
		"Provider-neutral InfrastructureConfig YAML consumed by the multi-owner harness. Unset preserves legacy env-based synthesis. [env E2E_INFRASTRUCTURE_CONFIG]")
	fs.String(KubernetesVersionKey, "",
		"Workload Kubernetes version. Unset, it defaults per provider: "+
			"v1.36.2 (kubevirt), v1.36.2 (aws, custom image-builder AMI via mage e2e:imageBuildAWS). [env E2E_KUBERNETES_VERSION]")
	fs.String(NFSExportCIDRsKey, "",
		"NFS export CIDR for the zfs-tank-nfs StorageClass (sharenfs rw=@<cidr>). "+
			"Unset, it defaults to the selected E2E provider fixture CIDR. "+
			"[env E2E_NFS_EXPORT_CIDRS]")
	fs.Bool(EncryptionEnabledKey, true,
		"Deploy OpenBao (dev-mode Transit) and install the chart with per-volume "+
			"ZFS native encryption enabled (renders the zfs-tank-nvme-encrypted "+
			"StorageClass; conformance also runs an encrypted testdriver). Default "+
			"on; set E2E_ENCRYPTION=0 to skip. [env E2E_ENCRYPTION]")
	fs.Bool(TransportTLSEnabledKey, false,
		"Enable transport-TLS StorageClasses, smoke probes, and TLS conformance testdrivers. "+
			"Opt-in; default off. [env E2E_TRANSPORT_TLS]")
	fs.Bool(ConformanceTLSOnlyKey, false,
		"Run conformance only against transport-TLS testdrivers. Requires E2E_TRANSPORT_TLS=1. [env E2E_CONFORMANCE_TLS_ONLY]")
	fs.Bool(PodCertificateAcceptanceKey, false,
		"Run bounded AWS PodCertificate NFS mTLS acceptance, including a natural kubelet rotation wait. "+
			"Requires E2E_TRANSPORT_TLS=1. [env E2E_POD_CERTIFICATE_ACCEPTANCE]")
	fs.Bool(RunConformanceKey, false,
		"Run the upstream external-storage conformance suite (both testdrivers) "+
			"after the smokes. Opt-in: long (~30-60m). [env E2E_RUN_CONFORMANCE]")
	fs.String(SSHPrivateKeyPathKey, "",
		"Path to the local workload-node SSH private key required when conformance is enabled. "+
			"The key is read by the launcher and is not stored in Kubernetes. [env E2E_SSH_PRIVATE_KEY_PATH]")
	fs.String(SSHUserKey, "",
		"SSH username for conformance workload nodes and the optional bastion. "+
			"Unset defaults by provider (aws: ubuntu). [env E2E_SSH_USER]")
	fs.String(ConformanceFocusKey, "",
		"Override the conformance ginkgo -focus regex. Unset, defaults to "+
			"External.Storage.*co.uk. [env E2E_CONFORMANCE_FOCUS]")
	fs.String(ConformanceSkipKey, "",
		"Override the conformance ginkgo -skip regex. Unset, defaults to "+
			"Disruptive|Serial|Slow|Feature. [env E2E_CONFORMANCE_SKIP]")
	fs.String(ConformanceImageKey, "",
		"Override the conformance image (registry.k8s.io/conformance:vX). Unset, "+
			"derived from the workload Kubernetes version. [env E2E_CONFORMANCE_IMAGE]")
	fs.Bool(ConformanceDryRunKey, false,
		"Run the conformance spec with ginkgo --dry-run: enumerate specs + load "+
			"testdrivers without touching the cluster (~1-2m pre-flight). "+
			"[env E2E_CONFORMANCE_DRY_RUN]")
	fs.String(WorkloadKubeconfigKey, "",
		"Kubeconfig of the pre-existing workload cluster for the static provider. "+
			"Required when the infrastructure provider is static; unused otherwise. [env E2E_WORKLOAD_KUBECONFIG]")
	fs.String(NonBlockingTaintsKey, "",
		"Comma-separated taint keys the conformance node-readiness preflight ignores. "+
			"Unset keeps the in-tree default (storage + control-plane taints). [env E2E_NON_BLOCKING_TAINTS]")
	fs.String(AllowedNotReadyNodesKey, "",
		"Conformance --allowed-not-ready-nodes count. Unset defaults to 1 for the static "+
			"provider (shared clusters may carry an unrelated NotReady node) and 0 otherwise. [env E2E_ALLOWED_NOT_READY_NODES]")
	fs.Bool(ConformanceDisruptiveKey, false,
		"Opt a static-provider conformance run into [Disruptive]/[Serial] specs "+
			"(requires the SSH key and a dedicated maintenance window). Default off. [env E2E_CONFORMANCE_DISRUPTIVE]")
	fs.Bool(SkipSnapshotBundleKey, false,
		"Skip force-applying the vendored external-snapshotter bundle when the "+
			"VolumeSnapshot CRDs already exist (implied by the static provider). "+
			"The VolumeSnapshotClass is still ensured. [env E2E_SKIP_SNAPSHOT_BUNDLE]")
	fs.String(StorageClassOverridesKey, "",
		"Comma-separated chartKey=name StorageClass renames (e.g. tankNVMe=e2e-tank-nvme) "+
			"to avoid name collisions on shared clusters. Applied to the helm install and "+
			"the generated testdriver copies. [env E2E_STORAGE_CLASS_OVERRIDES]")
}

const (
	defaultPreflightImage         = "ghcr.io/randomvariable/zfs-csi:e2e-libzfs"
	defaultChartRef               = "charts/zfs-csi"
	defaultConfigRel              = "test/e2e/e2e-config.yaml"
	defaultInfrastructureProvider = "kubevirt"
	defaultFlavor                 = "zfs-csi"
	defaultDataDiskByID           = "/dev/disk/by-id/virtio-tank0"
	defaultLegacyStorageOwnerName = "storage"
	defaultStorageOwnerLabelKey   = "zfs.csi.randomvariable.co.uk/storage-owner"
	defaultStorageTaintKey        = "zfs.csi.randomvariable.co.uk/storage"
	defaultStorageTaintValue      = "true"
	defaultNFSChannelPort         = 2049
	defaultNVMeChannelPort        = 4420
	// Per-provider workload Kubernetes version defaults. Both lanes run the
	// golden-image-baked version: KubeVirt from the QEMU image, AWS from the
	// custom image-builder AMI (mage e2e:imageBuildAWS) which bakes v1.36.2.
	// AWS was previously capped at v1.34.8 (newest CAPA-published ubuntu-24.04
	// AMI); building our own AMI lifts that cap, so both now pin v1.36.2. An
	// explicit E2E_KUBERNETES_VERSION still overrides per lane.
	defaultKubeVirtKubernetesVersion = "v1.36.2"
	defaultAWSKubernetesVersion      = "v1.36.2"
	// Provider fixture CIDRs permit only the E2E consumer-node network. They are
	// not product defaults and must remain explicit at chart installation time.
	defaultKubeVirtNFSExportCIDR = "192.0.2.0/24"
	defaultAWSNFSExportCIDR      = "10.0.0.0/16"
)

// Init binds flags + env vars via mage-common/config. Idempotent and safe to
// call from every mage target / the test suite entrypoint.
func Init() error {
	return config.Init()
}

// --- accessors (read via viper after Init) -----------------------------------

// RunID returns the configured run ID.
func RunID() string { return viper.GetString(RunIDKey) }

// ConfigPath returns the e2e-config.yaml path, resolving the in-repo default
// relative to the working directory when unset.
func ConfigPath() (string, error) {
	v := strings.TrimSpace(viper.GetString(ConfigKey))
	if v != "" {
		return v, nil
	}
	abs, err := filepath.Abs(defaultConfigRel)
	if err != nil {
		return "", fmt.Errorf("resolve default e2e config path: %w", err)
	}
	return abs, nil
}

// HostKubeconfigPath returns the management kubeconfig path, falling back to
// KUBECONFIG then ~/.kube/config.
func HostKubeconfigPath() string {
	v := strings.TrimSpace(viper.GetString(HostKubeconfigKey))
	if v != "" {
		return v
	}
	if v := strings.TrimSpace(os.Getenv("KUBECONFIG")); v != "" {
		return v
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".kube", "config")
}

// IsCleanupOnly reports whether only the teardown phase should run.
func IsCleanupOnly() bool { return viper.GetBool(CleanupOnlyKey) }

// IsSkipCleanup reports whether teardown should be skipped after a provision/test run.
func IsSkipCleanup() bool { return viper.GetBool(SkipCleanupKey) }

// PoolName returns the ZFS pool name (default "tank").
func PoolName() string {
	if v := strings.TrimSpace(viper.GetString(StoragePoolKey)); v != "" {
		return v
	}
	return "tank"
}

// DriverImageRef returns the configured driver image, or "" if unset.
func DriverImageRef() string { return strings.TrimSpace(viper.GetString(DriverImageKey)) }

// PreflightImageRef returns the preflight image, defaulting to the driver image
// when set, else the configured/default preflight image.
func PreflightImageRef() string {
	if img := DriverImageRef(); img != "" {
		return img
	}
	if v := strings.TrimSpace(viper.GetString(PreflightImageKey)); v != "" {
		return v
	}
	return defaultPreflightImage
}

// ChartReference returns the helm chart ref (default "charts/zfs-csi").
func ChartReference() string {
	if v := strings.TrimSpace(viper.GetString(ChartRefKey)); v != "" {
		return v
	}
	return defaultChartRef
}

// KubeconfigOutPath returns the optional kubeconfig output path for
// `mage e2e:kubeconfig`, or "" (stdout).
func KubeconfigOutPath() string { return strings.TrimSpace(viper.GetString(KubeconfigOutKey)) }

// InfrastructureProvider returns the CAPI infrastructure provider for the
// lifecycle lane (default "kubevirt").
func InfrastructureProvider() string {
	if v := strings.TrimSpace(viper.GetString(InfrastructureProviderKey)); v != "" {
		return v
	}
	return defaultInfrastructureProvider
}

// Flavor returns the clusterctl flavor (default "zfs-csi").
func Flavor() string {
	if v := strings.TrimSpace(viper.GetString(FlavorKey)); v != "" {
		return v
	}
	return defaultFlavor
}

// DataDiskByID returns the by-id device path for the storage-node ZFS pool
// disk (default "/dev/disk/by-id/virtio-tank0").
func DataDiskByID() string {
	if v := strings.TrimSpace(viper.GetString(DataDiskByIDKey)); v != "" {
		return v
	}
	return defaultDataDiskByID
}

// DataDiskByIDB returns optional second-owner by-id device identity.
func DataDiskByIDB() string { return strings.TrimSpace(viper.GetString(DataDiskByIDBKey)) }

// StorageOwner describes provider-neutral owner identity available before live
// Machine/Node and pool-GUID discovery. Authoritative pool GUIDs remain absent
// by design: the later harness reads them from each created pool.
type StorageOwner struct {
	Name          string
	MachineSuffix string
	NodeSelector  map[string]string
	PoolDeviceID  string
	PoolName      string
	DiskID        string
	DiskDiscovery DiskDiscovery
	NetworkDomain string
	ReachableFrom []string
	NFSHost       string
	NFSPort       int
	NVMeHost      string
	NVMePort      int
	StorageTaint  string
}

// DiskDiscovery binds a provider attachment identity to its final exact by-id path.
type DiskDiscovery struct {
	Provider             string
	AttachmentDeviceName string
}

// ConsumerWorker describes one CAPI worker group and its topology domain.
// NodeNames is required for static infrastructure configs. CAPI configs leave
// it empty and discover nodes through NodeSelector after provisioning.
type ConsumerWorker struct {
	Name                    string
	MachineDeploymentSuffix string
	NodeSelector            map[string]string
	NodeNames               []string `json:"nodeNames"`
	Replicas                int
	NetworkDomain           string
}

type infrastructureConfig struct {
	Spec struct {
		Provider      string `json:"provider"`
		Flavor        string `json:"flavor"`
		StorageOwners []struct {
			Name                    string            `json:"name"`
			MachineDeploymentSuffix string            `json:"machineDeploymentSuffix"`
			NodeSelector            map[string]string `json:"nodeSelector"`
			Pool                    struct {
				Name      string `json:"name"`
				DiskID    string `json:"diskID"`
				DeviceID  string `json:"deviceID"`
				Discovery struct {
					Provider             string `json:"provider"`
					AttachmentDeviceName string `json:"attachmentDeviceName"`
				} `json:"discovery"`
			} `json:"pool"`
			NetworkDomain string `json:"networkDomain"`
			Endpoints     struct {
				NFS  infrastructureEndpoint `json:"nfs"`
				NVMe infrastructureEndpoint `json:"nvme"`
			} `json:"endpoints"`
		} `json:"storageOwners"`
		ConsumerWorkers []ConsumerWorker `json:"consumerWorkers"`
	} `json:"spec"`
}

type infrastructureEndpoint struct {
	IPv4 string `json:"ipv4"`
	IPv6 string `json:"ipv6"`
	Port int    `json:"port"`
}

// StorageOwners returns configured owners or synthesizes the legacy owner.
// Explicit env entries use name,selector,device,domain,nfs-host,nvme-host fields;
// InfrastructureConfig preserves provider attachment discovery separately.
func StorageOwners() ([]StorageOwner, error) {
	if path := strings.TrimSpace(viper.GetString(InfrastructureConfigKey)); path != "" {
		config, err := readInfrastructureConfig(path)
		if err != nil {
			return nil, err
		}
		owners := make([]StorageOwner, 0, len(config.Spec.StorageOwners))
		for _, input := range config.Spec.StorageOwners {
			nfsHost := preferredEndpointHost(input.Endpoints.NFS)
			nvmeHost := preferredEndpointHost(input.Endpoints.NVMe)
			owner := StorageOwner{
				Name:          input.Name,
				MachineSuffix: input.MachineDeploymentSuffix,
				NodeSelector:  input.NodeSelector,
				PoolDeviceID:  input.Pool.DeviceID,
				PoolName:      input.Pool.Name,
				DiskID:        input.Pool.DiskID,
				DiskDiscovery: DiskDiscovery{
					Provider:             input.Pool.Discovery.Provider,
					AttachmentDeviceName: input.Pool.Discovery.AttachmentDeviceName,
				},
				NetworkDomain: input.NetworkDomain,
				ReachableFrom: []string{input.NetworkDomain},
				NFSHost:       nfsHost,
				NFSPort:       input.Endpoints.NFS.Port,
				NVMeHost:      nvmeHost,
				NVMePort:      input.Endpoints.NVMe.Port,
				StorageTaint:  defaultStorageTaintKey + "=" + defaultStorageTaintValue + ":NoSchedule",
			}
			owners = append(owners, owner)
		}
		if err := validateStorageOwners(owners); err != nil {
			return nil, err
		}
		return owners, nil
	}
	value := strings.TrimSpace(viper.GetString(StorageOwnersKey))
	if value == "" {
		owner := newStorageOwner(
			defaultLegacyStorageOwnerName,
			defaultLegacyStorageOwnerName,
			DataDiskByID(),
			"workers",
			"",
			"",
		)
		return []StorageOwner{owner}, nil
	}

	entries := strings.Split(value, ";")
	owners := make([]StorageOwner, 0, len(entries))
	for i, entry := range entries {
		fields := strings.Split(entry, ",")
		if len(fields) != 6 {
			return nil, fmt.Errorf("%s owner %d must have 6 comma-separated fields (name,selector,device,domain,nfs-host,nvme-host)", Env[StorageOwnersKey], i+1)
		}
		for j := range fields {
			fields[j] = strings.TrimSpace(fields[j])
			if fields[j] == "" {
				return nil, fmt.Errorf("%s owner %d field %d must not be empty", Env[StorageOwnersKey], i+1, j+1)
			}
		}
		owners = append(owners, newStorageOwner(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]))
	}
	if err := validateStorageOwners(owners); err != nil {
		return nil, err
	}
	return owners, nil
}

// ConsumerDomains returns configured consumer topology domains. Unset keeps
// the legacy static "workers" domain.
func ConsumerDomains() ([]string, error) {
	if path := strings.TrimSpace(viper.GetString(InfrastructureConfigKey)); path != "" {
		workers, err := ConsumerWorkers()
		if err != nil {
			return nil, err
		}
		domains := make([]string, 0, len(workers))
		for _, worker := range workers {
			domains = append(domains, worker.NetworkDomain)
		}
		return domains, nil
	}
	value := strings.TrimSpace(viper.GetString(ConsumerDomainsKey))
	if value == "" {
		return []string{"workers"}, nil
	}
	domains := strings.Split(value, ",")
	for i := range domains {
		domains[i] = strings.TrimSpace(domains[i])
		if domains[i] == "" {
			return nil, fmt.Errorf("%s contains an empty network domain", Env[ConsumerDomainsKey])
		}
	}
	return domains, nil
}

// ConsumerWorkers returns configured worker groups or legacy worker synthesis.
func ConsumerWorkers() ([]ConsumerWorker, error) {
	if path := strings.TrimSpace(viper.GetString(InfrastructureConfigKey)); path != "" {
		config, err := readInfrastructureConfig(path)
		if err != nil {
			return nil, err
		}
		if len(config.Spec.ConsumerWorkers) == 0 {
			return nil, fmt.Errorf("infrastructure config %q has no consumerWorkers", path)
		}
		workers := config.Spec.ConsumerWorkers
		for i := range workers {
			if len(workers[i].NodeSelector) == 0 {
				workers[i].NodeSelector = map[string]string{"zfs-csi.randomvariable.co.uk/consumer-group": workers[i].Name}
			}
		}
		if err := validateConsumerWorkers(workers); err != nil {
			return nil, err
		}
		return workers, nil
	}
	domains, err := ConsumerDomains()
	if err != nil {
		return nil, err
	}
	workers := make([]ConsumerWorker, 0, len(domains))
	for i, domain := range domains {
		name := "workers"
		suffix := "md-0"
		if len(domains) > 1 {
			name = fmt.Sprintf("workers-%d", i)
			suffix = name
		}
		workers = append(workers, ConsumerWorker{Name: name, MachineDeploymentSuffix: suffix, NodeSelector: map[string]string{"zfs-csi.randomvariable.co.uk/consumer-group": name}, Replicas: 1, NetworkDomain: domain})
	}
	return workers, nil
}

func readInfrastructureConfig(path string) (infrastructureConfig, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return infrastructureConfig{}, fmt.Errorf("read infrastructure config %q: %w", path, err)
	}
	var config infrastructureConfig
	if err := yaml.Unmarshal(body, &config); err != nil {
		return infrastructureConfig{}, fmt.Errorf("parse infrastructure config %q: %w", path, err)
	}
	if config.Spec.Provider != InfrastructureProvider() || config.Spec.Flavor == "" {
		return infrastructureConfig{}, fmt.Errorf("infrastructure config %q provider/flavor does not match selected provider %q", path, InfrastructureProvider())
	}
	return config, nil
}

func validateConsumerWorkers(workers []ConsumerWorker) error {
	seenNames := map[string]struct{}{}
	seenNodes := map[string]string{}
	for _, worker := range workers {
		if worker.Name == "" || worker.NetworkDomain == "" || worker.Replicas < 1 {
			return fmt.Errorf("consumer worker %q must define name, positive replicas, and network domain", worker.Name)
		}
		if _, exists := seenNames[worker.Name]; exists {
			return fmt.Errorf("duplicate consumer worker name %q", worker.Name)
		}
		seenNames[worker.Name] = struct{}{}
		if len(worker.NodeNames) > 0 && len(worker.NodeNames) != worker.Replicas {
			return fmt.Errorf("consumer worker %q explicitly names %d nodes, want exactly %d replicas", worker.Name, len(worker.NodeNames), worker.Replicas)
		}
		for _, nodeName := range worker.NodeNames {
			if !validKubernetesNodeName(nodeName) {
				return fmt.Errorf("consumer worker %q node name %q is not a valid Kubernetes DNS name", worker.Name, nodeName)
			}
			if previous := seenNodes[nodeName]; previous != "" {
				return fmt.Errorf("consumer worker %q reuses explicitly named Node %q from worker %q", worker.Name, nodeName, previous)
			}
			seenNodes[nodeName] = worker.Name
		}
	}
	if InfrastructureProvider() == "static" {
		for _, worker := range workers {
			if len(worker.NodeNames) != worker.Replicas {
				return fmt.Errorf("static consumer worker %q must explicitly name exactly %d nodes", worker.Name, worker.Replicas)
			}
		}
	}
	return nil
}

func validKubernetesNodeName(name string) bool {
	if len(name) == 0 || len(name) > 253 || name[0] == '.' || name[len(name)-1] == '.' {
		return false
	}
	for _, label := range strings.Split(name, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
				return false
			}
		}
	}
	return true
}

func preferredEndpointHost(endpoint infrastructureEndpoint) string {
	if endpoint.IPv4 != "" {
		return endpoint.IPv4
	}
	return endpoint.IPv6
}

func newStorageOwner(name, selector, device, domain, nfsHost, nvmeHost string) StorageOwner {
	return StorageOwner{
		Name:          name,
		MachineSuffix: selector,
		NodeSelector:  map[string]string{defaultStorageOwnerLabelKey: name},
		PoolDeviceID:  device,
		PoolName:      PoolName(),
		DiskID:        strings.TrimPrefix(device, "/dev/disk/by-id/virtio-"),
		NetworkDomain: domain,
		ReachableFrom: []string{domain},
		NFSHost:       nfsHost,
		NFSPort:       defaultNFSChannelPort,
		NVMeHost:      nvmeHost,
		NVMePort:      defaultNVMeChannelPort,
		StorageTaint:  defaultStorageTaintKey + "=" + defaultStorageTaintValue + ":NoSchedule",
	}
}

func validateStorageOwners(owners []StorageOwner) error {
	if len(owners) == 0 {
		return fmt.Errorf("storage owner contract must not be empty")
	}
	seenNames := map[string]struct{}{}
	seenMachines := map[string]struct{}{}
	seenDevices := map[string]struct{}{}
	seenNFSEndpoints := map[string]struct{}{}
	seenNVMeEndpoints := map[string]struct{}{}
	for _, owner := range owners {
		if !validStorageOwnerName(owner.Name) {
			return fmt.Errorf("storage owner %q must be a DNS label for its TLS leaf Secret", owner.Name)
		}
		if owner.Name == "" || owner.MachineSuffix == "" || owner.NetworkDomain == "" {
			return fmt.Errorf("storage owner identity, selector, pool device, and network domain must not be empty")
		}
		if owner.PoolDeviceID == "" && owner.DiskDiscovery.Provider == "" {
			return fmt.Errorf("storage owner %q must define an exact pool device or discovery binding", owner.Name)
		}
		if owner.PoolDeviceID != "" && !strings.HasPrefix(owner.PoolDeviceID, "/dev/disk/by-id/") {
			return fmt.Errorf("storage owner %q pool device %q must use /dev/disk/by-id", owner.Name, owner.PoolDeviceID)
		}
		if owner.PoolDeviceID != "" && (strings.ContainsAny(owner.PoolDeviceID, "*?[]{}") || strings.ContainsAny(owner.PoolDeviceID, " \t\r\n")) {
			return fmt.Errorf("storage owner %q pool device %q must be an exact by-id path without globs or whitespace", owner.Name, owner.PoolDeviceID)
		}
		if strings.HasPrefix(owner.PoolDeviceID, "/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_") && !isExactAWSEBSByID(owner.PoolDeviceID) {
			return fmt.Errorf("storage owner %q AWS pool device %q must include an exact EBS volume ID", owner.Name, owner.PoolDeviceID)
		}
		if err := rejectDuplicate(seenNames, owner.Name, "name"); err != nil {
			return err
		}
		if err := rejectDuplicate(seenMachines, owner.MachineSuffix, "machine selector"); err != nil {
			return err
		}
		if owner.PoolDeviceID != "" {
			if err := rejectDuplicate(seenDevices, owner.PoolDeviceID, "pool device"); err != nil {
				return err
			}
		}
		nfsEndpoint, err := canonicalEndpoint(owner.NFSHost, owner.NFSPort)
		if err != nil {
			return fmt.Errorf("storage owner %q NFS endpoint: %w", owner.Name, err)
		}
		if err := rejectDuplicate(seenNFSEndpoints, nfsEndpoint, "NFS endpoint"); err != nil {
			return err
		}
		nvmeEndpoint, err := canonicalEndpoint(owner.NVMeHost, owner.NVMePort)
		if err != nil {
			return fmt.Errorf("storage owner %q NVMe endpoint: %w", owner.Name, err)
		}
		if err := rejectDuplicate(seenNVMeEndpoints, nvmeEndpoint, "NVMe endpoint"); err != nil {
			return err
		}
		if !slices.Contains(owner.ReachableFrom, owner.NetworkDomain) {
			return fmt.Errorf("storage owner %q network domain %q must appear in reachable domains", owner.Name, owner.NetworkDomain)
		}
	}
	return nil
}

func validStorageOwnerName(name string) bool {
	if len(name) == 0 || len(name) > 63 || name[0] == '-' || name[len(name)-1] == '-' {
		return false
	}
	for _, r := range name {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
			return false
		}
	}
	return true
}

func isExactAWSEBSByID(deviceID string) bool {
	const prefix = "/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_vol"
	if !strings.HasPrefix(deviceID, prefix) || len(deviceID) != len(prefix)+17 {
		return false
	}
	volumeID := strings.TrimPrefix(deviceID, prefix)
	if volumeID == "" {
		return false
	}
	for _, r := range volumeID {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

func canonicalEndpoint(host string, port int) (string, error) {
	if host == "" || strings.TrimSpace(host) != host || strings.ContainsAny(host, " \t\r\n") {
		return "", fmt.Errorf("host must not be empty or contain whitespace")
	}
	if port < 1 || port > 65535 {
		return "", fmt.Errorf("port %d is outside 1-65535", port)
	}
	canonicalHost := strings.ToLower(host)
	if address, err := netip.ParseAddr(host); err == nil {
		canonicalHost = address.String()
	} else if strings.Contains(host, ":") {
		return "", fmt.Errorf("host %q is not a valid IP address or DNS name", host)
	}
	return net.JoinHostPort(canonicalHost, strconv.Itoa(port)), nil
}

func rejectDuplicate(seen map[string]struct{}, value, field string) error {
	if _, exists := seen[value]; exists {
		return fmt.Errorf("duplicate storage owner %s %q", field, value)
	}
	seen[value] = struct{}{}
	return nil
}

// allowedInfrastructureProviders is the allowlist of valid infrastructure
// providers for the lifecycle lane. An unknown E2E_INFRASTRUCTURE_PROVIDER
// (typo, stale flag) would otherwise fall through ensureFabric's default case
// and silently run the wrong lane's fabric setup. Validate rejects it up front.
// kubevirt/aws provision a workload cluster via CAPI clusterctl; static runs
// against a pre-existing cluster reached via E2E_WORKLOAD_KUBECONFIG.
var allowedInfrastructureProviders = map[string]struct{}{
	"kubevirt": {},
	"aws":      {},
	"static":   {},
}

// Validate enforces required knobs for the lifecycle suite. It returns a
// user-actionable error naming the missing flag/env var, mirroring the old
// requireE2EConfig behaviour but for the unified config surface.
func Validate() error {
	var missing []string
	if strings.TrimSpace(viper.GetString(RunIDKey)) == "" {
		missing = append(missing, Env[RunIDKey])
	}
	if _, err := ConfigPath(); err != nil {
		missing = append(missing, Env[ConfigKey])
	}
	if len(missing) > 0 {
		return fmt.Errorf("%s required for CAPI/CAPK lifecycle E2E (set via --flag or env var)", strings.Join(missing, " and "))
	}
	// Reject an unknown infrastructure provider early so a typo (e.g. E2E_INFRASTRUCTURE_PROVIDER=awss)
	// fails fast instead of silently running the wrong lane via ensureFabric's default case.
	p := InfrastructureProvider()
	if _, ok := allowedInfrastructureProviders[p]; !ok {
		return fmt.Errorf("%s=%q is not a supported infrastructure provider; want one of kubevirt, aws, static", Env[InfrastructureProviderKey], p)
	}
	if p == "static" && WorkloadKubeconfigPath() == "" {
		return fmt.Errorf("%s required when %s=static: the static lane runs against a pre-existing workload cluster", Env[WorkloadKubeconfigKey], Env[InfrastructureProviderKey])
	}
	// EncryptionEnabled defaults true, which force-applies dev-mode OpenBao into
	// the shared cluster's openbao namespace. On the static provider that would
	// clobber a real OpenBao, so require an explicit opt-out regardless of entry
	// point (not only via the mage target's env defaults).
	if p == "static" && EncryptionEnabled() {
		return fmt.Errorf("%s=0 required when %s=static: the static lane must not deploy dev-mode OpenBao into a shared cluster", Env[EncryptionEnabledKey], Env[InfrastructureProviderKey])
	}
	if !IsCleanupOnly() && ConformanceTLSOnly() && !TransportTLSEnabled() {
		return fmt.Errorf("%s=1 requires %s=1", Env[ConformanceTLSOnlyKey], Env[TransportTLSEnabledKey])
	}
	owners, err := StorageOwners()
	if err != nil {
		return err
	}
	domains, err := ConsumerDomains()
	if err != nil {
		return err
	}
	if strings.TrimSpace(viper.GetString(StorageOwnersKey)) != "" || strings.TrimSpace(viper.GetString(InfrastructureConfigKey)) != "" {
		for _, owner := range owners {
			if !slices.Contains(domains, owner.NetworkDomain) {
				return fmt.Errorf("storage owner %q network domain %q has no configured consumer reachability", owner.Name, owner.NetworkDomain)
			}
		}
	}
	if RunConformance() && !IsCleanupOnly() {
		keyPath := SSHPrivateKeyPath()
		// The static provider with disruptive specs excluded never SSHes into
		// nodes (kubelet-restart tests are skipped), so the key is optional
		// there. Disruptive runs — and every CAPI-provisioning lane — still
		// require a readable, parseable key. A supplied key is always validated.
		sshKeyOptional := p == "static" && !ConformanceDisruptive()
		if keyPath == "" && !sshKeyOptional {
			return fmt.Errorf("%s required when %s is enabled", Env[SSHPrivateKeyPathKey], Env[RunConformanceKey])
		}
		if keyPath != "" {
			keyData, err := os.ReadFile(keyPath)
			if err != nil {
				return fmt.Errorf("read %s %q: %w", Env[SSHPrivateKeyPathKey], keyPath, err)
			}
			if _, err := ssh.ParsePrivateKey(keyData); err != nil {
				var passphraseMissing *ssh.PassphraseMissingError
				if errors.As(err, &passphraseMissing) {
					return fmt.Errorf("parse %s %q: encrypted private keys are unsupported; provide an unencrypted key because the E2E launcher has no passphrase flow", Env[SSHPrivateKeyPathKey], keyPath)
				}
				return fmt.Errorf("parse %s %q: %w", Env[SSHPrivateKeyPathKey], keyPath, err)
			}
		}
	}
	return nil
}

// ValidateStaticProviderSubstrate rejects explicit configurations that cannot
// be provisioned by the selected static clusterctl flavor. The static
// infrastructure provider bypasses these shape checks: it provisions nothing,
// so the InfrastructureConfig carries an explicit owner-to-node mapping
// (arbitrary owner names/selectors) instead of the rendered
// storage-a/storage-b/workers-a substrate contract.
func ValidateStaticProviderSubstrate() error {
	if InfrastructureProvider() == "static" {
		return nil
	}
	if strings.TrimSpace(viper.GetString(InfrastructureConfigKey)) == "" {
		return nil
	}
	owners, err := StorageOwners()
	if err != nil {
		return err
	}
	workers, err := ConsumerWorkers()
	if err != nil {
		return err
	}
	if len(owners) < 1 || len(owners) > 2 || len(workers) != 1 {
		return fmt.Errorf("static %s flavor %q provisions 1 or 2 configured storage owners and exactly 1 consumer worker group; configuration requests %d owners and %d groups", InfrastructureProvider(), Flavor(), len(owners), len(workers))
	}
	for _, owner := range owners {
		if owner.MachineSuffix != owner.Name || (owner.Name != "storage-a" && owner.Name != "storage-b") {
			return fmt.Errorf("static %s flavor %q does not provision storage owner %q with MachineDeployment suffix %q", InfrastructureProvider(), Flavor(), owner.Name, owner.MachineSuffix)
		}
		if len(owner.NodeSelector) != 1 || owner.NodeSelector[defaultStorageOwnerLabelKey] != owner.Name {
			return fmt.Errorf("static %s flavor %q storage owner %q selector %v does not match rendered substrate", InfrastructureProvider(), Flavor(), owner.Name, owner.NodeSelector)
		}
	}
	worker := workers[0]
	if worker.Name != "workers-a" || worker.MachineDeploymentSuffix != "md-0" || len(worker.NodeSelector) != 1 || worker.NodeSelector["zfs-csi.randomvariable.co.uk/consumer-group"] != "workers-a" {
		return fmt.Errorf("static %s flavor %q consumer group %q with MachineDeployment suffix %q and selector %v does not match rendered substrate", InfrastructureProvider(), Flavor(), worker.Name, worker.MachineDeploymentSuffix, worker.NodeSelector)
	}
	return nil
}

// ChildEnv returns "KEY=VALUE" strings for every E2E knob that has a non-empty
// value, for bridging mage-side viper values into a child `go test` process's
// environment. The child's own Init()+AutomaticEnv then resolves them without
// the caller hard-coding env-var names. Bool knobs are emitted as "1"/"".
func ChildEnv() []string {
	out := make([]string, 0, len(Env))
	for key, envName := range Env {
		var val string
		switch key {
		case CleanupOnlyKey, SkipCleanupKey, RunConformanceKey, ConformanceDryRunKey, TransportTLSEnabledKey, ConformanceTLSOnlyKey, PodCertificateAcceptanceKey, ConformanceDisruptiveKey, SkipSnapshotBundleKey:
			if viper.GetBool(key) {
				val = "1"
			}
		case EncryptionEnabledKey:
			// Default-true bool: emit "0" ONLY when explicitly disabled, so the
			// child (which also defaults true) honours the override. When enabled,
			// omit it — emitting "0" on an accidental empty value would silently
			// disable encryption in the child. Uses the accessor, which treats an
			// empty value as the true default.
			if !EncryptionEnabled() {
				val = "0"
			}
		case ConfigKey:
			p, err := ConfigPath()
			if err != nil {
				continue
			}
			val = p
		case HostKubeconfigKey:
			val = HostKubeconfigPath()
		case StoragePoolKey:
			val = PoolName()
		case PreflightImageKey:
			val = PreflightImageRef()
		case ChartRefKey:
			val = ChartReference()
		default:
			val = strings.TrimSpace(viper.GetString(key))
		}
		if val == "" {
			continue
		}
		out = append(out, envName+"="+val)
	}
	return out
}

// KubernetesVersion returns the workload Kubernetes version for the selected
// lane. An explicit E2E_KUBERNETES_VERSION wins; otherwise it defaults per
// provider (both kubevirt and aws pin v1.36.2). Centralised here so the
// magefile and the Ginkgo specs cannot drift. The AWS lane's custom
// image-builder AMI (mage e2e:imageBuildAWS) bakes v1.36.2, so it no longer
// depends on a CAPA-published AMI's version.
func KubernetesVersion() string {
	if v := strings.TrimSpace(viper.GetString(KubernetesVersionKey)); v != "" {
		return v
	}
	if InfrastructureProvider() == "aws" {
		return defaultAWSKubernetesVersion
	}
	return defaultKubeVirtKubernetesVersion
}

// NFSExportCIDRs returns the client networks the chart sets on NFS classes.
func NFSExportCIDRs() []string {
	if value := strings.TrimSpace(viper.GetString(NFSExportCIDRsKey)); value != "" {
		parts := strings.Split(value, ",")
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		return parts
	}
	if InfrastructureProvider() == "aws" {
		return []string{defaultAWSNFSExportCIDR}
	}
	return []string{defaultKubeVirtNFSExportCIDR}
}

// EncryptionEnabled reports whether the lane deploys OpenBao and installs the
// chart with per-volume ZFS native encryption (E2E_ENCRYPTION). Defaults true;
// set E2E_ENCRYPTION=0 (or false) to skip OpenBao + the encrypted
// StorageClass/testdriver.
//
// An explicitly-empty value (E2E_ENCRYPTION="") is treated as "use default"
// (true), NOT false — mirroring the string knobs. viper.GetBool("") would read
// an empty env as false, which would silently disable encryption if some
// wrapper exported the var empty; only an explicit 0/false disables.
func EncryptionEnabled() bool {
	if v := strings.TrimSpace(viper.GetString(EncryptionEnabledKey)); v == "" {
		return true
	}
	return viper.GetBool(EncryptionEnabledKey)
}

// TransportTLSEnabled reports whether gated transport-TLS qualification paths
// run. Defaults false; an empty environment value also remains false.
func TransportTLSEnabled() bool { return viper.GetBool(TransportTLSEnabledKey) }

// ConformanceTLSOnly reports whether conformance should use only TLS drivers.
func ConformanceTLSOnly() bool { return viper.GetBool(ConformanceTLSOnlyKey) }

// PodCertificateAcceptanceEnabled reports whether the bounded AWS PCR evidence
// suite should run. The caller also enforces AWS and transport-TLS prerequisites.
func PodCertificateAcceptanceEnabled() bool { return viper.GetBool(PodCertificateAcceptanceKey) }

// RunConformance reports whether the opt-in external-storage conformance spec
// should run (E2E_RUN_CONFORMANCE=1). Defaults false — the conformance It skips.
func RunConformance() bool { return viper.GetBool(RunConformanceKey) }

// SSHPrivateKeyPath returns the local workload-node SSH private key path used
// by the conformance skeleton provider.
func SSHPrivateKeyPath() string { return strings.TrimSpace(viper.GetString(SSHPrivateKeyPathKey)) }

// SSHUser returns the login used by conformance for workload nodes and the
// CAPA bastion. Both endpoints use the same image/key contract.
func SSHUser() string {
	if value := strings.TrimSpace(viper.GetString(SSHUserKey)); value != "" {
		return value
	}
	if InfrastructureProvider() == "aws" {
		return "ubuntu"
	}
	return ""
}

// ConformanceFocus returns the ginkgo -focus override for conformance, or "" to
// use the runner default (External.Storage.*co.uk).
func ConformanceFocus() string { return strings.TrimSpace(viper.GetString(ConformanceFocusKey)) }

// ConformanceSkip returns the ginkgo -skip override for conformance, or "" to
// use the runner default (Disruptive|Serial|Slow|Feature).
func ConformanceSkip() string { return strings.TrimSpace(viper.GetString(ConformanceSkipKey)) }

// ConformanceImage returns the conformance image override, or "" to derive it
// from the workload Kubernetes version (registry.k8s.io/conformance:vX).
func ConformanceImage() string { return strings.TrimSpace(viper.GetString(ConformanceImageKey)) }

// ConformanceDryRun reports whether the conformance spec should run ginkgo
// --dry-run (the pre-flight gate: enumerate specs, load testdrivers, touch no
// cluster). Set E2E_CONFORMANCE_DRY_RUN=1.
func ConformanceDryRun() bool { return viper.GetBool(ConformanceDryRunKey) }

// WorkloadKubeconfigPath returns the pre-existing workload cluster kubeconfig
// for the static provider, or "" when unset (CAPI lanes derive it from the
// framework instead).
func WorkloadKubeconfigPath() string {
	return strings.TrimSpace(viper.GetString(WorkloadKubeconfigKey))
}

// NonBlockingTaints returns the E2E_NON_BLOCKING_TAINTS override for the
// conformance --non-blocking-taints list, or "" to keep the in-tree default.
func NonBlockingTaints() string { return strings.TrimSpace(viper.GetString(NonBlockingTaintsKey)) }

// AllowedNotReadyNodes returns the conformance --allowed-not-ready-nodes count.
// Unset defaults per provider: 1 for static (a shared cluster may carry an
// unrelated NotReady node that must not block the suite preflight), 0 otherwise
// (unchanged CAPI-lane behaviour). Invalid values fail loudly.
func AllowedNotReadyNodes() (int, error) {
	value := strings.TrimSpace(viper.GetString(AllowedNotReadyNodesKey))
	if value == "" {
		if InfrastructureProvider() == "static" {
			return 1, nil
		}
		return 0, nil
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s=%q must be a non-negative integer", Env[AllowedNotReadyNodesKey], value)
	}
	return n, nil
}

// ConformanceDisruptive reports whether a static-provider conformance run opts
// into [Disruptive]/[Serial] specs (E2E_CONFORMANCE_DISRUPTIVE=1). Default off.
func ConformanceDisruptive() bool { return viper.GetBool(ConformanceDisruptiveKey) }

// SkipSnapshotBundle reports whether the vendored external-snapshotter bundle
// apply should be skipped when VolumeSnapshot CRDs already exist on the
// cluster. Implied by the static provider (a shared cluster owns its snapshot
// controller); explicit E2E_SKIP_SNAPSHOT_BUNDLE=1 forces it on other lanes.
func SkipSnapshotBundle() bool {
	return viper.GetBool(SkipSnapshotBundleKey) || InfrastructureProvider() == "static"
}

// StorageClassOverrides parses E2E_STORAGE_CLASS_OVERRIDES (comma-separated
// chartKey=name pairs, e.g. "tankNVMe=e2e-tank-nvme,tankNFS=e2e-tank-nfs")
// into a map of chart StorageClass value keys to override names. An empty
// value returns an empty map (no renames — current behaviour).
func StorageClassOverrides() (map[string]string, error) {
	value := strings.TrimSpace(viper.GetString(StorageClassOverridesKey))
	overrides := map[string]string{}
	if value == "" {
		return overrides, nil
	}
	for _, pair := range strings.Split(value, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		key, name, found := strings.Cut(pair, "=")
		key, name = strings.TrimSpace(key), strings.TrimSpace(name)
		if !found || key == "" || name == "" {
			return nil, fmt.Errorf("%s entry %q must be chartKey=name", Env[StorageClassOverridesKey], pair)
		}
		if _, duplicate := overrides[key]; duplicate {
			return nil, fmt.Errorf("%s repeats chart key %q", Env[StorageClassOverridesKey], key)
		}
		overrides[key] = name
	}
	return overrides, nil
}
