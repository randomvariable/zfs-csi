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

//go:build mage

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/magefile/mage/mg"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	yamlv3 "gopkg.in/yaml.v3"
	"sigs.k8s.io/yaml"

	"github.com/randomvariable/mage-common/config"
	magetools "github.com/randomvariable/mage-common/tools"

	"github.com/randomvariable/zfs-csi/test/e2e/e2econfig"
)

func init() {
	pflag.String("packages", "./...", "Go package pattern")
	pflag.String("envtest-version", "1.36.0", "Kubernetes envtest asset version")
	e2econfig.Register(pflag.CommandLine)
	pflag.Parse()
	config.CleanOSArgs()
}

const (
	// licenseHolder + licenseYear feed google/addlicense's built-in apache
	// template. Together with -s (SPDX) they reproduce hack/boilerplate.go.txt
	// and the goheader linter template byte-for-byte; keep all three in sync.
	licenseHolder = "Naadir Jeewa"
	licenseYear   = "2026"

	defaultAWSIdentityKind = "AWSClusterControllerIdentity"
	defaultAWSIdentityName = "default"
)

// licenseSourceDirs is the root addlicense walks. The whole repo is covered so
// every authored file (Go, YAML, Dockerfile, proto, chart) carries the header;
// vendored/generated/transient trees are removed via licenseIgnores.
var licenseSourceDirs = []string{"."}

// licenseIgnores are glob patterns addlicense must NOT touch:
//   - vendored third party: the hack/venv Python environment; upstream manifests
//     under test/e2e/data already carry their own upstream headers so addlicense
//     skips them automatically (it never overwrites an existing header).
//   - tool binaries + gitignored artifact/scratch trees (hack/bin, _artifacts,
//     _rendered, .slim, .cortexkit).
//   - generated output that is re-emitted by `mage generate:*` and would lose a
//     hand-added header on the next regen (zz_generated Go, controller-gen CRDs).
var licenseIgnores = []string{
	"hack/venv/**",
	"hack/bin/**",
	"hack/tools/bin/**",
	"hack/envtest/**",
	"_artifacts/**",
	"test/e2e/_artifacts/**",
	"test/e2e/_rendered/**",
	".cortexkit/**",
	".slim/**",
	".git/**",
	"**/zz_generated.*.go",
	"deploy/crd/**",
	"charts/zfs-csi/templates/crd/**",
	// Vendored upstream manifests — third-party copyright, do NOT stamp ours.
	"test/e2e/data/cni/calico*.yaml",                     // Tigera / Project Calico
	"test/e2e/data/snapshot/external-snapshotter-*.yaml", // kubernetes-csi
}

// licenseArgs assembles the shared addlicense flags (holder, apache+SPDX, pinned
// year) plus every ignore pattern, then the source root(s). checkOnly toggles
// -check (verify, no modify) for the PR-safe gate.
func licenseArgs(checkOnly bool) []string {
	args := []string{}
	if checkOnly {
		args = append(args, "-check")
	}
	args = append(args, "-c", licenseHolder, "-l", "apache", "-s", "-y", licenseYear)
	for _, ig := range licenseIgnores {
		args = append(args, "-ignore", ig)
	}

	return append(args, licenseSourceDirs...)
}

type Generate mg.Namespace

func (Generate) Deepcopy(ctx context.Context) error {
	_, err := magetools.Run(ctx, "controller-gen", []string{"object:headerFile=hack/boilerplate.go.txt", "paths=./api/..."})
	return wrap("controller-gen object", err)
}

func (Generate) CRD(ctx context.Context) error {
	_, err := magetools.Run(ctx, "controller-gen", []string{"crd:crdVersions=v1,generateEmbeddedObjectMeta=true", "paths=./api/...", "output:crd:artifacts:config=deploy/crd"})
	return wrap("controller-gen crd", err)
}

// Proto regenerates the StagePlugin gRPC stubs from proto/stage/*.proto using
// protoc + protoc-gen-go/protoc-gen-go-grpc. Output lands in
// internal/stagepb/<proto dir>/ with the repo boilerplate header prepended.
// Requires: protoc on PATH (libprotoc) and protoc-gen-go/protoc-gen-go-grpc
// in PATH (go install google.golang.org/protobuf/cmd/... and .../grpc/cmd/...).
// protoc is a system binary (not a mage-managed tool), so we invoke it directly.
func (Generate) Proto(ctx context.Context) error {
	if err := os.MkdirAll(filepath.Join("internal", "stagepb"), 0o750); err != nil {
		return wrap("mkdir stagepb", err)
	}

	cmd := exec.CommandContext(ctx, "protoc",
		"--proto_path=proto",
		"--go_out=internal/stagepb", "--go_opt=paths=source_relative",
		"--go-grpc_out=internal/stagepb", "--go-grpc_opt=paths=source_relative",
		"proto/stage/stage.proto",
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("protoc stage: %w: %s", err, out)
	}

	boilerplate, err := os.ReadFile(filepath.Join("hack", "boilerplate.go.txt"))
	if err != nil {
		return wrap("read boilerplate", err)
	}

	err = filepath.WalkDir(filepath.Join("internal", "stagepb"), func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".pb.go") {
			return err
		}

		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		// Skip if already prefixed (idempotent regen).
		if bytes.HasPrefix(body, boilerplate) {
			return nil
		}

		return os.WriteFile(path, append(boilerplate, body...), 0o600)
	})

	return wrap("prepend boilerplate", err)
}

// License ensures every Go source file carries the Apache-2.0 + SPDX header via
// google/addlicense. addlicense's built-in apache template with -s (SPDX) and
// the pinned copyright holder/year reproduces hack/boilerplate.go.txt EXACTLY,
// which is also the goheader linter template — so `mage generate:license` and
// the goheader lint check stay in lockstep with no separate template file to
// maintain. addlicense adds a header only to files that lack one (it skips
// already-licensed files); it does NOT rewrite a malformed/partial header, so a
// file with a truncated header must have it removed before this can fix it.
func (Generate) License(ctx context.Context) error {
	_, err := magetools.Run(ctx, "addlicense", licenseArgs(false))
	return wrap("addlicense", err)
}

// LicenseCheck verifies (without modifying) that every authored file carries the
// header. Suitable for a PR-safe CI gate. Non-zero exit if any file is missing.
func (Generate) LicenseCheck(ctx context.Context) error {
	_, err := magetools.Run(ctx, "addlicense", licenseArgs(true))
	return wrap("addlicense --check", err)
}

func (Generate) All(ctx context.Context) error {
	mg.SerialCtxDeps(ctx, Generate.Deepcopy, Generate.CRD, Generate.Proto, Generate.License)
	return nil
}

// lintToolsDir mirrors .tools.yaml spec.toolsDir; used only as the fallback for
// EffectiveLinterPath, which prefers the custom .custom-gcl.yml binary.
const lintToolsDir = "hack/bin"

// lintBuildTags are the build tags the linter must analyze under. The libzfs
// tag pulls in the cgo binding (internal/libzfs) — WITHOUT it the linter both
// skips that package AND, because the package then fails to typecheck, emits
// spurious cross-package diagnostics (a false SA4006 on an obviously-used var).
const lintBuildTags = "libzfs,e2e"

// nixDevelop runs a command inside the project's `nix develop` shell (flake.nix),
// which provides the libzfs headers, pkg-config, and CGO_ENABLED=1 required to
// compile/lint the `-tags=libzfs` cgo path. Any target that touches the libzfs
// binding MUST run through this. If already inside a nix shell (IN_NIX_SHELL) or
// when explicitly disabled (ZFS_CSI_NO_NIX=1, e.g. CI images that ship
// libzfs-dev natively), the command runs directly.
func nixDevelop(ctx context.Context, args []string, opts ...magetools.RunOption) (*magetools.RunResult, error) {
	if os.Getenv("IN_NIX_SHELL") != "" || os.Getenv("ZFS_CSI_NO_NIX") == "1" {
		return magetools.RunBinary(ctx, args[0], args[1:], opts...)
	}

	full := append([]string{
		"develop",
		"--extra-experimental-features", "nix-command flakes",
		"-c",
	}, args...)

	return magetools.RunBinary(ctx, "nix", full, opts...)
}

type Lint mg.Namespace

func (Lint) Run(ctx context.Context) error {
	if err := config.Init(); err != nil {
		return fmt.Errorf("init config: %w", err)
	}
	// Building the custom golangci-lint (kube-api-linter plugin) does not need
	// libzfs, so it runs outside the nix shell.
	if err := magetools.Ensure(ctx, "golangci-lint"); err != nil {
		return wrap("ensure golangci-lint", err)
	}
	linter := magetools.EffectiveLinterPath(lintToolsDir)
	// Lint WITH the libzfs tag inside nix develop so the cgo binding typechecks.
	_, err := nixDevelop(ctx, []string{linter, "run", "--build-tags=" + lintBuildTags, viper.GetString("packages")})
	return wrap("golangci-lint", err)
}

func (Lint) Fix(ctx context.Context) error {
	if err := config.Init(); err != nil {
		return fmt.Errorf("init config: %w", err)
	}
	if err := magetools.Ensure(ctx, "golangci-lint"); err != nil {
		return wrap("ensure golangci-lint", err)
	}
	linter := magetools.EffectiveLinterPath(lintToolsDir)
	_, err := nixDevelop(ctx, []string{linter, "run", "--fix", "--build-tags=" + lintBuildTags, viper.GetString("packages")})
	return wrap("golangci-lint --fix", err)
}

type Test mg.Namespace

func (Test) Unit(ctx context.Context) error {
	_, err := magetools.RunBinary(ctx, "go", []string{"test", "-count=1", "-timeout", "120s", "./..."})
	return wrap("unit tests", err)
}

func (Test) Race(ctx context.Context) error {
	_, err := magetools.RunBinary(ctx, "go", []string{"test", "-race", "-count=1", "-timeout", "120s", viper.GetString("packages")})
	return wrap("race tests", err)
}

func (Test) Sanity(ctx context.Context) error {
	_, err := magetools.RunBinary(ctx, "go", []string{"test", "-tags=sanity", "-count=1", "-timeout", "60s", "./test/sanity/..."})
	return wrap("csi sanity", err)
}

func (Test) Envtest(ctx context.Context) error {
	if err := config.Init(); err != nil {
		return fmt.Errorf("init config: %w", err)
	}
	version := viper.GetString("envtest-version")
	out, err := magetools.Run(ctx, "setup-envtest", []string{"use", version, "--bin-dir", "hack/envtest", "-p", "path"}, magetools.WithStdout())
	if err != nil {
		return fmt.Errorf("setup-envtest: %w", err)
	}
	assets := strings.TrimSpace(string(out.Stdout))
	if assets == "" {
		assets = filepath.Join("hack", "envtest", "k8s", version)
	}
	if !filepath.IsAbs(assets) {
		root, err := os.Getwd()
		if err != nil {
			return fmt.Errorf("resolve repository root for envtest assets: %w", err)
		}
		assets = filepath.Join(root, assets)
	}
	abs, err := filepath.Abs(assets)
	if err != nil {
		return fmt.Errorf("resolve envtest assets path: %w", err)
	}
	if err := os.Setenv("KUBEBUILDER_ASSETS", abs); err != nil {
		return fmt.Errorf("set KUBEBUILDER_ASSETS: %w", err)
	}
	_, err = magetools.RunBinary(ctx, "go", []string{"test", "-tags=envtest", "-count=1", "-timeout", "300s", "./internal/agent/...", "./internal/driver/..."})
	return wrap("envtest", err)
}

func (Test) All(ctx context.Context) error { mg.SerialCtxDeps(ctx, Test.Unit, Test.Sanity); return nil }

type Verify mg.Namespace

// Cgo compiles the real libzfs cgo binding (-tags=libzfs) inside the nix develop
// shell so the CGO path is build-verified — the default CGO_ENABLED=0 build uses
// the fake backend and never exercises internal/libzfs. Catches C-level breakage
// (wrong signatures, header drift) that pure-Go build/test cannot.
func (Verify) Cgo(ctx context.Context) error {
	_, err := nixDevelop(ctx, []string{"go", "build", "-tags=" + lintBuildTags, "./..."})
	return wrap("cgo build", err)
}

func (Verify) All(ctx context.Context) error {
	mg.SerialCtxDeps(ctx, Generate.All, Lint.Run, Verify.Cgo, Test.All)
	return nil
}

// Docs builds and serves the MkDocs (Material) documentation site under docs/.
// mkdocs is a mage-managed tool installed as a uvx shim (see .tools.yaml), so no
// system Python/pip is required — magetools.Run ensures the shim before invoking
// it. The published site (GitHub Pages) is built by .github/workflows/docs.yml,
// which runs `mkdocs build --strict`; these targets are the local mirror.
type Docs mg.Namespace

// Build renders the static site into site/ with --strict, so any broken internal
// link, missing nav entry, or non-excluded orphan page fails the build. This is
// the same gate CI runs; treat a non-zero exit as a docs defect.
func (Docs) Build(ctx context.Context) error {
	_, err := magetools.Run(ctx, "mkdocs", []string{"build", "--strict"}, magetools.WithStdout())
	return wrap("mkdocs build --strict", err)
}

// Serve runs the live-reload dev server on http://127.0.0.1:8000 for authoring.
// It blocks until interrupted; it is not used in CI.
func (Docs) Serve(ctx context.Context) error {
	_, err := magetools.Run(ctx, "mkdocs", []string{"serve"}, magetools.WithStdout())
	return wrap("mkdocs serve", err)
}

// Deploy publishes the built site to the gh-pages branch via mkdocs gh-deploy.
// Prefer the CI workflow (.github/workflows/docs.yml) for canonical publishing;
// this target is for a manual one-off deploy from a maintainer workstation.
func (Docs) Deploy(ctx context.Context) error {
	_, err := magetools.Run(ctx, "mkdocs", []string{"gh-deploy", "--force"}, magetools.WithStdout())
	return wrap("mkdocs gh-deploy", err)
}

type E2e mg.Namespace

var Aliases = map[string]interface{}{
	"e2e:test:up:down": TestUpDown,
}

func TestUpDown(ctx context.Context) error {
	return (E2e{}).TestUpDown(ctx)
}

type e2eTopologyContract struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Spec struct {
		Substrate struct {
			Target string `json:"target"`
		} `json:"substrate"`
		Fabric struct {
			Lane    string `json:"lane"`
			Network string `json:"network"`
		} `json:"fabric"`
		Images struct {
			Root struct {
				GoldenDataSource         string `json:"goldenDataSource"`
				PerRunClones             bool   `json:"perRunClones"`
				CloneCapableStorageClass string `json:"cloneCapableStorageClass"`
			} `json:"root"`
			ContainerDisk struct {
				BootOnly bool `json:"bootOnly"`
			} `json:"containerDisk"`
		} `json:"images"`
		CAPI struct {
			Lifecycle           string   `json:"lifecycle"`
			Readiness           []string `json:"readiness"`
			KubeconfigRetrieval string   `json:"kubeconfigRetrieval"`
		} `json:"capi"`
		Teardown struct {
			OwnershipLabels map[string]string `json:"ownershipLabels"`
		} `json:"teardown"`
		Nodes []struct {
			Name        string `json:"name"`
			Hostname    string `json:"hostname"`
			JoinRole    string `json:"joinRole"`
			Arch        string `json:"arch"`
			MachineType string `json:"machineType"`
			Resources   struct {
				VCPU      int `json:"vcpu"`
				MemoryMiB int `json:"memoryMiB"`
			} `json:"resources"`
			Labels     map[string]string `json:"labels"`
			Taints     []e2eTaint        `json:"taints"`
			Interfaces struct {
				Management struct {
					Address string `json:"address"`
					MAC     string `json:"mac"`
				} `json:"management"`
				Fabric struct {
					Address string `json:"address"`
					MAC     string `json:"mac"`
					MTU     int    `json:"mtu"`
				} `json:"fabric"`
			} `json:"interfaces"`
			Disks struct {
				Root map[string]any   `json:"root"`
				Data []map[string]any `json:"data"`
			} `json:"disks"`
			Placement struct {
				Lane string `json:"lane"`
			} `json:"placement"`
			Readiness []string `json:"readiness"`
		} `json:"nodes"`
	} `json:"spec"`
}

type e2eTemplateData struct {
	NamePrefix   string
	StateDir     string
	MgmtBridge   string
	FabricBridge string
	Role         string
	Arch         string
	VCPU         int
	MemoryMiB    int
	MgmtMAC      string
	FabricMAC    string
	ExtraDisks   string
}

type infrastructureConfig struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Spec struct {
		Provider        string                       `json:"provider"`
		Flavor          string                       `json:"flavor"`
		AddressFamilies []string                     `json:"addressFamilies"`
		StorageOwners   []infrastructureStorageOwner `json:"storageOwners"`
		ConsumerWorkers []struct {
			Name                    string `json:"name"`
			MachineDeploymentSuffix string `json:"machineDeploymentSuffix"`
			Replicas                int    `json:"replicas"`
			NetworkDomain           string `json:"networkDomain"`
		} `json:"consumerWorkers"`
	} `json:"spec"`
}

type infrastructureStorageOwner struct {
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
}

type infrastructureEndpoint struct {
	IPv4 string `json:"ipv4"`
	IPv6 string `json:"ipv6"`
	Port int    `json:"port"`
}

var (
	canonicalEBSVolumeIDPattern = regexp.MustCompile(`^vol-[0-9a-f]{17}$`)
	exactAWSEBSByIDPattern      = regexp.MustCompile(`^/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_vol[0-9a-f]{17}$`)
)

type e2eTaint struct {
	Key    string `json:"key"`
	Value  string `json:"value"`
	Effect string `json:"effect"`
}

type e2ePackerTemplateData struct {
	BuildName            string `json:"build_name"`
	KubernetesVersion    string `json:"kubernetes_semver"`
	KubernetesSeries     string `json:"kubernetes_series"`
	KubernetesDebVersion string `json:"kubernetes_deb_version"`
	KubernetesRPMVersion string `json:"kubernetes_rpm_version"`
	KubeVirt             string `json:"kubevirt"`
	DiskSizeMiB          string `json:"disk_size"`
	Format               string `json:"format"`
	ArtifactName         string `json:"artifact_name"`
	NodeCustomRoles      string `json:"node_custom_roles_post"`
	// FirstbootCustomRoles runs before the firstboot `sudo reboot now`, so the
	// SSH-reboot sever role is active for that reboot (a node_custom_roles_post
	// role would be too late -- node.yml only runs after the firstboot reboot).
	FirstbootCustomRoles string `json:"firstboot_custom_roles_post"`
	// ISOURL/ISOChecksum override image-builder's pinned Ubuntu ISO. Because
	// our rendered var-file is passed last, these win over the checkout's
	// qemu-ubuntu-2404.json, which pins a point release that Ubuntu removes
	// once it is superseded (the pinned .2 ISO 404s after .3/.4 ship).
	ISOURL      string `json:"iso_url"`
	ISOChecksum string `json:"iso_checksum"`
	// CPUs/Memory override image-builder's default 1 vCPU / 2048 MiB build VM,
	// which makes the QEMU build unnecessarily slow on capable hosts.
	CPUs     string `json:"cpus"`
	MemoryMB string `json:"memory"`
	// GossEntryFile points goss at a no-op gossfile we stage into the checkout,
	// disabling image-builder's brittle release-gate goss suite for our
	// once-per-Kubernetes-minor E2E golden image (see goss/zfs-csi-noop.yaml).
	GossEntryFile string `json:"goss_entry_file"`
}

// Check is the PR-safe static E2E lane. It verifies the topology contract,
// Tekton YAML, and rendered image-builder vars without requiring Packer, an
// image-builder checkout, or creating libvirt/KubeVirt resources. Set
// IMAGE_BUILDER_CAPI_DIR to additionally verify the upstream checkout shape.
func (E2e) Check(ctx context.Context) error {
	root := filepath.Join("test", "e2e")
	if err := validateTopologyContract(filepath.Join(root, "topology.yaml")); err != nil {
		return err
	}
	if err := validateInfrastructureConfigs(filepath.Join(root, "data")); err != nil {
		return err
	}
	if err := validateKubeVirtLane(root); err != nil {
		return err
	}
	if err := validateTektonYAML(ctx); err != nil {
		return err
	}
	if err := renderImageBuilderVars(filepath.Join("test", "e2e", "_rendered", "packer")); err != nil {
		return err
	}
	if err := validateRenderedPackerVars(renderedPackerVarsPath(filepath.Join("test", "e2e", "_rendered", "packer"))); err != nil {
		return err
	}
	// AWS-AMI packer var-file static validation (mirrors the QEMU check): render
	// + assert k8s pins, single-region ami_regions, and absence of QEMU-only keys.
	awsRenderDir := filepath.Join("test", "e2e", "_rendered", "packer-aws")
	if err := renderAWSImageBuilderVars(awsRenderDir); err != nil {
		return err
	}
	if err := validateAWSRenderedPackerVars(renderedPackerVarsPath(awsRenderDir)); err != nil {
		return err
	}
	// Non-mutating static validation of the CAPA/AWS lane (no cloud, no
	// clusterctl exec): the flavor + e2e-config parse, the storage MD /
	// AWSMachineTemplate contract holds, and the pool disk is /dev/xvdb.
	if err := validateAWSLane(); err != nil {
		return err
	}
	if imageBuilderDir := os.Getenv("IMAGE_BUILDER_CAPI_DIR"); imageBuilderDir != "" {
		if err := validateImageBuilderCAPIDir(imageBuilderDir); err != nil {
			return err
		}
	}
	if os.Getenv("E2E_LIBVIRT_REFERENCE") == "1" {
		return checkLibvirtReference(ctx)
	}
	return nil
}

// CheckMutating is the explicit CI lane for the full image-builder validation.
// It clones a mage-managed image-builder checkout (or uses IMAGE_BUILDER_CAPI_DIR
// if set) and runs the upstream QEMU validate target.
func (E2e) CheckMutating(ctx context.Context) error {
	return (E2e{}).ImageFactoryCheck(ctx)
}

// ImageBuild runs the upstream image-builder KubeVirt QEMU build target. It is
// explicitly gated because it writes into IMAGE_BUILDER_CAPI_DIR and runs Packer.
func (E2e) ImageBuild(ctx context.Context) error {
	packerPath, err := ensurePacker(ctx)
	if err != nil {
		return err
	}
	imageBuilderDir, err := ensureImageBuilderCheckout(ctx)
	if err != nil {
		return err
	}
	if err := renderImageBuilderVars(filepath.Join("test", "e2e", "_rendered", "packer")); err != nil {
		return err
	}
	if err := stageImageBuilderCustomization(imageBuilderDir); err != nil {
		return err
	}
	varsPath := filepath.Join(imageBuilderDir, "packer", "qemu", "zfs-csi-e2e.pkrvars.json")
	if err := validateRenderedPackerVars(varsPath); err != nil {
		return err
	}
	// Opportunistically clear the SLIRP post-reboot half-open-socket stall that
	// hangs Packer for up to its 2h ssh_timeout (see
	// docs/e2e-golden-image-build.md). The firstboot ssh-sever role is the
	// in-guest fix; this host-side watchdog is belt-and-braces for reboots that
	// still slip through (e.g. an image built before the sever role landed).
	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	go imageBuilderRebootWatchdog(watchCtx)

	_, err = magetools.RunBinary(ctx, "make", []string{"-C", imageBuilderDir, "build-kubevirt-qemu-ubuntu-2404"}, imageBuilderPackerEnv(packerPath, varsPath)...)
	return wrap("image-builder build-kubevirt-qemu-ubuntu-2404", err)
}

// ImageBuildAWS runs the upstream image-builder AWS-AMI build target for Ubuntu
// 24.04 / Kubernetes v1.36.2, baking the zfs_csi_e2e node role into the AMI. It
// is explicitly gated (writes into the image-builder checkout, runs Packer, and
// creates a real EC2 build instance + AMI, so it needs AWS credentials with
// ec2:RunInstances/CreateImage/CopyImage/CreateTags etc). AWS_REGION selects the
// build+copy region (must match e2e-config-aws.yaml's AWS_REGION so the AWS lane
// can consume the AMI). On success it prints and returns the built AMI id.
func (E2e) ImageBuildAWS(ctx context.Context) error {
	packerPath, err := ensurePacker(ctx)
	if err != nil {
		return err
	}
	imageBuilderDir, err := ensureImageBuilderCheckout(ctx)
	if err != nil {
		return err
	}
	renderDir := filepath.Join("test", "e2e", "_rendered", "packer-aws")
	if err := renderAWSImageBuilderVars(renderDir); err != nil {
		return err
	}
	if err := stageAWSImageBuilderCustomization(imageBuilderDir); err != nil {
		return err
	}
	varsPath := filepath.Join(imageBuilderDir, "packer", "ami", "zfs-csi-e2e.pkrvars.json")
	if err := validateAWSRenderedPackerVars(varsPath); err != nil {
		return err
	}
	// deps-ami installs ansible + inits the ansible/goss packer plugins from
	// config.pkr.hcl. It does NOT install the amazon plugin: the AMI build uses
	// the legacy JSON packer/ami/packer.json (amazon-ebs builder), and legacy
	// JSON cannot declare required_plugins, so `packer init config.pkr.hcl`
	// (ansible+goss only) never pulls it. Packer 1.15 bundles no builders, so we
	// install the amazon plugin explicitly before the build or it fails with
	// "builder amazon-ebs is unknown by Packer".
	if _, err := magetools.RunBinary(ctx, "make", []string{"-C", imageBuilderDir, "deps-ami"}, imageBuilderPackerEnv(packerPath, varsPath)...); err != nil {
		return fmt.Errorf("image-builder deps-ami: %w", err)
	}
	if _, err := magetools.RunBinary(ctx, packerPath, []string{"plugins", "install", "github.com/hashicorp/amazon"}); err != nil {
		return fmt.Errorf("install packer amazon plugin: %w", err)
	}
	if _, err := magetools.RunBinary(ctx, "make", []string{"-C", imageBuilderDir, "build-ami-ubuntu-2404"}, imageBuilderPackerEnv(packerPath, varsPath)...); err != nil {
		return fmt.Errorf("image-builder build-ami-ubuntu-2404: %w", err)
	}
	amiID, err := parseAMIIDFromManifest(awsAMIManifestPath(imageBuilderDir))
	if err != nil {
		return err
	}
	// artifact_id is "region:ami-id"; surface the bare ami-id for AWS_AMI_ID.
	bareAMI := amiID
	if _, id, ok := strings.Cut(amiID, ":"); ok {
		bareAMI = id
	}
	fmt.Printf("\nBuilt AWS AMI (region:ami-id): %s\nExport it for the AWS e2e lane, e.g.:\n  export AWS_AMI_ID=%s\n", amiID, bareAMI)
	return nil
}

// imageBuilderRebootWatchdog clears the SLIRP post-reboot half-open-socket
// stall (see docs/e2e-golden-image-build.md) as a fallback. It is OPT-IN
// (ZFS_CSI_WATCHDOG=1) because the firstboot zfs_csi_e2e_ssh role is the primary
// fix -- images built with that role reboot cleanly and never need this. It
// exists for rebuilding an older image that lacks the role.
//
// It fires ONLY when Packer's SSH connection to the guest has been idle in both
// directions past the threshold AND no ansible-playbook process is running. The
// second condition is the key discriminator: the reboot stall happens during
// Packer's `sudo reboot now` SHELL provisioner (no ansible running), whereas a
// legitimately quiet phase (a slow node.yml / sysprep task like `find`) always
// has ansible-playbook alive -- so this never severs a live ansible connection.
// When it fires it issues `sudo ss -K` on the idle packer-owned connection,
// forcing EOF so Packer reconnects. Best-effort: needs passwordless sudo, only
// targets packer connections, and any failure just backs off.
// Tune the idle threshold (seconds) with ZFS_CSI_WATCHDOG_IDLE_SECONDS (300).
func imageBuilderRebootWatchdog(ctx context.Context) {
	if os.Getenv("ZFS_CSI_WATCHDOG") != "1" {
		return
	}
	if err := exec.Command("sudo", "-n", "true").Run(); err != nil {
		fmt.Fprintln(os.Stderr, "[watchdog] passwordless sudo unavailable; SLIRP ss -K auto-clear disabled")
		return
	}
	idle := time.Duration(envDefaultInt("ZFS_CSI_WATCHDOG_IDLE_SECONDS", 300)) * time.Second
	fmt.Fprintf(os.Stderr, "[watchdog] armed (opt-in); will ss -K a packer SSH connection idle >%s while no ansible-playbook runs\n", idle)

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		pid := qemuGuestPID()
		if pid == 0 {
			continue
		}
		// Only a shell provisioner (the reboot) has no ansible-playbook; a slow
		// ansible task keeps it alive, so we never sever a live task connection.
		if ansiblePlaybookRunning() {
			continue
		}
		port := qemuSSHForwardPort(pid)
		if port == "" {
			continue
		}
		if cleared := severStalledPackerConns(port, idle); cleared > 0 {
			fmt.Fprintf(os.Stderr, "[watchdog] no ansible running and packer SSH idle >%s; ss -K cleared %d connection(s) on port %s\n", idle, cleared, port)
		}
	}
}

// ImageFactoryCheck verifies the full upstream image-builder lane. It uses the
// mage-common managed Packer binary and validates the Ubuntu 24.04 QEMU target,
// but still does not run packer build or create KubeVirt resources.
func (E2e) ImageFactoryCheck(ctx context.Context) error {
	if err := validateTopologyContract(filepath.Join("test", "e2e", "topology.yaml")); err != nil {
		return err
	}
	packerPath, err := ensurePacker(ctx)
	if err != nil {
		return err
	}
	imageBuilderDir, err := ensureImageBuilderCheckout(ctx)
	if err != nil {
		return err
	}
	if err := renderImageBuilderVars(filepath.Join("test", "e2e", "_rendered", "packer")); err != nil {
		return err
	}
	if err := stageImageBuilderCustomization(imageBuilderDir); err != nil {
		return err
	}
	varsPath := filepath.Join(imageBuilderDir, "packer", "qemu", "zfs-csi-e2e.pkrvars.json")
	if err := validateRenderedPackerVars(varsPath); err != nil {
		return err
	}
	if _, err := magetools.RunBinary(ctx, "make", []string{"-C", imageBuilderDir, "validate-kubevirt-qemu-ubuntu-2404"}, imageBuilderPackerEnv(packerPath, varsPath)...); err != nil {
		return fmt.Errorf("image-builder validate-kubevirt-qemu-ubuntu-2404: %w", err)
	}
	return nil
}

// LibvirtReference verifies optional local-dev libvirt reference prerequisites.
// It does not define networks, create disks, start guests, or touch KubeVirt.
func (E2e) LibvirtReference(ctx context.Context) error {
	return checkLibvirtReference(ctx)
}

func ensurePacker(ctx context.Context) (string, error) {
	_, err := magetools.Run(ctx, "packer", []string{"version"})
	if err != nil {
		return "", fmt.Errorf("packer tool: %w", err)
	}
	packerPath, err := filepath.Abs(filepath.Join("hack", "bin", runtime.GOOS, runtime.GOARCH, "packer"))
	if err != nil {
		return "", fmt.Errorf("resolve managed packer path: %w", err)
	}
	return packerPath, nil
}

func imageBuilderPackerEnv(packerPath, varsPath string) []magetools.RunOption {
	return []magetools.RunOption{magetools.WithEnv(
		"PACKER_BIN="+packerPath,
		"PACKER_VAR_FILES="+varsPath,
		"VAR_FILES="+varsPath,
	)}
}

func checkLibvirtReference(ctx context.Context) error {
	for _, binary := range []string{"cloud-localds", "qemu-img", "virsh"} {
		if _, err := exec.LookPath(binary); err != nil {
			return fmt.Errorf("missing %s on PATH: %w", binary, err)
		}
	}
	if _, err := os.Stat("/dev/kvm"); err != nil {
		return fmt.Errorf("/dev/kvm unavailable: %w", err)
	}
	_, err := magetools.RunBinary(ctx, "virsh", []string{"version"})
	return wrap("virsh version", err)
}

// Render writes libvirt XML from templates into test/e2e/_rendered. It is safe
// to run on developer machines because it does not call virsh define/create.
func (E2e) Render(ctx context.Context) error {
	_ = ctx
	outDir := filepath.Join("test", "e2e", "_rendered")
	packerOutDir := filepath.Join(outDir, "packer")
	stateDir, err := filepath.Abs(filepath.Join("test", "e2e", "state"))
	if err != nil {
		return fmt.Errorf("resolve e2e state dir: %w", err)
	}
	if err := os.MkdirAll(outDir, 0o750); err != nil {
		return wrap("mkdir rendered e2e output", err)
	}
	if err := os.MkdirAll(packerOutDir, 0o750); err != nil {
		return wrap("mkdir rendered packer output", err)
	}
	if err := removeRenderedPackerVars(packerOutDir); err != nil {
		return err
	}

	base := e2eTemplateData{NamePrefix: "zfs-csi-e2e", StateDir: stateDir, MgmtBridge: "zcsi-mgmt0", FabricBridge: "zcsi-fab0"}
	if err := renderTemplate(filepath.Join("test", "e2e", "libvirt", "mgmt0-network.xml.tmpl"), filepath.Join(outDir, "mgmt0-network.xml"), base); err != nil {
		return err
	}
	if err := renderTemplate(filepath.Join("test", "e2e", "libvirt", "fabric0-network.xml.tmpl"), filepath.Join(outDir, "fabric0-network.xml"), base); err != nil {
		return err
	}
	roles, err := loadTopologyTemplateData(filepath.Join("test", "e2e", "topology.yaml"))
	if err != nil {
		return err
	}
	for _, role := range roles {
		role.NamePrefix = base.NamePrefix
		role.StateDir = stateDir
		role.MgmtBridge = base.MgmtBridge
		role.FabricBridge = base.FabricBridge
		if role.Role == "storage0" {
			role.ExtraDisks = storageExtraDisks(stateDir)
		}
		if err := renderTemplate(filepath.Join("test", "e2e", "libvirt", "domain.xml.tmpl"), filepath.Join(outDir, role.Role+"-domain.xml"), role); err != nil {
			return err
		}
	}
	if err := renderImageBuilderVars(packerOutDir); err != nil {
		return err
	}
	return nil
}

func (E2e) Up(ctx context.Context) error {
	if err := os.Setenv("E2E_SKIP_CLEANUP", "1"); err != nil {
		return fmt.Errorf("set E2E_SKIP_CLEANUP: %w", err)
	}
	return runE2ETest(ctx, "-ginkgo.v", "-test.v", "-ginkgo.focus=CAPI/CAPK lifecycle")
}

// setAWSLaneEnv sets the four env vars that select the CAPA/AWS lifecycle lane
// (provider, flavor, config path, storage-disk by-id). Both Aws and AwsDown set
// these because teardown still loads the AWS e2e-config to build the clusterctl
// repository. Kept in one place so the two targets cannot drift.
func setAWSLaneEnv() error {
	env := [][2]string{
		{"E2E_INFRASTRUCTURE_PROVIDER", "aws"},
		{"E2E_FLAVOR", "zfs-csi-aws"},
		{"E2E_CONFIG", filepath.Join("test", "e2e", "e2e-config-aws.yaml")},
		{"E2E_INFRASTRUCTURE_CONFIG", filepath.Join("test", "e2e", "data", "infrastructure-aws", "two-owner.yaml")},
		{"E2E_DATA_DISK_BY_ID", "/dev/xvdb"},
	}
	for _, kv := range env {
		if err := os.Setenv(kv[0], kv[1]); err != nil {
			return fmt.Errorf("set %s: %w", kv[0], err)
		}
	}
	defaults := [][2]string{
		{"AWS_IDENTITY_KIND", defaultAWSIdentityKind},
		{"AWS_IDENTITY_NAME", defaultAWSIdentityName},
	}
	for _, kv := range defaults {
		if strings.TrimSpace(os.Getenv(kv[0])) == "" {
			if err := os.Setenv(kv[0], kv[1]); err != nil {
				return fmt.Errorf("set %s: %w", kv[0], err)
			}
		}
	}
	return nil
}

// Aws provisions (or reuses) the CAPA/AWS EC2 workload cluster and runs the
// lifecycle + smoke suite, leaving the cluster standing on exit
// (E2E_SKIP_CLEANUP=1). It is the dev-loop target: because the run ID is pinned
// (the state file, or an explicit E2E_RUN_ID), repeated `mage e2e:aws` runs
// target the same cluster name -- clusterctl re-applies via SSA and the ready
// waits pass instantly, so you iterate on the driver against one long-lived
// cluster. Run `mage e2e:awsDown` to destroy it when finished.
func (E2e) Aws(ctx context.Context) error {
	if err := validateAWSLane(); err != nil {
		return err
	}
	if err := setAWSLaneEnv(); err != nil {
		return err
	}
	if err := validateAWSProvisionEnv(); err != nil {
		return err
	}
	if err := ensureAWSDriverImage(ctx); err != nil {
		return err
	}
	if err := os.Setenv("E2E_SKIP_CLEANUP", "1"); err != nil {
		return fmt.Errorf("set E2E_SKIP_CLEANUP: %w", err)
	}
	return runE2ETest(ctx, "-ginkgo.v", "-test.v", "-ginkgo.focus=CAPI/CAPK lifecycle")
}

// AwsPodCertificate runs the bounded direct PodCertificate NFS mTLS acceptance
// on the pinned AWS lane. It leaves the cluster standing for diagnostics and
// writes non-secret PCR/tlshd evidence into the normal artifact directory.
func (E2e) AwsPodCertificate(ctx context.Context) error {
	if err := os.Setenv("E2E_TRANSPORT_TLS", "1"); err != nil {
		return fmt.Errorf("enable E2E transport TLS: %w", err)
	}
	if err := os.Setenv("E2E_POD_CERTIFICATE_ACCEPTANCE", "1"); err != nil {
		return fmt.Errorf("enable PodCertificate acceptance: %w", err)
	}
	if err := validateAWSLane(); err != nil {
		return err
	}
	if err := setAWSLaneEnv(); err != nil {
		return err
	}
	if err := validateAWSProvisionEnv(); err != nil {
		return err
	}
	if err := ensureAWSDriverImage(ctx); err != nil {
		return err
	}
	if err := os.Setenv("E2E_SKIP_CLEANUP", "1"); err != nil {
		return fmt.Errorf("set E2E_SKIP_CLEANUP: %w", err)
	}
	return runE2ETest(ctx, "-ginkgo.v", "-test.v", "-ginkgo.focus=accepts direct PodCertificate NFS mTLS on AWS")
}

// AwsDown destroys the CAPA/AWS workload cluster for the pinned run
// (E2E_CLEANUP_ONLY=1) and clears the pinned run state on success, ending the
// dev loop started by `mage e2e:aws`.
func (E2e) AwsDown(ctx context.Context) error {
	if err := validateAWSLane(); err != nil {
		return err
	}
	if err := setAWSLaneEnv(); err != nil {
		return err
	}
	if err := os.Setenv("E2E_CLEANUP_ONLY", "1"); err != nil {
		return fmt.Errorf("set E2E_CLEANUP_ONLY: %w", err)
	}
	err := runE2ETest(ctx, "-ginkgo.v", "-ginkgo.focus=CAPI/CAPK lifecycle")
	if err == nil {
		clearE2EState()
	}
	return err
}

// AwsReapCheck runs the read-only orphan detector locally: it classifies AWS
// resources tagged zfs-csi-e2e=owned as LIVE / UNCLASSIFIABLE / SUSPECTED_ORPHAN
// against the live CAPI Cluster objects, and NEVER deletes. The
// reaper-cronjob.yaml CronJob runs the same script unattended in-cluster.
// Actual deletion is a separate, deferred, human-gated tool. Requires aws +
// kubectl + jq on PATH and a kube-context pointed at the CAPA mgmt cluster.
func (E2e) AwsReapCheck(ctx context.Context) error {
	repoRoot, err := filepath.Abs(".")
	if err != nil {
		return fmt.Errorf("resolve repo root: %w", err)
	}
	script := filepath.Join(repoRoot, "test", "e2e", "aws", "reaper", "orphan-detector.sh")
	cmd := exec.CommandContext(ctx, "bash", script)
	cmd.Env = os.Environ()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("orphan detector: %w", err)
	}
	return nil
}

// staticLaneRequiredEnv are the knobs the static lane cannot default: the
// gitignored InfrastructureConfig (owner→node mapping, endpoints), the
// pre-existing workload cluster kubeconfig, and the driver image. All real
// infrastructure identity arrives through these at runtime — none of it may
// live in the repository.
var staticLaneRequiredEnv = []string{
	"E2E_INFRASTRUCTURE_CONFIG",
	"E2E_WORKLOAD_KUBECONFIG",
	"E2E_DRIVER_IMAGE",
}

// setStaticLaneEnv selects the static (pre-existing cluster) lane and applies
// its safety defaults: E2E_SKIP_CLEANUP=1 (the suite never tears the cluster
// down implicitly — teardown of the driver release is `mage e2e:staticDown`)
// and E2E_ENCRYPTION=0 (never deploy the dev-mode OpenBao onto a shared
// cluster; it would apply into the `openbao` namespace). Explicit env values
// win over the defaults.
func setStaticLaneEnv() error {
	if err := os.Setenv("E2E_INFRASTRUCTURE_PROVIDER", "static"); err != nil {
		return fmt.Errorf("set E2E_INFRASTRUCTURE_PROVIDER: %w", err)
	}
	defaults := [][2]string{
		{"E2E_SKIP_CLEANUP", "1"},
		{"E2E_ENCRYPTION", "0"},
	}
	for _, kv := range defaults {
		if strings.TrimSpace(os.Getenv(kv[0])) == "" {
			if err := os.Setenv(kv[0], kv[1]); err != nil {
				return fmt.Errorf("set %s: %w", kv[0], err)
			}
		}
	}
	return nil
}

// validateStaticLaneEnv fails closed with an actionable message when a
// required static-lane knob is missing. Values come from the gitignored
// runtime configuration; there are no in-repo defaults by design.
func validateStaticLaneEnv() error {
	var missing []string
	for _, key := range staticLaneRequiredEnv {
		if strings.TrimSpace(os.Getenv(key)) == "" {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("the static E2E lane requires these env vars (see docs/e2e-static-lane.md; values live in gitignored configuration): %s", strings.Join(missing, ", "))
	}
	if path := os.Getenv("E2E_WORKLOAD_KUBECONFIG"); path != "" {
		if _, err := os.Stat(path); err != nil {
			return fmt.Errorf("E2E_WORKLOAD_KUBECONFIG %q is not readable: %w", path, err)
		}
	}
	if path := os.Getenv("E2E_INFRASTRUCTURE_CONFIG"); path != "" {
		if _, err := os.Stat(path); err != nil {
			return fmt.Errorf("E2E_INFRASTRUCTURE_CONFIG %q is not readable: %w", path, err)
		}
	}
	return nil
}

// staticRunLabelSelector is the e2eOwnershipLabels contract as a kubectl label
// selector: only objects the harness itself created for this run match it.
// Cleanup and reap listing operate EXCLUSIVELY through this selector plus the
// helm release — never by namespace or name sweep — so foreign resources on
// the shared cluster are unreachable by construction.
func staticRunLabelSelector(runID string) string {
	return "app.kubernetes.io/name=zfs-csi-e2e,app.kubernetes.io/managed-by=ginkgo-e2e,zfs-csi.randomvariable.co.uk/e2e-run-id=" + runID
}

// staticReapResourceKinds are the resource kinds the static lane may have
// created with run ownership labels. Namespaced kinds are scanned across all
// namespaces; the label selector keeps the scan (and any deletion) scoped to
// harness-owned objects only.
var staticReapResourceKinds = []string{
	"pods",
	"persistentvolumeclaims",
	"configmaps",
	"persistentvolumes",
	"storageclasses",
	"volumesnapshotclasses.snapshot.storage.k8s.io",
	"volumeattributesclasses.storage.k8s.io",
}

// Static runs the lifecycle + smoke suite against a PRE-EXISTING cluster
// reached via E2E_WORKLOAD_KUBECONFIG (no CAPI provisioning, no cluster
// teardown). It fails closed unless the gitignored runtime configuration
// supplies E2E_INFRASTRUCTURE_CONFIG, E2E_WORKLOAD_KUBECONFIG, and
// E2E_DRIVER_IMAGE. Cleanup of the driver release and run-labeled objects is
// the explicit `mage e2e:staticDown`; the suite itself never deletes the
// cluster or foreign resources.
func (E2e) Static(ctx context.Context) error {
	if err := setStaticLaneEnv(); err != nil {
		return err
	}
	if err := validateStaticLaneEnv(); err != nil {
		return err
	}
	return runE2ETest(ctx, "-ginkgo.v", "-test.v", "-ginkgo.focus=CAPI/CAPK lifecycle")
}

// StaticConformance runs the static lifecycle lane with the external-storage
// conformance suite and transport TLS enabled. Unlike the generic static lane,
// this preset requires explicit NFS export CIDRs because static NFS classes
// cannot safely infer the consumer network from provider fixtures.
func (E2e) StaticConformance(ctx context.Context) error {
	if err := setStaticLaneEnv(); err != nil {
		return err
	}
	defaults := [][2]string{
		{"E2E_RUN_CONFORMANCE", "1"},
		{"E2E_TRANSPORT_TLS", "1"},
		{"E2E_ENCRYPTION", "0"},
	}
	for _, kv := range defaults {
		if strings.TrimSpace(os.Getenv(kv[0])) == "" {
			if err := os.Setenv(kv[0], kv[1]); err != nil {
				return fmt.Errorf("set %s: %w", kv[0], err)
			}
		}
	}
	if strings.TrimSpace(os.Getenv("E2E_NFS_EXPORT_CIDRS")) == "" {
		return fmt.Errorf("e2e:staticConformance requires E2E_NFS_EXPORT_CIDRS: set explicit consumer export CIDRs for static NFS and NFS-mTLS StorageClasses")
	}
	if err := validateStaticLaneEnv(); err != nil {
		return err
	}
	return runE2ETest(ctx, "-ginkgo.v", "-test.v", "-ginkgo.focus=CAPI/CAPK lifecycle")
}

// StaticDown removes THIS RUN's footprint from the pre-existing cluster: it
// helm-uninstalls the driver release and deletes only objects carrying the
// run's ownership labels (staticRunLabelSelector). It NEVER deletes the
// cluster, namespaces, or any object without the run labels.
func (E2e) StaticDown(ctx context.Context) error {
	if err := setStaticLaneEnv(); err != nil {
		return err
	}
	kubeconfig := strings.TrimSpace(os.Getenv("E2E_WORKLOAD_KUBECONFIG"))
	if kubeconfig == "" {
		return fmt.Errorf("E2E_WORKLOAD_KUBECONFIG required for e2e:staticDown (the pre-existing cluster's kubeconfig)")
	}
	if err := e2econfig.Init(); err != nil {
		return fmt.Errorf("init e2e config: %w", err)
	}
	runID, _, err := resolveE2ERunID()
	if err != nil {
		return err
	}
	fmt.Printf("[e2e:staticDown] run=%s: deleting run-labeled objects, then uninstalling driver release\n", runID)

	selector := staticRunLabelSelector(runID)

	// 1. Delete run-labeled leftovers (smoke pods/PVCs, setup pods, generated
	// classes) WHILE the driver is still installed: the CSI controller and
	// storage agent must still be running to serve DeleteVolume, remove the
	// Volume CR finalizer, and destroy the backend dataset. Uninstalling first
	// would orphan those objects forever on the shared cluster. The label
	// selector is the entire deletion scope: an object without this run's
	// ownership labels cannot match, so foreign resources and other runs'
	// objects are untouched. Namespaces are never deleted.
	for _, kind := range staticReapResourceKinds {
		out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig,
			"delete", kind, "--all-namespaces", "--selector", selector,
			"--ignore-not-found", "--wait=false",
		).CombinedOutput()
		if err != nil {
			return fmt.Errorf("delete run-labeled %s: %w\n%s", kind, err, string(out))
		}
		if trimmed := strings.TrimSpace(string(out)); trimmed != "" && !strings.Contains(trimmed, "No resources found") {
			fmt.Println(trimmed)
		}
	}

	// 2. Wait for the run's PersistentVolumes to disappear so the driver has
	// finished reclaiming backing datasets before we remove the driver itself.
	pvDeadline := time.Now().Add(3 * time.Minute)
	for {
		out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig,
			"get", "persistentvolumes", "--selector", selector,
			"--ignore-not-found", "-o", "name",
		).CombinedOutput()
		if err == nil && strings.TrimSpace(string(out)) == "" {
			break
		}
		if time.Now().After(pvDeadline) {
			return fmt.Errorf("run-labeled PersistentVolumes still present after 3m; not uninstalling driver (would orphan backing datasets)\n%s", string(out))
		}
		time.Sleep(5 * time.Second)
	}

	// 3. Uninstall the helm release only after the run's volumes are gone —
	// and only if the release carries THIS run's ownership label. A zfs-csi
	// release with no or a foreign run marker is not ours to delete.
	out, err := exec.CommandContext(ctx, "helm", "list",
		"--kubeconfig", kubeconfig, "--namespace", "zfs-csi", "-q",
	).CombinedOutput()
	if err != nil {
		return fmt.Errorf("helm list zfs-csi: %w\n%s", err, string(out))
	}
	releaseExists := false
	for _, name := range strings.Fields(string(out)) {
		if name == "zfs-csi" {
			releaseExists = true
		}
	}
	if releaseExists {
		owned, err := exec.CommandContext(ctx, "helm", "list",
			"--kubeconfig", kubeconfig, "--namespace", "zfs-csi", "-q",
			"--selector", "zfs-csi.randomvariable.co.uk/e2e-run-id="+runID,
		).CombinedOutput()
		if err != nil {
			return fmt.Errorf("helm list zfs-csi ownership: %w\n%s", err, string(owned))
		}
		if !strings.Contains(string(owned), "zfs-csi") {
			return fmt.Errorf("refusing to uninstall: release zfs-csi in namespace zfs-csi does not carry this run's ownership label (run %s); it may belong to another tenant or run", runID)
		}
		out, err := exec.CommandContext(ctx, "helm", "uninstall", "zfs-csi",
			"--kubeconfig", kubeconfig, "--namespace", "zfs-csi", "--ignore-not-found",
		).CombinedOutput()
		if err != nil {
			return fmt.Errorf("helm uninstall zfs-csi: %w\n%s", err, string(out))
		}
		fmt.Print(string(out))
	} else {
		fmt.Println("[e2e:staticDown] no zfs-csi release present; nothing to uninstall")
	}

	clearE2EState()
	return nil
}

// StaticReapCheck is the read-only leak detector for the static lane: it lists
// every object still carrying the run's ownership labels and NEVER deletes
// anything. Run it after `mage e2e:staticDown` to prove the shared cluster is
// clean, or any time to inventory the harness footprint.
func (E2e) StaticReapCheck(ctx context.Context) error {
	kubeconfig := strings.TrimSpace(os.Getenv("E2E_WORKLOAD_KUBECONFIG"))
	if kubeconfig == "" {
		return fmt.Errorf("E2E_WORKLOAD_KUBECONFIG required for e2e:staticReapCheck (the pre-existing cluster's kubeconfig)")
	}
	if err := e2econfig.Init(); err != nil {
		return fmt.Errorf("init e2e config: %w", err)
	}
	runID, fromState, err := resolveE2ERunID()
	if err != nil {
		return err
	}
	if strings.TrimSpace(viper.GetString(e2econfig.RunIDKey)) == "" && !fromState {
		// A freshly generated run ID would match nothing and report a vacuously
		// clean cluster. Fail loudly instead of emitting a green-but-empty proof.
		return fmt.Errorf("no run ID available: set %s (or run mage e2e:static first); a freshly generated ID would make this check meaningless", e2econfig.Env[e2econfig.RunIDKey])
	}
	selector := staticRunLabelSelector(runID)
	fmt.Printf("[e2e:staticReapCheck] read-only inventory of objects labeled %s\n", selector)
	for _, kind := range staticReapResourceKinds {
		out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig,
			"get", kind, "--all-namespaces", "--selector", selector, "--ignore-not-found",
		).CombinedOutput()
		if err != nil {
			// Report and continue: a missing CRD (e.g. snapshot classes on a
			// cluster without external-snapshotter) is not a leak.
			fmt.Printf("## %s: %v\n%s", kind, err, string(out))
			continue
		}
		if trimmed := strings.TrimSpace(string(out)); trimmed != "" {
			fmt.Printf("## %s\n%s\n", kind, trimmed)
		}
	}
	return nil
}

// TestUpDown runs the full lifecycle: create then destroy.
func (E2e) TestUpDown(ctx context.Context) error {
	if err := runE2ETest(ctx, "-ginkgo.v", "-ginkgo.focus=CAPI/CAPK lifecycle"); err != nil {
		return err
	}
	return runE2ETest(ctx, "-ginkgo.v", "-ginkgo.focus=CAPI/CAPK lifecycle", "-ginkgo.focus=cleanup")
}

func (E2e) Load(context.Context) error {
	return fmt.Errorf("e2e load is scaffolded but not implemented")
}

func (E2e) Deploy(context.Context) error {
	return fmt.Errorf("e2e deploy is scaffolded but not implemented")
}

func (E2e) Test(ctx context.Context) error {
	return runE2ETest(ctx, "-ginkgo.v")
}

func (E2e) Down(ctx context.Context) error {
	if err := os.Setenv("E2E_CLEANUP_ONLY", "1"); err != nil {
		return fmt.Errorf("set E2E_CLEANUP_ONLY: %w", err)
	}
	err := runE2ETest(ctx, "-ginkgo.v", "-ginkgo.focus=CAPI/CAPK lifecycle")
	if err == nil {
		clearE2EState()
	}
	return err
}

func (E2e) Reboot(context.Context) error {
	return fmt.Errorf("e2e reboot is scaffolded but not implemented")
}

// Clusters lists active clusters in the shared e2e namespace, newest first.
func (E2e) Clusters(ctx context.Context) error {
	clusters, err := listE2EClusters(ctx)
	if err != nil {
		return wrap("list e2e clusters", err)
	}
	if len(clusters) == 0 {
		fmt.Fprintf(os.Stderr, "no clusters found in %s\n", e2eNamespace)
		return nil
	}
	for i := len(clusters) - 1; i >= 0; i-- {
		fmt.Println(clusters[i])
	}
	return nil
}

// Vms lists the VirtualMachineInstances in the latest (or E2E_RUN_ID-named)
// E2E run, with phase and pod IP.
func (E2e) Vms(ctx context.Context) error {
	ns, err := resolveE2ERunNamespace(ctx)
	if err != nil {
		return err
	}
	return kubectlStream(ctx, "get", "vmi", "-n", ns, "-o",
		"custom-columns=NAME:.metadata.name,PHASE:.status.phase,IP:.status.interfaces[0].ipAddress,AGE:.metadata.creationTimestamp")
}

// Kubeconfig prints the workload cluster kubeconfig for the latest (or
// E2E_RUN_ID-named) E2E run to stdout. The kubeconfig secret's server IP is
// rewritten to the run's LoadBalancer service ClusterIP so it is reachable from
// the host. Set --e2e-kubeconfig (E2E_KUBECONFIG) to write to a file instead.
//
// Example: mage e2e:kubeconfig > /tmp/wl.conf
//
//	E2E_RUN_ID=r07061617 mage e2e:kubeconfig
func (E2e) Kubeconfig(ctx context.Context) error {
	if err := e2econfig.Init(); err != nil {
		return fmt.Errorf("init e2e config: %w", err)
	}
	ns, err := resolveE2ERunNamespace(ctx)
	if err != nil {
		return err
	}
	raw, err := kubectlOut(ctx, "get", "secret", ns+"-kubeconfig", "-n", ns, "-o", "jsonpath={.data.value}")
	if err != nil {
		return wrap("get workload kubeconfig secret", err)
	}
	decoded, err := base64StdDecode(strings.TrimSpace(raw))
	if err != nil {
		return wrap("decode kubeconfig secret", err)
	}
	// With controlPlaneServiceTemplate: type=LoadBalancer, CAPK writes the
	// LB external IP directly into the kubeconfig secret AND into the
	// apiserver cert SANs. The secret is already correct — no rewrite
	// needed. (The old ClusterIP-rewrite hack broke TLS because the
	// ClusterIP is not in the cert SANs.)
	if path := e2econfig.KubeconfigOutPath(); path != "" {
		if err := os.WriteFile(path, []byte(decoded), 0o600); err != nil {
			return wrap("write kubeconfig file", err)
		}
		fmt.Fprintln(os.Stderr, "wrote "+path)
		return nil
	}
	fmt.Print(decoded)
	return nil
}

// Ssh opens an interactive SSH session to a VM in the latest (or
// E2E_RUN_ID-named) E2E run. The node selector matches the VMI name by
// substring; defaults to "control-plane". Recognised shortcuts: "cp" ==
// "control-plane". Examples:
//
//	mage e2e:ssh                  # control plane
//	mage e2e:ssh storage
//	mage e2e:ssh md-0-h5srf-bv4cv # exact VMI name
//	E2E_RUN_ID=r07061617 mage e2e:ssh cp
func (E2e) Ssh(ctx context.Context, node string) error {
	if node == "" || node == "cp" {
		node = "control-plane"
	}
	ns, err := resolveE2ERunNamespace(ctx)
	if err != nil {
		return err
	}
	keyB64, err := kubectlOut(ctx, "get", "secret", ns+"-ssh-keys", "-n", ns, "-o", "jsonpath={.data.key}")
	if err != nil {
		return wrap("get run ssh-keys secret", err)
	}
	keyPEM, err := base64StdDecode(strings.TrimSpace(keyB64))
	if err != nil {
		return wrap("decode ssh key", err)
	}
	keyFile, err := os.CreateTemp("", "zfs-csi-e2e-capk-key-*")
	if err != nil {
		return wrap("create ssh key temp file", err)
	}
	defer func() { _ = os.Remove(keyFile.Name()) }()
	if err := os.Chmod(keyFile.Name(), 0o600); err != nil {
		return wrap("chmod ssh key", err)
	}
	if _, err := keyFile.WriteString(keyPEM); err != nil {
		return wrap("write ssh key", err)
	}
	if err := keyFile.Close(); err != nil {
		return wrap("close ssh key temp file", err)
	}
	vmi, ip, err := resolveVMIDNS(ctx, ns, node)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "ssh capk@%s (%s)\n", ip, vmi)
	cmd := exec.CommandContext(ctx, "ssh",
		"-i", keyFile.Name(),
		"-o", "StrictHostKeyChecking=no",
		"-o", "UserKnownHostsFile=/dev/null",
		"-o", "LogLevel=ERROR",
		"capk@"+ip,
	)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	// SSH requires a controlling tty for interactive sessions; let it inherit.
	if err := cmd.Run(); err != nil {
		return wrap("ssh", err)
	}
	return nil
}

func renderTemplate(src, dst string, data e2eTemplateData) error {
	tpl, err := template.ParseFiles(src)
	if err != nil {
		return fmt.Errorf("parse %s: %w", src, err)
	}
	out, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("create %s: %w", dst, err)
	}
	defer func() { _ = out.Close() }()
	if err := tpl.Execute(out, data); err != nil {
		return fmt.Errorf("render %s: %w", src, err)
	}
	return nil
}

func loadTopologyTemplateData(path string) ([]e2eTemplateData, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read e2e topology contract: %w", err)
	}
	var topo e2eTopologyContract
	if err := yaml.Unmarshal(body, &topo); err != nil {
		return nil, fmt.Errorf("parse e2e topology contract: %w", err)
	}
	roles := make([]e2eTemplateData, 0, len(topo.Spec.Nodes))
	for _, node := range topo.Spec.Nodes {
		roles = append(roles, e2eTemplateData{
			Role:      node.Name,
			Arch:      libvirtArch(node.Arch),
			VCPU:      node.Resources.VCPU,
			MemoryMiB: node.Resources.MemoryMiB,
			MgmtMAC:   node.Interfaces.Management.MAC,
			FabricMAC: node.Interfaces.Fabric.MAC,
		})
	}
	return roles, nil
}

func libvirtArch(arch string) string {
	switch arch {
	case "amd64":
		return "x86_64"
	case "arm64":
		return "aarch64"
	default:
		return arch
	}
}

func renderImageBuilderVars(dir string) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("mkdir rendered packer output: %w", err)
	}
	data := e2ePackerTemplateData{
		BuildName:            "ubuntu-2404",
		KubernetesVersion:    "v1.36.2",
		KubernetesSeries:     "v1.36",
		KubernetesDebVersion: "1.36.2-2.1",
		KubernetesRPMVersion: "1.36.2",
		KubeVirt:             "true",
		DiskSizeMiB:          "40960",
		Format:               "qcow2",
		ArtifactName:         "ubuntu-2404-kube-v1.36.2",
		NodeCustomRoles:      "zfs_csi_e2e",
		FirstbootCustomRoles: "zfs_csi_e2e_ssh",
		ISOURL:               "https://releases.ubuntu.com/24.04/ubuntu-24.04.4-live-server-amd64.iso",
		ISOChecksum:          "e907d92eeec9df64163a7e454cbc8d7755e8ddc7ed42f99dbc80c40f1a138433",
		CPUs:                 "4",
		MemoryMB:             "8192",
		GossEntryFile:        "goss/zfs-csi-noop.yaml",
	}
	return renderPackerVars(renderedPackerVarsPath(dir), data)
}

func renderPackerVars(dst string, data e2ePackerTemplateData) error {
	content, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal rendered packer vars: %w", err)
	}
	content = append(content, '\n')
	return wrap("write rendered packer vars", os.WriteFile(dst, content, 0o600))
}

func renderedPackerVarsPath(dir string) string {
	return filepath.Join(dir, "zfs-csi-e2e.pkrvars.json")
}

// awsPackerTemplateData is the AWS-AMI-shaped image-builder var-file. It is
// DELIBERATELY distinct from e2ePackerTemplateData (the QEMU shape): the AWS
// packer builder (packer/ami/*.json) takes no ISO / format / kubevirt / cpus /
// memory (it starts from a Canonical AMI resolved by ami_filter, not an ISO
// install), and needs no firstboot SLIRP-reboot-sever role (real-network SSH,
// no SLIRP half-open-socket stall). It carries the SAME k8s version vars as the
// QEMU build (verified: pkgs.k8s.io v1.36 channel ships kubeadm 1.36.2-2.1) plus
// the AWS build knobs. ami_regions is pinned to the single build region to skip
// image-builder's default multi-region AMI copy (slow + costly for an e2e AMI).
type awsPackerTemplateData struct {
	BuildName            string `json:"build_name"`
	KubernetesVersion    string `json:"kubernetes_semver"`
	KubernetesSeries     string `json:"kubernetes_series"`
	KubernetesDebVersion string `json:"kubernetes_deb_version"`
	KubernetesRPMVersion string `json:"kubernetes_rpm_version"`
	// NodeCustomRoles bakes the zfs_csi_e2e role (zfsutils-linux, nvme/nfs
	// modules) into the AMI, so the workload nodes need no runtime apt install.
	NodeCustomRoles string `json:"node_custom_roles_post"`
	// AWSRegion is the region the AMI is built AND (via AMIRegions) the only
	// region it is copied to.
	AWSRegion string `json:"aws_region"`
	// AMIRegions overrides image-builder's big default copy list with just the
	// build region — an e2e AMI is consumed only where it is built.
	AMIRegions string `json:"ami_regions"`
	// AMIFilterOwners pins Canonical's AWS account so the source-AMI filter
	// cannot resolve to a third-party noble image.
	AMIFilterOwners string `json:"ami_filter_owners"`
	// Keep launch permissions private: AWS account-level AMI Block Public Access
	// rejects the ModifyImageAttribute calls that sharing would otherwise make.
	AMIGroups      string `json:"ami_groups"`
	AMIUsers       string `json:"ami_users"`
	SnapshotGroups string `json:"snapshot_groups"`
	SnapshotUsers  string `json:"snapshot_users"`
	// GossEntryFile points goss at our no-op gossfile (same as the QEMU build) to
	// disable image-builder's brittle release-gate goss suite.
	GossEntryFile string `json:"goss_entry_file"`
}

// awsImageBuilderVars are the k8s + build values shared by the render and its
// validation, so the two cannot drift.
var awsImageBuilderVars = struct {
	BuildName, KubernetesVersion, KubernetesSeries, KubernetesDebVersion, KubernetesRPMVersion, NodeCustomRoles string
}{
	BuildName:            "ubuntu-2404",
	KubernetesVersion:    "v1.36.2",
	KubernetesSeries:     "v1.36",
	KubernetesDebVersion: "1.36.2-2.1",
	KubernetesRPMVersion: "1.36.2",
	NodeCustomRoles:      "zfs_csi_e2e",
}

// renderAWSImageBuilderVars writes the AWS-AMI packer var-file. AWS_REGION is
// required (the AMI is region-scoped); default us-east-1 mirrors the AWS lane's
// convention but the caller should set AWS_REGION to match e2e-config-aws.yaml.
func renderAWSImageBuilderVars(dir string) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("mkdir rendered aws packer output: %w", err)
	}
	region := envDefaultString("AWS_REGION", "us-east-1")
	data := awsPackerTemplateData{
		BuildName:            awsImageBuilderVars.BuildName,
		KubernetesVersion:    awsImageBuilderVars.KubernetesVersion,
		KubernetesSeries:     awsImageBuilderVars.KubernetesSeries,
		KubernetesDebVersion: awsImageBuilderVars.KubernetesDebVersion,
		KubernetesRPMVersion: awsImageBuilderVars.KubernetesRPMVersion,
		NodeCustomRoles:      awsImageBuilderVars.NodeCustomRoles,
		AWSRegion:            region,
		AMIRegions:           region,
		AMIFilterOwners:      "099720109477", // Canonical
		AMIGroups:            "",
		AMIUsers:             "",
		SnapshotGroups:       "",
		SnapshotUsers:        "",
		GossEntryFile:        "goss/zfs-csi-noop.yaml",
	}
	body, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal aws packer vars: %w", err)
	}
	return os.WriteFile(renderedPackerVarsPath(dir), append(body, '\n'), 0o600)
}

// validateAWSRenderedPackerVars is the static gate for the AWS packer var-file
// (mirrors validateRenderedPackerVars for the QEMU shape). It asserts the k8s
// version pins, that the QEMU-only keys are ABSENT (so an accidental struct
// merge cannot smuggle iso/format/kubevirt into the AWS build), and that
// ami_regions is a single region (not the multi-region default).
func validateAWSRenderedPackerVars(path string) error {
	body, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read aws packer vars %s: %w", path, err)
	}
	parsed := map[string]any{}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return fmt.Errorf("parse aws packer vars %s: %w", path, err)
	}
	required := []string{"build_name", "kubernetes_semver", "kubernetes_series", "kubernetes_deb_version", "kubernetes_rpm_version", "node_custom_roles_post", "aws_region", "ami_regions", "ami_filter_owners", "ami_groups", "ami_users", "snapshot_groups", "snapshot_users", "goss_entry_file"}
	for _, key := range required {
		if _, ok := parsed[key]; !ok {
			return fmt.Errorf("aws packer vars %s missing %q", path, key)
		}
	}
	for _, forbidden := range []string{"kubevirt", "format", "iso_url", "iso_checksum", "firstboot_custom_roles_post", "extra_debs"} {
		if _, ok := parsed[forbidden]; ok {
			return fmt.Errorf("aws packer vars %s must not set %q (QEMU-only / role-owned)", path, forbidden)
		}
	}
	want := map[string]string{
		"build_name":             awsImageBuilderVars.BuildName,
		"kubernetes_semver":      awsImageBuilderVars.KubernetesVersion,
		"kubernetes_series":      awsImageBuilderVars.KubernetesSeries,
		"kubernetes_deb_version": awsImageBuilderVars.KubernetesDebVersion,
		"kubernetes_rpm_version": awsImageBuilderVars.KubernetesRPMVersion,
		"node_custom_roles_post": awsImageBuilderVars.NodeCustomRoles,
		"ami_filter_owners":      "099720109477",
	}
	for key, wantValue := range want {
		if got, ok := parsed[key].(string); !ok || got != wantValue {
			return fmt.Errorf("aws packer vars %s %q must be %q, got %v", path, key, wantValue, parsed[key])
		}
	}
	for _, key := range []string{"ami_groups", "ami_users", "snapshot_groups", "snapshot_users"} {
		if got, ok := parsed[key].(string); !ok || got != "" {
			return fmt.Errorf("aws packer vars %s %q must be empty for private-only sharing, got %v", path, key, parsed[key])
		}
	}
	region, _ := parsed["aws_region"].(string)
	if regions, _ := parsed["ami_regions"].(string); regions != region {
		return fmt.Errorf("aws packer vars %s ami_regions %q must equal aws_region %q (single-region e2e AMI)", path, regions, region)
	}
	return nil
}

// stageAWSImageBuilderCustomization copies the AWS packer var-file into
// packer/ami/ and the customization roles into ansible/roles/. The var-file
// goes to packer/ami/ (not packer/qemu/) because the AWS make target reads its
// PACKER_VAR_FILES relative to that builder.
func stageAWSImageBuilderCustomization(imageBuilderDir string) error {
	varsSrc := renderedPackerVarsPath(filepath.Join("test", "e2e", "_rendered", "packer-aws"))
	varsDst := filepath.Join(imageBuilderDir, "packer", "ami", "zfs-csi-e2e.pkrvars.json")
	if err := copyFile(varsSrc, varsDst, 0o600); err != nil {
		return err
	}
	gossSrc := filepath.Join("test", "e2e", "packer", "image-builder", "goss", "zfs-csi-noop.yaml")
	gossDst := filepath.Join(imageBuilderDir, "packer", "goss", "zfs-csi-noop.yaml")
	if err := copyFile(gossSrc, gossDst, 0o600); err != nil {
		return err
	}
	// Only the node role is needed for AWS (no firstboot SLIRP-sever role).
	src := filepath.Join("test", "e2e", "packer", "image-builder", "ansible", "roles", "zfs_csi_e2e")
	dst := filepath.Join(imageBuilderDir, "ansible", "roles", "zfs_csi_e2e")
	return copyDir(src, dst)
}

// awsAMIManifestPath is where image-builder's AWS packer manifest post-processor
// writes the built AMI id(s). packer/ami/packer.json sets manifest_output to a
// BARE relative "manifest.json", and the AMI Makefile rule invokes packer with
// NO cd (the template arg is CWD-relative "packer/ami/packer.json"), so with
// `make -C <imageBuilderDir>` packer's CWD is imageBuilderDir (images/capi) and
// the manifest lands at <imageBuilderDir>/manifest.json — NOT under packer/ami/.
func awsAMIManifestPath(imageBuilderDir string) string {
	return filepath.Join(imageBuilderDir, "manifest.json")
}

// parseAMIIDFromManifest reads image-builder's packer manifest and returns the
// most recent build's artifact_id, which packer formats as "region:ami-id"
// (one comma-joined pair per copied region; single region here).
func parseAMIIDFromManifest(manifestPath string) (string, error) {
	body, err := os.ReadFile(manifestPath)
	if err != nil {
		return "", fmt.Errorf("read packer manifest %s: %w", manifestPath, err)
	}
	var manifest struct {
		Builds []struct {
			ArtifactID string `json:"artifact_id"`
		} `json:"builds"`
	}
	if err := json.Unmarshal(body, &manifest); err != nil {
		return "", fmt.Errorf("parse packer manifest %s: %w", manifestPath, err)
	}
	if len(manifest.Builds) == 0 {
		return "", fmt.Errorf("packer manifest %s has no builds", manifestPath)
	}
	artifact := manifest.Builds[len(manifest.Builds)-1].ArtifactID
	if artifact == "" {
		return "", fmt.Errorf("packer manifest %s last build has empty artifact_id", manifestPath)
	}
	return artifact, nil
}

const (
	imageBuilderGitRepo = "https://github.com/kubernetes-sigs/image-builder.git"
	// imageBuilderGitRef is the image-builder ref the zfs-csi golden build is
	// validated against. Bumping it requires re-validating the full
	// e2e:imageBuild flow: image-builder's ansible/packer templates and its
	// pinned ansible-core version can change between tags. v0.1.53 pins
	// ansible-core 2.18.6 and carries the boolean-correct `| bool` role
	// conditionals that strict ansible-core (>=2.18) requires.
	imageBuilderGitRef  = "v0.1.53"
	imageBuilderRefEnv  = "ZFS_CSI_IMAGE_BUILDER_REF"
	imageBuilderPathEnv = "IMAGE_BUILDER_CAPI_DIR"
)

// ensureImageBuilderCheckout returns the image-builder images/capi directory,
// cloning a mage-managed, gitignored copy under test/e2e/_artifacts when needed.
//
// mage owning the checkout means staging the zfs-csi role/vars never pollutes a
// developer's personal image-builder working tree (which previously caused the
// staged role to be lost on a git clean of that personal checkout). Set
// IMAGE_BUILDER_CAPI_DIR to override with an existing checkout for offline use.
func ensureImageBuilderCheckout(ctx context.Context) (string, error) {
	if dir := os.Getenv(imageBuilderPathEnv); dir != "" {
		if err := validateImageBuilderCAPIDir(dir); err != nil {
			return "", err
		}
		return dir, nil
	}
	base, err := filepath.Abs(filepath.Join("test", "e2e", "_artifacts", "image-builder"))
	if err != nil {
		return "", fmt.Errorf("resolve image-builder checkout dir: %w", err)
	}
	capiDir := filepath.Join(base, "images", "capi")
	if err := validateImageBuilderCAPIDir(capiDir); err == nil {
		return capiDir, nil // reuse a healthy existing checkout
	}
	if err := os.RemoveAll(base); err != nil {
		return "", fmt.Errorf("clean stale image-builder checkout: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(base), 0o750); err != nil {
		return "", fmt.Errorf("mkdir image-builder parent: %w", err)
	}
	ref := envDefaultString(imageBuilderRefEnv, imageBuilderGitRef)
	if _, err := magetools.RunBinary(ctx, "git",
		[]string{"clone", "--depth", "1", "--branch", ref, imageBuilderGitRepo, base},
		magetools.WithStdout()); err != nil {
		return "", wrap("clone image-builder "+ref, err)
	}
	if err := validateImageBuilderCAPIDir(capiDir); err != nil {
		return "", fmt.Errorf("cloned image-builder checkout invalid: %w", err)
	}
	return capiDir, nil
}

func validateImageBuilderCAPIDir(imageBuilderDir string) error {
	checks := []string{
		filepath.Join("packer", "qemu", "qemu-ubuntu-2404.json"),
		filepath.Join("packer", "qemu", "packer.json.tmpl"),
		filepath.Join("packer", "qemu", "scripts", "build_kubevirt_image.sh"),
		filepath.Join("ansible", "roles"),
	}
	for _, rel := range checks {
		if _, err := os.Stat(filepath.Join(imageBuilderDir, rel)); err != nil {
			return fmt.Errorf("IMAGE_BUILDER_CAPI_DIR does not look like image-builder/images/capi, missing %s: %w", rel, err)
		}
	}
	return nil
}

func stageImageBuilderCustomization(imageBuilderDir string) error {
	varsSrc := renderedPackerVarsPath(filepath.Join("test", "e2e", "_rendered", "packer"))
	varsDst := filepath.Join(imageBuilderDir, "packer", "qemu", "zfs-csi-e2e.pkrvars.json")
	if err := copyFile(varsSrc, varsDst, 0o600); err != nil {
		return err
	}
	gossSrc := filepath.Join("test", "e2e", "packer", "image-builder", "goss", "zfs-csi-noop.yaml")
	gossDst := filepath.Join(imageBuilderDir, "packer", "goss", "zfs-csi-noop.yaml")
	if err := copyFile(gossSrc, gossDst, 0o600); err != nil {
		return err
	}
	rolesBase := filepath.Join("test", "e2e", "packer", "image-builder", "ansible", "roles")
	for _, role := range []string{"zfs_csi_e2e", "zfs_csi_e2e_ssh"} {
		src := filepath.Join(rolesBase, role)
		dst := filepath.Join(imageBuilderDir, "ansible", "roles", role)
		if err := copyDir(src, dst); err != nil {
			return err
		}
	}
	return nil
}

func validateTektonYAML(ctx context.Context) error {
	files, err := filepath.Glob(filepath.Join(".tekton", "*.yaml"))
	if err != nil {
		return fmt.Errorf("find tekton yaml: %w", err)
	}
	for _, file := range files {
		body, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("read %s: %w", file, err)
		}
		var doc any
		if err := yaml.Unmarshal(body, &doc); err != nil {
			return fmt.Errorf("parse %s: %w", file, err)
		}
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		return nil
	}
	for _, file := range files {
		if _, err := magetools.RunBinary(ctx, "kubectl", []string{"create", "--dry-run=client", "--validate=false", "-f", file}); err != nil {
			return wrap("kubectl dry-run client "+file, err)
		}
	}
	return nil
}

func validateRenderedPackerVars(path string) error {
	body, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read rendered packer vars %s: %w", path, err)
	}
	parsed := map[string]any{}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return fmt.Errorf("parse rendered packer vars %s: %w", path, err)
	}
	required := []string{"build_name", "kubernetes_semver", "kubernetes_series", "kubernetes_deb_version", "kubernetes_rpm_version", "kubevirt", "disk_size", "format", "artifact_name", "node_custom_roles_post", "firstboot_custom_roles_post", "iso_url", "iso_checksum", "cpus", "memory", "goss_entry_file"}
	for _, key := range required {
		if _, ok := parsed[key]; !ok {
			return fmt.Errorf("rendered packer vars %s missing %q", path, key)
		}
	}
	if _, ok := parsed["extra_debs"]; ok {
		return fmt.Errorf("rendered packer vars %s must not set extra_debs; zfs_csi_e2e role owns packages", path)
	}
	want := map[string]string{
		"build_name":             "ubuntu-2404",
		"kubernetes_semver":      "v1.36.2",
		"kubernetes_series":      "v1.36",
		"kubernetes_deb_version": "1.36.2-2.1",
		"kubernetes_rpm_version": "1.36.2",
		"kubevirt":               "true",
		"disk_size":              "40960",
		"format":                 "qcow2",
		"artifact_name":          "ubuntu-2404-kube-v1.36.2",
		"node_custom_roles_post": "zfs_csi_e2e",
	}
	for key, wantValue := range want {
		if got, ok := parsed[key].(string); !ok || got != wantValue {
			return fmt.Errorf("rendered packer vars %s %q must be %q, got %v", path, key, wantValue, parsed[key])
		}
	}
	return nil
}

func validateAWSLane() error {
	const (
		clusterTemplatePath = "test/e2e/aws/cluster-template-zfs-csi-aws.yaml"
		e2eConfigPath       = "test/e2e/e2e-config-aws.yaml"
	)
	storageMDNames := []string{"${CLUSTER_NAME}-storage-a", "${CLUSTER_NAME}-storage-b"}

	docs, body, err := readYAMLDocuments(clusterTemplatePath)
	if err != nil {
		return err
	}
	if len(docs) != 15 {
		return fmt.Errorf("AWS cluster template %s must contain 15 non-empty YAML documents, got %d", clusterTemplatePath, len(docs))
	}
	if err := validateAWSClusterDocument(clusterTemplatePath, docs); err != nil {
		return err
	}
	if err := validateAWSCCMDocuments(clusterTemplatePath, docs); err != nil {
		return err
	}
	if err := validateAWSStorageDocuments(clusterTemplatePath, docs, storageMDNames...); err != nil {
		return err
	}
	if err := validateAWSIdentityRefDocument(clusterTemplatePath, docs); err != nil {
		return err
	}
	// Check the default path and the documented cross-account override without
	// requiring credentials or rendering a clusterctl repository.
	if err := validateAWSIdentityReference(defaultAWSIdentityKind, defaultAWSIdentityName); err != nil {
		return fmt.Errorf("validate default AWS identity: %w", err)
	}
	if err := validateAWSIdentityReference("AWSClusterStaticIdentity", "e2e-static-identity"); err != nil {
		return fmt.Errorf("validate static AWS identity override: %w", err)
	}
	if err := validateRenderedAWSIdentityRef(body, defaultAWSIdentityKind, defaultAWSIdentityName); err != nil {
		return fmt.Errorf("render default AWS identity reference: %w", err)
	}
	if err := validateRenderedAWSIdentityRef(body, "AWSClusterStaticIdentity", "e2e-static-identity"); err != nil {
		return fmt.Errorf("render static AWS identity reference: %w", err)
	}

	e2eConfig, err := readYAMLDocument(e2eConfigPath)
	if err != nil {
		return err
	}
	if !hasAWSFlavorFile(e2eConfig) {
		return fmt.Errorf("AWS e2e config %s must register aws InfrastructureProvider flavor cluster-template-zfs-csi-aws.yaml", e2eConfigPath)
	}
	return nil
}

func validateKubeVirtLane(root string) error {
	path := filepath.Join(root, "kubevirt", "cluster-template-zfs-csi.yaml")
	docs, _, err := readYAMLDocuments(path)
	if err != nil {
		return err
	}
	if len(docs) != 13 {
		return fmt.Errorf("KubeVirt cluster template %s must contain 13 non-empty YAML documents, got %d", path, len(docs))
	}
	for _, suffix := range []string{"storage-a", "storage-b"} {
		name := "${CLUSTER_NAME}-" + suffix
		if err := validateKubeVirtStorageDocuments(path, docs, name, suffix); err != nil {
			return err
		}
	}
	return nil
}

func validateKubeVirtStorageDocuments(path string, docs []map[string]any, name, owner string) error {
	counts := map[string]int{}
	for _, doc := range docs {
		if stringAt(doc, "metadata", "name") != name {
			continue
		}
		kind := stringAt(doc, "kind")
		counts[kind]++
		switch kind {
		case "MachineDeployment":
			if got := stringAt(doc, "spec", "template", "metadata", "labels", "zfs.csi.randomvariable.co.uk/storage-owner"); got != owner {
				return fmt.Errorf("KubeVirt storage MachineDeployment %s owner selector must be %q, got %q", name, owner, got)
			}
			if got := stringAt(doc, "spec", "template", "spec", "bootstrap", "configRef", "name"); got != name {
				return fmt.Errorf("KubeVirt storage MachineDeployment %s bootstrap ref must be %q, got %q", name, name, got)
			}
			if got := stringAt(doc, "spec", "template", "spec", "infrastructureRef", "name"); got != name {
				return fmt.Errorf("KubeVirt storage MachineDeployment %s infrastructure ref must be %q, got %q", name, name, got)
			}
		case "KubevirtMachineTemplate":
			serial := "tank-" + strings.TrimPrefix(owner, "storage-")
			body, err := yaml.Marshal(doc)
			if err != nil {
				return fmt.Errorf("marshal KubeVirt storage MachineTemplate %s: %w", name, err)
			}
			if !bytes.Contains(body, []byte("serial: "+serial)) {
				return fmt.Errorf("KubeVirt storage MachineTemplate %s must define deterministic disk serial %q", name, serial)
			}
		case "KubeadmConfigTemplate":
			labels := stringAt(doc, "spec", "template", "spec", "joinConfiguration", "nodeRegistration", "kubeletExtraArgs", "node-labels")
			if labels != "zfs.csi.randomvariable.co.uk/storage-owner="+owner {
				return fmt.Errorf("KubeVirt storage bootstrap %s owner label must be %q, got %q", name, owner, labels)
			}
		}
	}
	for _, kind := range []string{"MachineDeployment", "KubevirtMachineTemplate", "KubeadmConfigTemplate"} {
		if counts[kind] != 1 {
			return fmt.Errorf("KubeVirt template %s must define exactly one %s named %q, got %d", path, kind, name, counts[kind])
		}
	}
	return nil
}

// validateAWSProvisionEnv fails fast on the caller-supplied AWS provisioning env
// vars that the flavor interpolates into every AWSMachineTemplate + AWSCluster
// (${AWS_AMI_ID}/${AWS_SSH_KEY_NAME}/${AWS_REGION}), the bastion ingress CIDR,
// plus the driver image. If any
// is empty, clusterctl substitutes "" and provisioning burns the full
// control-plane-init timeout (~60m) before failing with an opaque EC2
// "MissingParameter: ImageId". These are account-specific with no sensible
// default, so require them up front. Called ONLY on the provisioning path (Aws),
// NOT on teardown (AwsDown), which deletes by name and needs none of them.
func validateAWSProvisionEnv() error {
	required := []string{"AWS_REGION", "AWS_SSH_KEY_NAME", "E2E_AWS_BASTION_ALLOWED_CIDR", "AWS_AMI_ID", imageRepoEnvName}
	var missing []string
	for _, k := range required {
		if strings.TrimSpace(os.Getenv(k)) == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("AWS provisioning requires these env vars (see docs/e2e-aws-lane.md): %s", strings.Join(missing, ", "))
	}
	return validateAWSIdentityReference(os.Getenv("AWS_IDENTITY_KIND"), os.Getenv("AWS_IDENTITY_NAME"))
}

// ensureAWSDriverImage publishes the mutable local development image before the
// AWS lifecycle suite applies its driver installation manifest.
func ensureAWSDriverImage(ctx context.Context) error {
	if err := (Image{}).Driver(ctx); err != nil {
		return fmt.Errorf("build and push AWS E2E driver image: %w", err)
	}
	ref := driverImageRef()
	if err := os.Setenv("E2E_DRIVER_IMAGE", ref); err != nil {
		return fmt.Errorf("set E2E_DRIVER_IMAGE: %w", err)
	}
	fmt.Printf("[e2e:aws] E2E_DRIVER_IMAGE=%s\n", ref)
	return nil
}

// validateAWSIdentityReference accepts the CAPA cluster identity kinds usable by
// AWSCluster.spec.identityRef. Credentials belong in CAPA-managed Secrets, never
// in the generated workload-cluster template.
func validateAWSIdentityReference(kind, name string) error {
	kind = strings.TrimSpace(kind)
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("AWS_IDENTITY_NAME must not be empty")
	}
	switch kind {
	case "AWSClusterControllerIdentity", "AWSClusterStaticIdentity", "AWSClusterRoleIdentity":
		return nil
	default:
		return fmt.Errorf("AWS_IDENTITY_KIND=%q is unsupported; want AWSClusterControllerIdentity, AWSClusterStaticIdentity, or AWSClusterRoleIdentity", kind)
	}
}

func validateAWSIdentityRefDocument(path string, docs []map[string]any) error {
	clusters := 0
	for _, doc := range docs {
		if stringAt(doc, "kind") != "AWSCluster" {
			continue
		}
		clusters++
		if got := stringAt(doc, "spec", "identityRef", "kind"); got != "${AWS_IDENTITY_KIND}" {
			return fmt.Errorf("AWSCluster %s identityRef.kind must be ${AWS_IDENTITY_KIND}, got %q", path, got)
		}
		if got := stringAt(doc, "spec", "identityRef", "name"); got != "${AWS_IDENTITY_NAME}" {
			return fmt.Errorf("AWSCluster %s identityRef.name must be ${AWS_IDENTITY_NAME}, got %q", path, got)
		}
	}
	if clusters != 1 {
		return fmt.Errorf("AWS cluster template %s must define exactly one AWSCluster document, got %d", path, clusters)
	}
	return nil
}

func validateRenderedAWSIdentityRef(body []byte, kind, name string) error {
	body = bytes.ReplaceAll(body, []byte("${AWS_IDENTITY_KIND}"), []byte(kind))
	body = bytes.ReplaceAll(body, []byte("${AWS_IDENTITY_NAME}"), []byte(name))

	decoder := yamlv3.NewDecoder(bytes.NewReader(body))
	for {
		var doc map[string]any
		if err := decoder.Decode(&doc); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("parse rendered AWS template: %w", err)
		}
		if stringAt(doc, "kind") != "AWSCluster" {
			continue
		}
		if got := stringAt(doc, "spec", "identityRef", "kind"); got != kind {
			return fmt.Errorf("identityRef.kind = %q, want %q", got, kind)
		}
		if got := stringAt(doc, "spec", "identityRef", "name"); got != name {
			return fmt.Errorf("identityRef.name = %q, want %q", got, name)
		}
		return nil
	}

	return errors.New("rendered AWS template has no AWSCluster")
}

func validateAWSClusterDocument(path string, docs []map[string]any) error {
	clusters := 0
	awsClusters := 0
	for i, doc := range docs {
		if got := findNestedKey(doc, "apiVersion"); got != "" {
			return fmt.Errorf("AWS cluster template %s document %d must not contain nested apiVersion at %s; CAPI v1beta2 refs use apiGroup/kind/name", path, i+1, got)
		}
		if stringAt(doc, "kind") == "AWSCluster" {
			awsClusters++
			if enabled, ok := valueAt(doc, "spec", "bastion", "enabled"); !ok || enabled != true {
				return fmt.Errorf("AWSCluster %s spec.bastion.enabled must be true", path)
			}
			allowed := stringSliceAt(doc, "spec", "bastion", "allowedCIDRBlocks")
			if len(allowed) != 1 || allowed[0] != "${E2E_AWS_BASTION_ALLOWED_CIDR}" {
				return fmt.Errorf("AWSCluster %s spec.bastion.allowedCIDRBlocks must contain only ${E2E_AWS_BASTION_ALLOWED_CIDR}, got %v", path, allowed)
			}
			if _, ok := valueAt(doc, "spec", "network", "vpc", "ipv6"); !ok {
				return fmt.Errorf("AWSCluster %s spec.network.vpc.ipv6 must enable an AWS-assigned IPv6 CIDR", path)
			}
			if got := stringAt(doc, "spec", "controlPlaneLoadBalancer", "loadBalancerType"); got != "nlb" {
				return fmt.Errorf("AWSCluster %s dual-stack control plane load balancer must use nlb, got %q", path, got)
			}
			if got := stringAt(doc, "spec", "controlPlaneLoadBalancer", "targetGroupIPType"); got != "ipv6" {
				return fmt.Errorf("AWSCluster %s control plane target group IP type must be ipv6, got %q", path, got)
			}
			if _, unsupported := valueAt(doc, "spec", "controlPlaneLoadBalancer", "loadBalancerIPAddressType"); unsupported {
				return fmt.Errorf("AWSCluster %s control plane load balancer uses unsupported CAPA v2.11.1 field loadBalancerIPAddressType", path)
			}
		}
		if stringAt(doc, "kind") != "Cluster" {
			continue
		}
		clusters++
		if got := stringAt(doc, "apiVersion"); got != "cluster.x-k8s.io/v1beta2" {
			return fmt.Errorf("AWS Cluster document %s apiVersion must be cluster.x-k8s.io/v1beta2, got %q", path, got)
		}
		if got := stringAt(doc, "spec", "infrastructureRef", "apiGroup"); got != "infrastructure.cluster.x-k8s.io" {
			return fmt.Errorf("AWS Cluster document %s infrastructureRef.apiGroup must be infrastructure.cluster.x-k8s.io, got %q", path, got)
		}
		if got := stringAt(doc, "spec", "infrastructureRef", "kind"); got != "AWSCluster" {
			return fmt.Errorf("AWS Cluster document %s infrastructureRef.kind must be AWSCluster, got %q", path, got)
		}
		if got := stringAt(doc, "spec", "infrastructureRef", "name"); got != "${CLUSTER_NAME}" {
			return fmt.Errorf("AWS Cluster document %s infrastructureRef.name must be ${CLUSTER_NAME}, got %q", path, got)
		}
		if got := stringAt(doc, "spec", "controlPlaneRef", "apiGroup"); got != "controlplane.cluster.x-k8s.io" {
			return fmt.Errorf("AWS Cluster document %s controlPlaneRef.apiGroup must be controlplane.cluster.x-k8s.io, got %q", path, got)
		}
		if got := stringAt(doc, "spec", "controlPlaneRef", "kind"); got != "KubeadmControlPlane" {
			return fmt.Errorf("AWS Cluster document %s controlPlaneRef.kind must be KubeadmControlPlane, got %q", path, got)
		}
		if got := stringAt(doc, "spec", "controlPlaneRef", "name"); got != "${CLUSTER_NAME}-control-plane" {
			return fmt.Errorf("AWS Cluster document %s controlPlaneRef.name must be ${CLUSTER_NAME}-control-plane, got %q", path, got)
		}
	}
	if clusters != 1 {
		return fmt.Errorf("AWS cluster template %s must define exactly one CAPI Cluster document, got %d", path, clusters)
	}
	if awsClusters != 1 {
		return fmt.Errorf("AWS cluster template %s must define exactly one AWSCluster document, got %d", path, awsClusters)
	}
	return nil
}

func validateAWSCCMDocuments(path string, docs []map[string]any) error {
	crs := 0
	configMaps := 0
	for _, doc := range docs {
		switch {
		case stringAt(doc, "kind") == "ClusterResourceSet" && stringAt(doc, "metadata", "name") == "crs-ccm":
			crs++
			if got := stringAt(doc, "spec", "clusterSelector", "matchLabels", "ccm"); got != "external" {
				return fmt.Errorf("AWS CCM ClusterResourceSet %s must select ccm=external, got %q", path, got)
			}
		case stringAt(doc, "kind") == "ConfigMap" && stringAt(doc, "metadata", "name") == "cloud-controller-manager-addon":
			configMaps++
			if got := stringAt(doc, "metadata", "namespace"); got != "${NAMESPACE}" {
				return fmt.Errorf("AWS CCM ConfigMap %s namespace must be ${NAMESPACE}, got %q", path, got)
			}
			if got := stringAt(doc, "data", "aws-ccm-external.yaml"); got == "" {
				return fmt.Errorf("AWS CCM ConfigMap %s must include aws-ccm-external.yaml data", path)
			}
		}
	}
	if crs != 1 {
		return fmt.Errorf("AWS cluster template %s must define exactly one crs-ccm ClusterResourceSet, got %d", path, crs)
	}
	if configMaps != 1 {
		return fmt.Errorf("AWS cluster template %s must define exactly one cloud-controller-manager-addon ConfigMap, got %d", path, configMaps)
	}
	return nil
}

func validateAWSStorageDocuments(path string, docs []map[string]any, storageMDNames ...string) error {
	if len(storageMDNames) == 0 {
		return fmt.Errorf("AWS cluster template %s must define at least one storage owner", path)
	}
	wanted := make(map[string]struct{}, len(storageMDNames))
	for _, name := range storageMDNames {
		if name == "" {
			return fmt.Errorf("AWS cluster template %s storage owner name must not be empty", path)
		}
		if _, exists := wanted[name]; exists {
			return fmt.Errorf("AWS cluster template %s duplicate storage owner name %q", path, name)
		}
		wanted[name] = struct{}{}
	}
	storageMDs := map[string]int{}
	storageBootstrapTemplates := map[string]int{}
	storageMachineTemplates := map[string]int{}
	for _, doc := range docs {
		kind := stringAt(doc, "kind")
		name := stringAt(doc, "metadata", "name")
		if _, ok := wanted[name]; !ok {
			continue
		}
		switch {
		case kind == "MachineDeployment":
			storageMDs[name]++
			if got := stringAt(doc, "spec", "template", "spec", "bootstrap", "configRef", "name"); got != name {
				return fmt.Errorf("AWS storage MachineDeployment %s bootstrap configRef.name must be %q, got %q", path, name, got)
			}
			if got := stringAt(doc, "spec", "template", "spec", "infrastructureRef", "name"); got != name {
				return fmt.Errorf("AWS storage MachineDeployment %s infrastructureRef.name must be %q, got %q", path, name, got)
			}
		case kind == "KubeadmConfigTemplate":
			storageBootstrapTemplates[name]++
			commands := stringSliceAt(doc, "spec", "template", "spec", "preKubeadmCommands")
			installIndex := indexCommandContaining(commands, "apt-get install", "nfs-kernel-server")
			unmaskIndex := slices.Index(commands, "systemctl unmask nfs-server")
			enableIndex := slices.Index(commands, "systemctl enable --now nfs-server")
			if installIndex < 0 {
				return fmt.Errorf("AWS storage KubeadmConfigTemplate %s must install nfs-kernel-server", path)
			}
			if unmaskIndex < 0 {
				return fmt.Errorf("AWS storage KubeadmConfigTemplate %s must include fail-closed bootstrap command %q", path, "systemctl unmask nfs-server")
			}
			if enableIndex < 0 {
				return fmt.Errorf("AWS storage KubeadmConfigTemplate %s must include fail-closed bootstrap command %q", path, "systemctl enable --now nfs-server")
			}
			if !(installIndex < unmaskIndex && unmaskIndex < enableIndex) {
				return fmt.Errorf("AWS storage KubeadmConfigTemplate %s must install, unmask, then enable and start nfs-server", path)
			}
		case kind == "AWSMachineTemplate":
			storageMachineTemplates[name]++
			if !hasNonRootVolumeDevice(doc, "/dev/xvdb") {
				return fmt.Errorf("AWS storage MachineTemplate %s must define nonRootVolumes deviceName /dev/xvdb", path)
			}
			if got := stringAt(doc, "spec", "template", "spec", "assignPrimaryIPv6"); got != "enabled" {
				return fmt.Errorf("AWS storage MachineTemplate %s must enable primary IPv6 assignment, got %q", path, got)
			}
		}
	}
	for _, name := range storageMDNames {
		if storageMDs[name] != 1 {
			return fmt.Errorf("AWS cluster template %s must define exactly one storage MachineDeployment named %q, got %d", path, name, storageMDs[name])
		}
		if storageBootstrapTemplates[name] != 1 {
			return fmt.Errorf("AWS cluster template %s must define exactly one storage KubeadmConfigTemplate named %q, got %d", path, name, storageBootstrapTemplates[name])
		}
		if storageMachineTemplates[name] != 1 {
			return fmt.Errorf("AWS cluster template %s must define exactly one storage AWSMachineTemplate named %q, got %d", path, name, storageMachineTemplates[name])
		}
	}
	return nil
}

func stringSliceAt(doc map[string]any, path ...string) []string {
	items := sliceAt(doc, path...)
	result := make([]string, 0, len(items))
	for _, item := range items {
		if value, ok := item.(string); ok {
			result = append(result, value)
		}
	}
	return result
}

func indexCommandContaining(commands []string, fragments ...string) int {
	for i, command := range commands {
		matches := true
		for _, fragment := range fragments {
			if !strings.Contains(command, fragment) {
				matches = false
				break
			}
		}
		if matches {
			return i
		}
	}
	return -1
}

func readYAMLDocuments(path string) ([]map[string]any, []byte, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("read YAML %s: %w", path, err)
	}
	decoder := yamlv3.NewDecoder(bytes.NewReader(body))
	docs := []map[string]any{}
	for {
		var doc map[string]any
		if err := decoder.Decode(&doc); err != nil {
			if err == io.EOF {
				break
			}
			return nil, nil, fmt.Errorf("parse YAML %s: %w", path, err)
		}
		if len(doc) == 0 {
			continue
		}
		docs = append(docs, doc)
	}
	return docs, body, nil
}

func readYAMLDocument(path string) (map[string]any, error) {
	docs, _, err := readYAMLDocuments(path)
	if err != nil {
		return nil, err
	}
	if len(docs) != 1 {
		return nil, fmt.Errorf("YAML %s must contain exactly one non-empty document, got %d", path, len(docs))
	}
	return docs[0], nil
}

func hasAWSFlavorFile(config map[string]any) bool {
	for _, provider := range sliceAt(config, "providers") {
		providerMap, ok := provider.(map[string]any)
		if !ok || stringAt(providerMap, "name") != "aws" || stringAt(providerMap, "type") != "InfrastructureProvider" {
			continue
		}
		for _, version := range sliceAt(providerMap, "versions") {
			versionMap, ok := version.(map[string]any)
			if !ok {
				continue
			}
			for _, file := range sliceAt(versionMap, "files") {
				fileMap, ok := file.(map[string]any)
				if ok && stringAt(fileMap, "sourcePath") == "aws/cluster-template-zfs-csi-aws.yaml" && stringAt(fileMap, "targetName") == "cluster-template-zfs-csi-aws.yaml" {
					return true
				}
			}
		}
	}
	return false
}

func hasNonRootVolumeDevice(doc map[string]any, deviceName string) bool {
	for _, volume := range sliceAt(doc, "spec", "template", "spec", "nonRootVolumes") {
		volumeMap, ok := volume.(map[string]any)
		if ok && stringAt(volumeMap, "deviceName") == deviceName {
			return true
		}
	}
	return false
}

func validateInfrastructureConfigs(root string) error {
	for _, provider := range []string{"kubevirt", "aws"} {
		for _, mode := range []string{"legacy", "two-owner", "three-owner"} {
			path := filepath.Join(root, "infrastructure-"+provider, mode+".yaml")
			if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
				continue
			} else if err != nil {
				return fmt.Errorf("stat infrastructure config %s: %w", path, err)
			}
			if err := validateInfrastructureConfig(path, provider, mode == "legacy"); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateInfrastructureConfig(path, provider string, legacy bool) error {
	body, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read infrastructure config %s: %w", path, err)
	}
	var config infrastructureConfig
	if err := yaml.Unmarshal(body, &config); err != nil {
		return fmt.Errorf("parse infrastructure config %s: %w", path, err)
	}
	if config.Metadata.Name == "" || config.Spec.Provider != provider || config.Spec.Flavor == "" {
		return fmt.Errorf("infrastructure config %s has missing or mismatched identity/provider/flavor", path)
	}
	if len(config.Spec.StorageOwners) == 0 {
		return fmt.Errorf("infrastructure config %s must define at least one storage owner", path)
	}
	if len(config.Spec.ConsumerWorkers) == 0 {
		return fmt.Errorf("infrastructure config %s must define consumer workers", path)
	}
	families := map[string]bool{}
	for _, family := range config.Spec.AddressFamilies {
		if family != "ipv4" && family != "ipv6" {
			return fmt.Errorf("infrastructure config %s has unsupported address family %q", path, family)
		}
		if families[family] {
			return fmt.Errorf("infrastructure config %s has duplicate address family %q", path, family)
		}
		families[family] = true
	}
	if !families["ipv4"] || (!legacy && !families["ipv6"]) {
		return fmt.Errorf("infrastructure config %s is missing required address families", path)
	}
	names := map[string]bool{}
	suffixes := map[string]bool{}
	selectors := map[string]bool{}
	disks := map[string]bool{}
	devicePaths := map[string]bool{}
	ownerDomains := map[string]bool{}
	endpoints := map[string]bool{}
	for _, owner := range config.Spec.StorageOwners {
		if owner.Name == "" || owner.MachineDeploymentSuffix == "" || len(owner.NodeSelector) != 1 || owner.Pool.Name == "" || owner.Pool.DiskID == "" || owner.NetworkDomain == "" {
			return fmt.Errorf("infrastructure config %s contains incomplete storage owner identity, selector, pool, or domain", path)
		}
		if disks[owner.Pool.DiskID] {
			return fmt.Errorf("infrastructure config %s has duplicate storage owner disk identity %q", path, owner.Pool.DiskID)
		}
		disks[owner.Pool.DiskID] = true
		selector := ""
		for key, value := range owner.NodeSelector {
			selector = key + "=" + value
		}
		for field, value := range map[string]string{"name": owner.Name, "machine selector": owner.MachineDeploymentSuffix, "node selector": selector} {
			seen := map[string]bool{}
			switch field {
			case "name":
				seen = names
			case "machine selector":
				seen = suffixes
			case "node selector":
				seen = selectors
			}
			if seen[value] {
				return fmt.Errorf("infrastructure config %s has duplicate storage owner %s %q", path, field, value)
			}
			seen[value] = true
		}
		ownerDomains[owner.NetworkDomain] = true
		if provider == "aws" {
			if owner.Pool.Discovery.Provider != "aws-ebs-volume-id" || owner.Pool.Discovery.AttachmentDeviceName != "/dev/xvdb" {
				return fmt.Errorf("infrastructure config %s AWS pool must discover its EBS volume ID from non-root attachment deviceName /dev/xvdb", path)
			}
			if owner.Pool.DeviceID != "" && !isExactAWSEBSByID(owner.Pool.DeviceID) {
				return fmt.Errorf("infrastructure config %s AWS resolved deviceID %q must be an exact /dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_vol... path", path, owner.Pool.DeviceID)
			}
			if owner.Pool.DeviceID != "" {
				if devicePaths[owner.Pool.DeviceID] {
					return fmt.Errorf("infrastructure config %s has duplicate storage owner pool device %q", path, owner.Pool.DeviceID)
				}
				devicePaths[owner.Pool.DeviceID] = true
			}
		} else if provider == "kubevirt" {
			if owner.Pool.Discovery.Provider != "" || owner.Pool.Discovery.AttachmentDeviceName != "" || !isExactDeviceID(owner.Pool.DeviceID) || !strings.HasPrefix(owner.Pool.DeviceID, "/dev/disk/by-id/virtio-") {
				return fmt.Errorf("infrastructure config %s KubeVirt pool must use virtio by-id identity", path)
			}
			if devicePaths[owner.Pool.DeviceID] {
				return fmt.Errorf("infrastructure config %s has duplicate storage owner pool device %q", path, owner.Pool.DeviceID)
			}
			devicePaths[owner.Pool.DeviceID] = true
		} else {
			return fmt.Errorf("infrastructure config %s has unsupported provider %q", path, provider)
		}
		for protocol, endpoint := range map[string]infrastructureEndpoint{"nfs": owner.Endpoints.NFS, "nvme": owner.Endpoints.NVMe} {
			if endpoint.Port <= 0 || endpoint.IPv4 == "" || (!legacy && endpoint.IPv6 == "") {
				return fmt.Errorf("infrastructure config %s owner %q has incomplete %s endpoint inputs", path, owner.Name, protocol)
			}
			for family, address := range map[string]string{"ipv4": endpoint.IPv4, "ipv6": endpoint.IPv6} {
				if address == "" {
					continue
				}
				key := fmt.Sprintf("%s/%s/%s/%d", protocol, family, address, endpoint.Port)
				if endpoints[key] {
					return fmt.Errorf("infrastructure config %s has duplicate %s endpoint input %q", path, protocol, key)
				}
				endpoints[key] = true
			}
		}
	}
	consumerDomains := map[string]bool{}
	consumerSuffixes := map[string]bool{}
	consumerNames := map[string]bool{}
	for _, worker := range config.Spec.ConsumerWorkers {
		if worker.Name == "" || worker.MachineDeploymentSuffix == "" || worker.Replicas <= 0 || worker.NetworkDomain == "" {
			return fmt.Errorf("infrastructure config %s contains incomplete consumer worker input", path)
		}
		if consumerSuffixes[worker.MachineDeploymentSuffix] {
			return fmt.Errorf("infrastructure config %s has duplicate consumer machine selector %q", path, worker.MachineDeploymentSuffix)
		}
		if consumerNames[worker.Name] {
			return fmt.Errorf("infrastructure config %s has duplicate consumer worker name %q", path, worker.Name)
		}
		consumerSuffixes[worker.MachineDeploymentSuffix] = true
		consumerNames[worker.Name] = true
		consumerDomains[worker.NetworkDomain] = true
	}
	for domain := range ownerDomains {
		if !consumerDomains[domain] {
			return fmt.Errorf("infrastructure config %s storage network domain %q has no consumer workers", path, domain)
		}
	}
	return nil
}

func isExactDeviceID(deviceID string) bool {
	if deviceID == "" || !strings.HasPrefix(deviceID, "/dev/disk/by-id/") {
		return false
	}
	return !strings.ContainsAny(deviceID, "*?[]{}") && !strings.ContainsAny(deviceID, " \t\r\n")
}

func isExactAWSEBSByID(deviceID string) bool {
	return exactAWSEBSByIDPattern.MatchString(deviceID)
}

func resolveAWSEBSDeviceID(owner infrastructureStorageOwner, volumeID string) (string, error) {
	if owner.Pool.Discovery.Provider != "aws-ebs-volume-id" || owner.Pool.Discovery.AttachmentDeviceName == "" {
		return "", fmt.Errorf("storage owner %q lacks AWS EBS attachment discovery binding", owner.Name)
	}
	if !canonicalEBSVolumeIDPattern.MatchString(volumeID) {
		return "", fmt.Errorf("storage owner %q discovered EBS volume ID %q must be canonical vol- followed by 17 lowercase hexadecimal characters", owner.Name, volumeID)
	}
	compactID := "vol" + strings.TrimPrefix(volumeID, "vol-")
	deviceID := "/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_" + compactID
	if !isExactAWSEBSByID(deviceID) {
		return "", fmt.Errorf("storage owner %q discovered invalid EBS volume ID %q", owner.Name, volumeID)
	}
	return deviceID, nil
}

func stringAt(doc map[string]any, path ...string) string {
	value, ok := valueAt(doc, path...)
	if !ok {
		return ""
	}
	text, _ := value.(string)
	return text
}

func sliceAt(doc map[string]any, path ...string) []any {
	value, ok := valueAt(doc, path...)
	if !ok {
		return nil
	}
	items, _ := value.([]any)
	return items
}

func valueAt(doc map[string]any, path ...string) (any, bool) {
	var value any = doc
	for _, key := range path {
		next, ok := value.(map[string]any)
		if !ok {
			return nil, false
		}
		value, ok = next[key]
		if !ok {
			return nil, false
		}
	}
	return value, true
}

func findNestedKey(doc map[string]any, key string) string {
	for k, value := range doc {
		if k == key {
			continue
		}
		if found := findNestedKeyValue(value, key, k); found != "" {
			return found
		}
	}
	return ""
}

func findNestedKeyValue(value any, key, path string) string {
	switch typed := value.(type) {
	case map[string]any:
		for k, child := range typed {
			childPath := path + "." + k
			if k == key {
				return childPath
			}
			if found := findNestedKeyValue(child, key, childPath); found != "" {
				return found
			}
		}
	case []any:
		for i, child := range typed {
			childPath := path + "[" + strconv.Itoa(i) + "]"
			if found := findNestedKeyValue(child, key, childPath); found != "" {
				return found
			}
		}
	}
	return ""
}

func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("read %s: %w", src, err)
	}
	defer func() { _ = in.Close() }()
	if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(dst), err)
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return fmt.Errorf("write %s: %w", dst, err)
	}
	defer func() { _ = out.Close() }()
	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("copy %s to %s: %w", src, dst, err)
	}
	return nil
}

func copyDir(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		out := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(out, 0o750)
		}
		return copyFile(path, out, 0o600)
	})
}

func removeRenderedPackerVars(dir string) error {
	for _, pattern := range []string{"*.pkrvars.hcl", "*.pkrvars.json"} {
		matches, err := filepath.Glob(filepath.Join(dir, pattern))
		if err != nil {
			return fmt.Errorf("find rendered packer vars: %w", err)
		}
		for _, path := range matches {
			if err := os.Remove(path); err != nil {
				return fmt.Errorf("remove stale rendered packer vars %s: %w", path, err)
			}
		}
	}
	return nil
}

func storageExtraDisks(stateDir string) string {
	var b strings.Builder
	for i, name := range []string{"tank0", "tank1", "tank2", "flash0", "flash1"} {
		dev := string(rune('b' + i))
		fmt.Fprintf(&b, "<disk type=\"file\" device=\"disk\">\n")
		fmt.Fprintf(&b, "      <driver name=\"qemu\" type=\"raw\"/>\n")
		fmt.Fprintf(&b, "      <source file=\"%s/%s.raw\"/>\n", stateDir, name)
		fmt.Fprintf(&b, "      <target dev=\"vd%s\" bus=\"virtio\"/>\n", dev)
		fmt.Fprintf(&b, "    </disk>")
		if i < 4 {
			b.WriteByte('\n')
			b.WriteString("    ")
		}
	}
	return b.String()
}

func validateTopologyContract(path string) error {
	body, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read e2e topology contract: %w", err)
	}
	var topo e2eTopologyContract
	if err := yaml.Unmarshal(body, &topo); err != nil {
		return fmt.Errorf("parse e2e topology contract: %w", err)
	}

	if topo.Spec.Substrate.Target != "kubevirt" {
		return fmt.Errorf("e2e topology contract target must be kubevirt, got %q", topo.Spec.Substrate.Target)
	}
	if topo.Spec.Fabric.Lane != "single-node-l2-local" || topo.Spec.Fabric.Network != "multus-linux-bridge" {
		return fmt.Errorf("e2e topology contract first lane must be single-node-l2-local Multus bridge")
	}
	if topo.Spec.Images.Root.GoldenDataSource == "" || !topo.Spec.Images.Root.PerRunClones {
		return fmt.Errorf("e2e topology contract full lane must use per-run DataSource root clones")
	}
	if topo.Spec.Images.Root.CloneCapableStorageClass != "required" {
		return fmt.Errorf("e2e topology contract must require a clone-capable root StorageClass")
	}
	if !topo.Spec.Images.ContainerDisk.BootOnly {
		return fmt.Errorf("e2e topology contract must mark containerDisk boot-only")
	}
	if len(topo.Spec.Teardown.OwnershipLabels) == 0 {
		return fmt.Errorf("e2e topology contract must define teardown ownership-label guard")
	}
	if topo.Spec.CAPI.Lifecycle != "capk-managed-kubeadm" {
		return fmt.Errorf("e2e topology contract CAPI lifecycle must be capk-managed-kubeadm, got %q", topo.Spec.CAPI.Lifecycle)
	}
	if err := validateCAPIReadiness(topo.Spec.CAPI.Readiness); err != nil {
		return err
	}
	if topo.Spec.CAPI.KubeconfigRetrieval != "capi-cluster-kubeconfig-secret" {
		return fmt.Errorf("e2e topology contract kubeconfig retrieval must use CAPI cluster kubeconfig Secret")
	}

	wantRoles := map[string]bool{
		"cp0": false, "storage-a": false, "storage-b": false,
		"worker-a0": false, "worker-a1": false, "worker-b0": false, "worker-b1": false,
	}
	seenNames := map[string]bool{}
	seenMACs := map[string]string{}
	seenIPs := map[string]string{}
	placementLane := ""
	for _, node := range topo.Spec.Nodes {
		if _, ok := wantRoles[node.Name]; !ok {
			return fmt.Errorf("unexpected e2e topology node %q", node.Name)
		}
		if seenNames[node.Name] {
			return fmt.Errorf("duplicate e2e topology node %q", node.Name)
		}
		seenNames[node.Name] = true
		wantRoles[node.Name] = true
		if node.Hostname == "" || node.JoinRole == "" || node.Arch == "" || node.MachineType == "" {
			return fmt.Errorf("e2e topology node %q missing identity fields", node.Name)
		}
		if node.Resources.VCPU <= 0 || node.Resources.MemoryMiB <= 0 {
			return fmt.Errorf("e2e topology node %q missing CPU or memory resources", node.Name)
		}
		if node.Placement.Lane == "" {
			return fmt.Errorf("e2e topology node %q missing placement lane", node.Name)
		}
		if placementLane == "" {
			placementLane = node.Placement.Lane
		} else if node.Placement.Lane != placementLane {
			return fmt.Errorf("e2e topology node %q placement lane %q differs from %q", node.Name, node.Placement.Lane, placementLane)
		}
		if node.Interfaces.Management.Address == "" || node.Interfaces.Management.MAC == "" || node.Interfaces.Fabric.Address == "" || node.Interfaces.Fabric.MAC == "" {
			return fmt.Errorf("e2e topology node %q missing management/fabric address or MAC", node.Name)
		}
		for label, mac := range map[string]string{"management": node.Interfaces.Management.MAC, "fabric": node.Interfaces.Fabric.MAC} {
			if mac == "" {
				return fmt.Errorf("e2e topology node %q missing %s MAC", node.Name, label)
			}
			if other := seenMACs[mac]; other != "" {
				return fmt.Errorf("duplicate e2e topology %s MAC %q on %s and %s", label, mac, other, node.Name)
			}
			seenMACs[mac] = node.Name
		}
		for label, address := range map[string]string{"management": node.Interfaces.Management.Address, "fabric": node.Interfaces.Fabric.Address} {
			ip := addressHost(address)
			if ip == "" {
				return fmt.Errorf("e2e topology node %q missing %s IP", node.Name, label)
			}
			if other := seenIPs[ip]; other != "" {
				return fmt.Errorf("duplicate e2e topology %s IP %q on %s and %s", label, ip, other, node.Name)
			}
			seenIPs[ip] = node.Name
		}
		if node.Interfaces.Fabric.MTU != 9000 {
			return fmt.Errorf("e2e topology node %q fabric MTU must remain 9000", node.Name)
		}
		if source, ok := node.Disks.Root["source"].(string); !ok || source != "datasource-clone" {
			return fmt.Errorf("e2e topology node %q root disk must be a DataSource clone", node.Name)
		}
		if len(node.Disks.Root) == 0 || len(node.Labels) == 0 || len(node.Readiness) == 0 {
			return fmt.Errorf("e2e topology node %q missing disks, labels, or readiness sentinels", node.Name)
		}
		isStorage := strings.HasPrefix(node.Name, "storage-")
		if isStorage && len(node.Disks.Data) == 0 {
			return fmt.Errorf("e2e topology storage node %q must define data disks beyond root", node.Name)
		}
		if !isStorage && len(node.Disks.Data) != 0 {
			return fmt.Errorf("e2e topology node %q must not define storage data disks", node.Name)
		}
		for _, disk := range node.Disks.Data {
			if disk["name"] == nil || disk["size"] == nil || disk["pool"] == nil {
				return fmt.Errorf("e2e topology node %q has incomplete data disk contract", node.Name)
			}
		}
		if isStorage && len(node.Taints) == 0 {
			return fmt.Errorf("e2e topology storage node %q must carry the storage taint contract", node.Name)
		}
	}
	for role, seen := range wantRoles {
		if !seen {
			return fmt.Errorf("e2e topology contract missing node %q", role)
		}
	}
	return nil
}

func validateCAPIReadiness(readiness []string) error {
	required := []string{
		"infrastructure-ready",
		"control-plane-ready",
		"workers-ready",
		"kubeconfig-secret-ready",
		"cluster-mutation-ready",
	}
	last := -1
	for _, step := range required {
		idx := indexOfString(readiness, step)
		if idx == -1 {
			return fmt.Errorf("e2e topology contract missing CAPI readiness step %q", step)
		}
		if idx <= last {
			return fmt.Errorf("e2e topology CAPI readiness step %q must come after %q", step, readiness[last])
		}
		last = idx
	}
	return nil
}

func indexOfString(values []string, want string) int {
	for i, value := range values {
		if value == want {
			return i
		}
	}
	return -1
}

func addressHost(address string) string {
	if host, _, ok := strings.Cut(address, "/"); ok {
		return host
	}
	return address
}

func wrap(what string, err error) error {
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	return nil
}

// Image builds and pushes release artifacts: the driver OCI container image and
// the packaged Helm chart. These targets are the only image build/push concern
// in the repository — cluster orchestration and the test harness live in
// test/e2e. They are explicitly invoked and mutating (they push to a registry),
// so they are never wired into the non-mutating check lanes.
type Image mg.Namespace

const (
	defaultImageRepo   = "docker.io/randomvariable/zfs-csi"
	defaultChartOCI    = "oci://docker.io/randomvariable"
	defaultImageTag    = "dev"
	imageRepoEnvName   = "ZFS_CSI_IMAGE_REPO"
	imageTagEnvName    = "ZFS_CSI_IMAGE_TAG"
	chartOCIEnvName    = "ZFS_CSI_CHART_OCI"
	chartName          = "zfs-csi"
	chartBaseVersion   = "0.1.0"
	chartRelativeDir   = "charts/zfs-csi"
	imageBuildPlatform = "linux/amd64"

	// Golden node image publishing. The image-builder kubevirt post-processor
	// produces a local containerdisk OCI image named "<build_name>-container-disk";
	// Image.Golden retags+pushes it to Harbor and publishes a CDI DataSource that
	// CAPK clones per-run root PVCs from. The golden image is built once per
	// Kubernetes minor release, so this target is run rarely and out of band.
	goldenBuildName       = "ubuntu-2404"
	goldenArtifactName    = "ubuntu-2404-kube-v1.36.2"
	goldenLocalContainer  = goldenBuildName + "-container-disk"
	goldenImagesNamespace = "zfs-csi-e2e-images"
	e2eNamespace          = "zfs-csi-e2e-images"
	defaultGoldenRepo     = "localhost:5000/zfs-csi/ubuntu-2404-kube"
	// cephfs (RWX), not RBD: the golden PVC is the shared read-only backing
	// store for every node VM's ephemeral root overlay. All VMs mount it
	// concurrently read-only, which requires ReadWriteMany — RBD RWO cannot be
	// shared across virt-launcher pods. ceph-filesystem-e2e is the dedicated
	// E2E class (replicated data0 pool, mounter: kernel, reclaimPolicy: Delete)
	// so the golden PVC self-cleans and the concurrent boot reads use the fast
	// in-kernel cephfs client.
	defaultGoldenStorageSC = "ceph-filesystem-e2e"
	goldenRepoEnvName      = "ZFS_CSI_GOLDEN_REPO"
	goldenStorageEnvName   = "ZFS_CSI_GOLDEN_STORAGE_CLASS"
	goldenSizeEnvName      = "ZFS_CSI_GOLDEN_SIZE"
	// 50Gi, not 40Gi: the containerdisk's virtual image is 40 GiB, and a
	// Filesystem-mode PVC loses ~1Gi to filesystem overhead, so a 40Gi PVC
	// reports only ~39Gi available and CDI's convert step fails with "virtual
	// image size ... larger than the reported available storage".
	defaultGoldenSize = "50Gi"
)

// imageTag returns the run tag from ZFS_CSI_IMAGE_TAG, defaulting to "dev" for
// local pre-push runs. CI passes a unique tag (git SHA / run id).
func imageTag() string {
	return envDefaultString(imageTagEnvName, defaultImageTag)
}

func driverImageRef() string {
	return envDefaultString(imageRepoEnvName, defaultImageRepo) + ":" + imageTag()
}

// sanitizeSemverMetadata coerces an arbitrary tag into a valid SemVer build
// metadata identifier (only [0-9A-Za-z-] and dot separators are allowed), so it
// can be appended after "+" in a chart version.
func sanitizeSemverMetadata(tag string) string {
	var b strings.Builder
	for _, r := range tag {
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '-', r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	sanitized := strings.Trim(b.String(), ".")
	if sanitized == "" {
		return defaultImageTag
	}
	return sanitized
}

// Driver builds the driver OCI image with buildx and pushes it to the registry.
// The tag comes from ZFS_CSI_IMAGE_TAG (default "dev"); the repository from
// ZFS_CSI_IMAGE_REPO. The resulting mutable image reference is printed as
// E2E_DRIVER_IMAGE.
func (Image) Driver(ctx context.Context) error {
	ref := driverImageRef()

	if _, err := magetools.RunBinary(ctx, "docker", []string{
		"buildx", "build",
		"--platform", imageBuildPlatform,
		"-t", ref,
		"--push",
		".",
	}, magetools.WithStdout()); err != nil {
		return err
	}
	fmt.Printf("E2E_DRIVER_IMAGE=%s\n", ref)
	return nil
}

// Chart packages the Helm chart, stamped with the same tag as the driver image,
// and pushes it to the Harbor OCI registry (ZFS_CSI_CHART_OCI).
func (Image) Chart(ctx context.Context) error {
	tag := imageTag()
	ociRepo := envDefaultString(chartOCIEnvName, defaultChartOCI)
	version := chartBaseVersion + "+" + sanitizeSemverMetadata(tag)

	destDir, err := filepath.Abs(filepath.Join("test", "e2e", "_artifacts", "charts"))
	if err != nil {
		return fmt.Errorf("resolve chart output dir: %w", err)
	}
	if err := os.MkdirAll(destDir, 0o750); err != nil {
		return fmt.Errorf("create chart output dir: %w", err)
	}
	if _, err := magetools.RunBinary(ctx, "helm", []string{
		"package", chartRelativeDir,
		"--version", version,
		"--app-version", tag,
		"--destination", destDir,
	}, magetools.WithStdout()); err != nil {
		return err
	}
	packaged := filepath.Join(destDir, chartName+"-"+version+".tgz")
	if _, err := magetools.RunBinary(ctx, "helm", []string{
		"push", packaged, ociRepo,
	}, magetools.WithStdout()); err != nil {
		return err
	}
	fmt.Printf("chart pushed: %s/%s:%s\n", ociRepo, chartName, version)
	return nil
}

// All builds+pushes the driver image and then the chart.
func (Image) All(ctx context.Context) error {
	if err := (Image{}).Driver(ctx); err != nil {
		return err
	}
	return (Image{}).Chart(ctx)
}

// Golden publishes the golden node image (produced by e2e:imageBuild) to the
// cluster as a CDI DataSource that CAPK clones per-run root PVCs from. It is
// run once per Kubernetes minor release, out of band from PR CI. Steps:
//
//  1. retag the local containerdisk image "<build_name>-container-disk" to the
//     Harbor golden repo and push it;
//  2. delete any prior golden DataSource/DataVolume/PVC (they are immutable, so
//     a spec change — accessMode, storage class — requires recreate);
//  3. apply a CDI DataVolume that imports the containerdisk from Harbor into a
//     PVC in the images namespace (source.registry, pullMethod=node);
//  4. apply a CDI DataSource pointing at that PVC.
//
// Requires kubectl + docker with access to Harbor and the management cluster.
func (Image) Golden(ctx context.Context) error {
	goldenRepo := envDefaultString(goldenRepoEnvName, defaultGoldenRepo)
	tag := imageTag()
	ref := goldenRepo + ":" + tag

	if _, err := magetools.RunBinary(ctx, "docker", []string{"tag", goldenLocalContainer, ref}, magetools.WithStdout()); err != nil {
		return wrap("image:golden tag containerdisk (run e2e:imageBuild first)", err)
	}
	if _, err := magetools.RunBinary(ctx, "docker", []string{"push", ref}, magetools.WithStdout()); err != nil {
		return wrap("image:golden push containerdisk", err)
	}

	if err := deleteGoldenArtifacts(ctx); err != nil {
		return err
	}

	manifest, err := renderGoldenCDIManifest(ref)
	if err != nil {
		return err
	}
	if err := kubectlApplyManifest(ctx, "golden-cdi.yaml", manifest); err != nil {
		return wrap("image:golden apply CDI DataVolume/DataSource", err)
	}
	fmt.Printf("golden image published: DataSource %s/%s (from %s)\n", goldenImagesNamespace, goldenArtifactName, ref)
	return nil
}

// renderGoldenCDIManifest builds the CDI DataVolume + DataSource YAML that
// imports the pushed containerdisk into a golden PVC and exposes it as a
// clonable DataSource.
func renderGoldenCDIManifest(registryRef string) (string, error) {
	storageClass := envDefaultString(goldenStorageEnvName, defaultGoldenStorageSC)
	size := envDefaultString(goldenSizeEnvName, defaultGoldenSize)
	data := map[string]string{
		"Name":         goldenArtifactName,
		"Namespace":    goldenImagesNamespace,
		"RegistryURL":  "docker://" + registryRef,
		"StorageClass": storageClass,
		"Size":         size,
	}
	tmpl := template.Must(template.New("golden-cdi").Parse(goldenCDITemplate))
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("render golden CDI manifest: %w", err)
	}
	return buf.String(), nil
}

// deleteGoldenArtifacts removes any prior golden DataSource, DataVolume, and
// backing PVC so image:golden is idempotent across spec changes. CDI
// DataVolumes and PVCs are immutable, so republishing with a different
// accessMode or storage class (e.g. RBD RWO -> cephfs RWX) fails on apply
// unless the old objects are torn down first.
//
// Delete order matters: DataSource first (it only references the PVC, deleting
// it prevents nothing from recreating), then the DataVolume (CDI's PVC GC is
// owned by the DataVolume), then the PVC explicitly with a wait — CDI usually
// garbage-collects the PVC when the DataVolume is deleted, but the populator
// can lag, and a leftover PVC of the wrong accessMode/class would block the
// fresh import. --ignore-not-found makes the first-ever run a clean no-op.
func deleteGoldenArtifacts(ctx context.Context) error {
	ns := goldenImagesNamespace
	name := goldenArtifactName
	for _, kind := range []string{"datasource.cdi.kubevirt.io", "datavolume.cdi.kubevirt.io"} {
		if err := kubectlStream(ctx, "delete", kind, name, "-n", ns, "--ignore-not-found"); err != nil {
			return wrap(fmt.Sprintf("image:golden delete prior %s/%s", kind, name), err)
		}
	}
	// Wait for the DataVolume's PVC GC, then force-delete any straggler.
	if err := kubectlStream(ctx, "delete", "pvc", name, "-n", ns, "--ignore-not-found", "--wait=true"); err != nil {
		return wrap(fmt.Sprintf("image:golden delete prior pvc/%s", name), err)
	}
	return nil
}

// kubectlApplyManifest writes the manifest to a gitignored artifacts file and
// applies it, leaving the rendered YAML on disk for inspection.
func kubectlApplyManifest(ctx context.Context, name, manifest string) error {
	dir := filepath.Join("test", "e2e", "_artifacts")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("create artifacts dir: %w", err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(manifest), 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	_, err := magetools.RunBinary(ctx, "kubectl", []string{"apply", "-f", path}, magetools.WithStdout())
	return err
}

const goldenCDITemplate = `apiVersion: cdi.kubevirt.io/v1beta1
kind: DataVolume
metadata:
  name: {{ .Name }}
  namespace: {{ .Namespace }}
  labels:
    app.kubernetes.io/name: zfs-csi-e2e-images
    app.kubernetes.io/part-of: zfs-csi-e2e
    app.kubernetes.io/component: golden-node-image
  annotations:
    cdi.kubevirt.io/storage.usePopulator: "true"
spec:
  source:
    registry:
      url: "{{ .RegistryURL }}"
      pullMethod: node
  pvc:
    accessModes:
      # RWX: the golden PVC is the shared read-only backing store for every node
      # VM's ephemeral root overlay, so all virt-launcher pods mount it at once.
      # cephfs supports RWX; CDI imports the containerdisk once, then the VMs
      # only read.
      - ReadWriteMany
    # Filesystem, not Block: CDI's importer needs raw block access for Block
    # volumeMode, which crash-loops on this cluster's Ceph with
    # "blockdev: cannot open /dev/cdi-block-volume: Permission denied". The
    # proven-working ISO DataVolume on the same Ceph uses Filesystem; CDI writes
    # disk.img to the mounted FS and KubeVirt boots it as a file-backed root.
    volumeMode: Filesystem
    resources:
      requests:
        storage: {{ .Size }}
    storageClassName: {{ .StorageClass }}
---
apiVersion: cdi.kubevirt.io/v1beta1
kind: DataSource
metadata:
  name: {{ .Name }}
  namespace: {{ .Namespace }}
  labels:
    app.kubernetes.io/name: zfs-csi-e2e-images
    app.kubernetes.io/part-of: zfs-csi-e2e
    app.kubernetes.io/component: golden-node-image
spec:
  source:
    pvc:
      name: {{ .Name }}
      namespace: {{ .Namespace }}
`

func runE2ETest(ctx context.Context, extraArgs ...string) error {
	repoRoot, err := filepath.Abs(".")
	if err != nil {
		return fmt.Errorf("resolve repo root: %w", err)
	}
	if err := e2econfig.Init(); err != nil {
		return fmt.Errorf("init e2e config: %w", err)
	}
	runID, fromState, err := resolveE2ERunID()
	if err != nil {
		return err
	}
	if !fromState {
		if werr := writeE2EState(e2eRunState{
			RunID:     runID,
			Namespace: e2eNamespaceFor(runID),
			CreatedAt: time.Now().UTC().Format(time.RFC3339),
		}); werr != nil {
			fmt.Fprintf(os.Stderr, "warning: could not persist e2e run state: %v\n", werr)
		}
	}
	if err := os.Setenv(e2econfig.Env[e2econfig.RunIDKey], runID); err != nil {
		return fmt.Errorf("set run id env: %w", err)
	}
	ns := e2eNamespaceFor(runID)

	// Tee all runner output to a per-run log under _artifacts so background or
	// detached invocations (e.g. `mage e2e:aws` launched non-interactively)
	// leave an inspectable transcript instead of only a live pipe. The file is
	// gitignored with the rest of _artifacts.
	logDir := filepath.Join(repoRoot, "test", "e2e", "_artifacts", runID)
	if err := os.MkdirAll(logDir, 0o750); err != nil {
		return fmt.Errorf("create artifacts dir: %w", err)
	}
	logPath := filepath.Join(logDir, "e2e-process.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		return fmt.Errorf("create e2e log %s: %w", logPath, err)
	}
	artifactLog := &ansiLogWriter{dst: logFile}
	defer func() {
		_ = artifactLog.Flush()
		_ = logFile.Close()
	}()
	stdout := io.MultiWriter(os.Stdout, artifactLog)
	stderr := io.MultiWriter(os.Stderr, artifactLog)

	fmt.Fprintf(stdout, "[e2e] run=%s cluster=r%s namespace=%s\n", runID, runID, ns)
	fmt.Fprintf(stdout, "[e2e] driver=%s chart=%s\n", envDefaultString("E2E_DRIVER_IMAGE", "(default)"), envDefaultString("E2E_CHART_REF", "(default)"))
	fmt.Fprintf(stderr, "[e2e] logging to %s\n", logPath)

	// Build the test binary first (compilation is silent and slow), then run
	// it directly so stdout/stderr stream without go test's output buffering.
	binaryPath := filepath.Join(repoRoot, "test", "e2e", "e2e.test")
	buildCmd := exec.CommandContext(ctx, "go", "test", "-c", "-tags=e2e", "-o", binaryPath, "./test/e2e")
	buildCmd.Dir = repoRoot
	buildCmd.Stdout = stdout
	buildCmd.Stderr = stderr
	fmt.Fprintf(stderr, "[e2e] compiling test binary...\n")
	if err := buildCmd.Run(); err != nil {
		return fmt.Errorf("go test -c: %w", err)
	}
	defer os.Remove(binaryPath)

	// Run the compiled test binary directly — no go test wrapper, so
	// Ginkgo's output streams immediately to stdout/stderr.
	// THREE timeouts must all exceed the worst-case full run (provision ~15m +
	// smokes ~5m + conformance ceiling conformanceSuiteTimeout 360m across four
	// drivers), or the tightest one kills the run:
	//   1. -test.timeout       : the Go test binary's own timeout.
	//   2. -ginkgo.timeout     : the OUTER ginkgo suite timeout. Ginkgo defaults
	//      this to 1h — without setting it, the conformance It is killed at
	//      exactly 3600s ("Suite Timeout Elapsed") regardless of the other two.
	//   3. conformanceSuiteTimeout : the INNER ginkgo --timeout inside the
	//      conformance container (set in conformance.go).
	// 390m on the outer two also covers setup, smokes, and cleanup.
	args := []string{"-test.v", "-test.timeout", "390m", "-ginkgo.timeout", "390m"}
	args = append(args, extraArgs...)

	fmt.Fprintf(stderr, "[e2e] running: e2e.test %s\n\n", strings.Join(args, " "))

	cmd := exec.CommandContext(ctx, binaryPath, args...)
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(), e2econfig.ChildEnv()...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.Stdin = os.Stdin
	stopLogs := startPodLogCapture(ctx, runID, logDir, stderr)
	defer stopLogs()
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("e2e.test: %w", err)
	}
	return nil
}

// ansiLogWriter removes complete ANSI CSI sequences only from artifact logs.
// It preserves incomplete or invalid sequences verbatim rather than losing text.
type ansiLogWriter struct {
	mu      sync.Mutex
	dst     io.Writer
	pending []byte
	state   ansiLogState
}

type ansiLogState uint8

const (
	ansiLogText ansiLogState = iota
	ansiLogEscape
	ansiLogCSI
)

func (w *ansiLogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	out := make([]byte, 0, len(p))
	for _, b := range p {
		switch w.state {
		case ansiLogText:
			if b == '\x1b' {
				w.pending = append(w.pending, b)
				w.state = ansiLogEscape
			} else {
				out = append(out, b)
			}

		case ansiLogEscape:
			if b == '[' {
				w.pending = append(w.pending, b)
				w.state = ansiLogCSI
				continue
			}
			out = append(out, w.pending...)
			w.pending = w.pending[:0]
			w.state = ansiLogText
			if b == '\x1b' {
				w.pending = append(w.pending, b)
				w.state = ansiLogEscape
			} else {
				out = append(out, b)
			}

		case ansiLogCSI:
			w.pending = append(w.pending, b)
			if b >= 0x40 && b <= 0x7e {
				// CSI final bytes terminate color and terminal-control sequences.
				w.pending = w.pending[:0]
				w.state = ansiLogText
			} else if b < 0x20 || b > 0x3f {
				// This is not a CSI sequence; keep its bytes untouched in the log.
				out = append(out, w.pending...)
				w.pending = w.pending[:0]
				w.state = ansiLogText
			}
		}
	}

	if len(out) == 0 {
		return len(p), nil
	}
	if err := writeAll(w.dst, out); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Flush writes a partial sequence verbatim before the artifact file closes.
func (w *ansiLogWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.pending) == 0 {
		return nil
	}
	if err := writeAll(w.dst, w.pending); err != nil {
		return err
	}
	w.pending = w.pending[:0]
	w.state = ansiLogText
	return nil
}

func writeAll(dst io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := dst.Write(p)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}

// startPodLogCapture streams workload-cluster pod logs into one artifact per
// namespace. The workload kubeconfig is created by the E2E run itself, so the
// capture goroutine waits for it instead of delaying or failing the test run.
func startPodLogCapture(ctx context.Context, runID, logDir string, out io.Writer) (stop func()) {
	captureCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)
		defer cancel()

		if _, err := exec.LookPath("stern"); err != nil {
			fmt.Fprintf(out, "warning: pod log capture disabled: stern is not available: %v\n", err)
			return
		}
		runNamespace, err := resolveE2ERunNamespace(captureCtx)
		if err != nil {
			fmt.Fprintf(out, "warning: pod log capture disabled: resolve run namespace: %v\n", err)
			return
		}

		fmt.Fprintf(out, "[e2e] waiting for workload kubeconfig before capturing pod logs\n")
		var kubeconfig string
		for {
			raw, err := kubectlOut(captureCtx, "get", "secret", "r"+runID+"-kubeconfig", "-n", runNamespace, "-o", "jsonpath={.data.value}")
			if err == nil {
				kubeconfig, err = base64StdDecode(strings.TrimSpace(raw))
				if err == nil && kubeconfig != "" {
					break
				}
			}
			select {
			case <-captureCtx.Done():
				return
			case <-time.After(5 * time.Second):
			}
		}

		kubeconfigFile, err := os.CreateTemp(logDir, ".e2e-workload-kubeconfig-*")
		if err != nil {
			fmt.Fprintf(out, "warning: pod log capture disabled: create workload kubeconfig: %v\n", err)
			return
		}
		kubeconfigPath := kubeconfigFile.Name()
		defer os.Remove(kubeconfigPath)
		if err := kubeconfigFile.Chmod(0o600); err != nil {
			_ = kubeconfigFile.Close()
			fmt.Fprintf(out, "warning: pod log capture disabled: secure workload kubeconfig: %v\n", err)
			return
		}
		if _, err := kubeconfigFile.WriteString(kubeconfig); err != nil {
			_ = kubeconfigFile.Close()
			fmt.Fprintf(out, "warning: pod log capture disabled: write workload kubeconfig: %v\n", err)
			return
		}
		if err := kubeconfigFile.Close(); err != nil {
			fmt.Fprintf(out, "warning: pod log capture disabled: close workload kubeconfig: %v\n", err)
			return
		}

		var captures []*podLogCaptureProcess
		defer func() {
			for _, capture := range captures {
				capture.Stop()
			}
		}()

		for _, namespace := range []string{"zfs-csi", "kube-system", "zfs-csi-import-" + runID} {
			capture, err := startPodLogCaptureProcess(captureCtx, logDir, runID, kubeconfigPath, namespace, podLogCaptureCommand, (*exec.Cmd).Start)
			if err != nil {
				fmt.Fprintf(out, "warning: pod log capture disabled for %s: %v\n", namespace, err)
				continue
			}
			captures = append(captures, capture)
			warnOnUnexpectedPodLogCaptureExit(captureCtx, out, capture)
			fmt.Fprintf(out, "[e2e] capturing %s pod logs to %s\n", namespace, capture.artifactPath)
		}

		<-captureCtx.Done()
	}()

	return func() {
		cancel()
		<-done
	}
}

type podLogCaptureCommandFactory func(context.Context, string, string, io.Writer) *exec.Cmd

// podLogCaptureProcess owns one stern command, its artifact, and the only Wait call.
type podLogCaptureProcess struct {
	namespace    string
	artifactPath string
	file         *os.File
	cmd          *exec.Cmd
	done         chan struct{}
	waitErr      error
}

func startPodLogCaptureProcess(ctx context.Context, logDir, runID, kubeconfigPath, namespace string, command podLogCaptureCommandFactory, start func(*exec.Cmd) error) (*podLogCaptureProcess, error) {
	path := filepath.Join(logDir, fmt.Sprintf("kubernetes-live-%s.log", namespace))
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", path, err)
	}

	capture := &podLogCaptureProcess{
		namespace:    namespace,
		artifactPath: path,
		file:         file,
		done:         make(chan struct{}),
	}
	capture.cmd = command(ctx, kubeconfigPath, namespace, file)
	if err := start(capture.cmd); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("start stern (artifact %s): %w", path, err)
	}

	go func() {
		capture.waitErr = capture.cmd.Wait()
		close(capture.done)
	}()
	return capture, nil
}

// Stop waits for the dedicated waiter rather than calling Wait a second time.
func (c *podLogCaptureProcess) Stop() {
	if c.cmd.Process != nil {
		_ = c.cmd.Process.Kill()
	}
	<-c.done
	_ = c.file.Close()
}

func warnOnUnexpectedPodLogCaptureExit(ctx context.Context, out io.Writer, capture *podLogCaptureProcess) {
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-capture.done:
			// Context cancellation intentionally terminates stern during teardown.
			if ctx.Err() != nil {
				return
			}
			fmt.Fprintf(out, "warning: pod log capture for %s exited before capture stopped; inspect %s: %v\n", capture.namespace, capture.artifactPath, capture.waitErr)
		}
	}()
}

// podLogCaptureCommand keeps all stern output in its namespace-specific artifact.
func podLogCaptureCommand(ctx context.Context, kubeconfigPath, namespace string, artifact io.Writer) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "stern", ".*", "--kubeconfig", kubeconfigPath,
		"--namespace", namespace, "--tail", "0", "--color", "never", "--timestamps=short")
	cmd.Stdout = artifact
	cmd.Stderr = artifact
	return cmd
}

func uniqueE2ERunID() string {
	return "e2e-" + time.Now().UTC().Format("20060102150405") + "-" + strconv.Itoa(os.Getpid())
}

func envDefaultString(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envDefaultInt(key string, fallback int) int {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		if n, err := strconv.Atoi(value); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

// qemuGuestPID returns the PID of the image-builder QEMU guest, or 0 if none is
// running. It matches the ubuntu-2404 build VM specifically.
func qemuGuestPID() int {
	out, err := exec.Command("pgrep", "-f", "qemu-system-x86_64.*ubuntu-2404").Output()
	if err != nil {
		return 0
	}
	for _, line := range strings.Fields(string(out)) {
		if pid, err := strconv.Atoi(line); err == nil {
			return pid
		}
	}
	return 0
}

// ansiblePlaybookRunning reports whether an ansible-playbook process is active.
// While one is, the build is in an ansible provisioner (node.yml/sysprep/etc.),
// not the reboot shell provisioner, so the watchdog must not sever.
func ansiblePlaybookRunning() bool {
	return exec.Command("pgrep", "-f", "ansible-playbook").Run() == nil
}

// qemuSSHForwardPort parses the guest's SLIRP hostfwd SSH port (hostfwd=tcp::PORT-:22)
// from the QEMU command line, or "".
func qemuSSHForwardPort(pid int) string {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cmdline"))
	if err != nil {
		return ""
	}
	cmdline := strings.ReplaceAll(string(data), "\x00", " ")
	m := regexp.MustCompile(`hostfwd=tcp::(\d+)-:22`).FindStringSubmatch(cmdline)
	if len(m) != 2 {
		return ""
	}
	return m[1]
}

// severStalledPackerConns issues `sudo ss -K` against packer-owned SSH
// connections to 127.0.0.1:<port> that have been idle longer than idle,
// returning how many it cleared. It never targets non-packer connections (e.g.
// a developer's own ssh), so a false positive cannot kill an interactive
// session. It relies on `ss -tnpi` idle timers (lastsnd/lastrcv, milliseconds).
func severStalledPackerConns(port string, idle time.Duration) int {
	out, err := exec.Command("ss", "-tnpi").Output()
	if err != nil {
		return 0
	}
	dst := "127.0.0.1:" + port
	idleMS := idle.Milliseconds()
	lines := strings.Split(string(out), "\n")
	cleared := 0
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		if !strings.Contains(line, dst) || !strings.Contains(line, "packer") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		local := fields[3] // src 127.0.0.1:PORT (the packer side)
		if !strings.HasPrefix(local, "127.0.0.1:") || strings.HasSuffix(local, ":"+port) {
			continue // skip the qemu-side half of the pair
		}
		// The -i detail line follows; require both send+recv idle > threshold.
		detail := ""
		if i+1 < len(lines) {
			detail = lines[i+1]
		}
		if !bothDirectionsIdle(detail, idleMS) {
			continue
		}
		src := strings.TrimPrefix(local, "127.0.0.1:")
		if err := exec.Command("sudo", "-n", "ss", "-K", "src", "127.0.0.1:"+src, "dst", dst).Run(); err == nil {
			cleared++
		}
	}
	return cleared
}

// bothDirectionsIdle reports whether the ss -i detail line shows both lastsnd
// and lastrcv older than idleMS milliseconds.
func bothDirectionsIdle(detail string, idleMS int64) bool {
	snd := ssIdleField(detail, "lastsnd:")
	rcv := ssIdleField(detail, "lastrcv:")
	return snd >= idleMS && rcv >= idleMS
}

func ssIdleField(detail, key string) int64 {
	idx := strings.Index(detail, key)
	if idx < 0 {
		return 0
	}
	rest := detail[idx+len(key):]
	end := strings.IndexAny(rest, " \t")
	if end >= 0 {
		rest = rest[:end]
	}
	n, err := strconv.ParseInt(rest, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// --- E2E run state (pins the active run across mage invocations) -------------

// e2eRunState is the persisted handle to the active E2E run. It lives at
// test/e2e/_artifacts/e2e-run.json (gitignored). Wiping the _artifacts dir, or
// `rm test/e2e/_artifacts/e2e-run.json`, resets the pinned run so the next
// `e2e:up` generates a fresh one.
type e2eRunState struct {
	RunID     string `json:"runId"`
	Namespace string `json:"namespace"`
	CreatedAt string `json:"createdAt"`
}

func e2eStatePath() string {
	return filepath.Join("test", "e2e", "_artifacts", "e2e-run.json")
}

// e2eNamespaceFor returns the shared E2E namespace. All clusters, golden
// DataSources, and NADs live here. Multiple runs coexist, differentiated
// by cluster name.
func e2eNamespaceFor(runID string) string {
	return e2eNamespace
}

func readE2EState() (e2eRunState, error) {
	var st e2eRunState
	data, err := os.ReadFile(e2eStatePath())
	if err != nil {
		return st, err
	}
	if err := json.Unmarshal(data, &st); err != nil {
		return st, fmt.Errorf("parse %s: %w", e2eStatePath(), err)
	}
	return st, nil
}

func writeE2EState(st e2eRunState) error {
	dir := filepath.Dir(e2eStatePath())
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("create artifacts dir: %w", err)
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal e2e state: %w", err)
	}
	return os.WriteFile(e2eStatePath(), data, 0o600)
}

func clearE2EState() {
	_ = os.Remove(e2eStatePath())
}

// resolveE2ERunID picks the run ID to use, preferring an explicit override
// (--e2e-run-id flag / E2E_RUN_ID env via viper), then the pinned state file
// (so re-invocations reuse the same run), else generating a fresh one. The bool
// reports whether the value came from state.
func resolveE2ERunID() (string, bool, error) {
	if runID := strings.TrimSpace(viper.GetString(e2econfig.RunIDKey)); runID != "" {
		return runID, false, nil
	}
	if st, err := readE2EState(); err == nil && st.RunID != "" {
		return st.RunID, true, nil
	}
	return uniqueE2ERunID(), false, nil
}

// resolveE2ERunNamespace returns the active run's namespace. With single-namespace
// consolidation, this is always e2eNamespace ("zfs-csi-e2e-images").
func resolveE2ERunNamespace(ctx context.Context) (string, error) {
	return e2eNamespace, nil
}

// listE2EClusters lists clusters in the shared e2e namespace.
func listE2EClusters(ctx context.Context) ([]string, error) {
	out, err := kubectlOut(ctx, "get", "clusters", "-n", e2eNamespace,
		"-o", "jsonpath={.items[*].metadata.name}")
	if err != nil {
		return nil, err
	}
	var clusters []string
	for _, c := range strings.Fields(out) {
		clusters = append(clusters, c)
	}
	sort.Strings(clusters)
	return clusters, nil
}

// --- kubectl helpers ----------------------------------------------------------

// kubectlOut runs kubectl and returns its stdout as a trimmed string.
func kubectlOut(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("kubectl %s: %w", strings.Join(args, " "), err)
	}
	return stdout.String(), nil
}

// kubectlStream runs kubectl with stdout/stderr inherited (table output, etc.).
func kubectlStream(ctx context.Context, args ...string) error {
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("kubectl %s: %w", strings.Join(args, " "), err)
	}
	return nil
}

// resolveVMIDNS finds a VMI in ns whose name contains selector (substring),
// returning the VMI name and its first interface pod IP. The selector
// "control-plane" / "cp" matches the control-plane VMI; "storage" the storage
// node; an md-0 substring narrows to a worker. If multiple match, the first is
// returned.
func resolveVMIDNS(ctx context.Context, ns, selector string) (string, string, error) {
	items, err := kubectlOut(ctx, "get", "vmi", "-n", ns,
		"-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\t\"}{.status.interfaces[0].ipAddress}{\"\\n\"}{end}")
	if err != nil {
		return "", "", wrap("list vmis", err)
	}
	for _, line := range strings.Split(items, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "\t", 2)
		name := parts[0]
		if !strings.Contains(name, selector) {
			continue
		}
		ip := ""
		if len(parts) == 2 {
			ip = strings.TrimSpace(parts[1])
		}
		if ip == "" {
			return "", "", fmt.Errorf("vmi %s has no pod IP yet (still booting?)", name)
		}
		return name, ip, nil
	}
	return "", "", fmt.Errorf("no vmi matching %q in %s", selector, ns)
}

// base64StdDecode decodes a standard base64 string.
func base64StdDecode(s string) (string, error) {
	if s == "" {
		return "", nil
	}
	data, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// rewriteKubeconfigServer is retained for reference but no longer used: with
// controlPlaneServiceTemplate: type=LoadBalancer, the secret's server address
// is already the LB external IP (in the apiserver cert SANs), so rewriting it
// to the service ClusterIP would break TLS verification.
func rewriteKubeconfigServer(kubeconfig, ip string) string {
	if ip == "" {
		return kubeconfig
	}
	re := regexp.MustCompile(`(server:\s*https?://)[^:/]+(:\d+)?`)
	return re.ReplaceAllString(kubeconfig, "${1}"+ip+"${2}")
}
