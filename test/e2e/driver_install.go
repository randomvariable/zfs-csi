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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	kjson "k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	nvmetv1 "github.com/randomvariable/zfs-csi/api/nvmet/v1alpha1"
	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/test/e2e/e2econfig"
)

type helmStorageOwner struct {
	Name                   string              `json:"name"`
	Enabled                bool                `json:"enabled"`
	NodeSelector           map[string]string   `json:"nodeSelector"`
	Tolerations            []corev1.Toleration `json:"tolerations"`
	AuthoritativePoolGUIDs []string            `json:"authoritativePoolGUIDs"`
	PoolMountRoot          string              `json:"poolMountRoot"`
	NFS                    helmEndpoint        `json:"nfs"`
	NVMe                   helmEndpoint        `json:"nvme"`
	NetworkDomain          string              `json:"networkDomain"`
	ReachableFrom          []string            `json:"reachableFrom"`
}

type helmEndpoint struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

func helmStorageOwnerValues(owners []storageOwner) ([]helmStorageOwner, error) {
	if len(owners) == 0 {
		return nil, fmt.Errorf("at least one resolved storage owner is required")
	}
	values := make([]helmStorageOwner, 0, len(owners))
	seenGUIDs := map[string]string{}
	for _, owner := range owners {
		if owner.Name == "" || owner.Node.Name == "" || owner.PoolGUID == "" || owner.DataDeviceID == "" {
			return nil, fmt.Errorf("storage owner %q lacks resolved node, pool GUID, or device identity", owner.Name)
		}
		if previous := seenGUIDs[owner.PoolGUID]; previous != "" {
			return nil, fmt.Errorf("pool GUID %q is shared by storage owners %q and %q", owner.PoolGUID, previous, owner.Name)
		}
		seenGUIDs[owner.PoolGUID] = owner.Name
		values = append(values, helmStorageOwner{
			Name:                   owner.Node.Name,
			Enabled:                true,
			NodeSelector:           owner.NodeSelector,
			Tolerations:            storageOwnerTolerations(),
			AuthoritativePoolGUIDs: []string{owner.PoolGUID},
			PoolMountRoot:          "/" + strings.TrimPrefix(owner.PoolName, "/"),
			NFS:                    helmEndpoint{Host: owner.Node.NFSServer, Port: owner.NFSPort},
			NVMe:                   helmEndpoint{Host: owner.Node.PortalHost, Port: owner.NVMePort},
			NetworkDomain:          owner.NetworkDomain,
			ReachableFrom:          append([]string(nil), owner.ReachableFrom...),
		})
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Name < values[j].Name })
	return values, nil
}

// storageOwnerTolerations returns the tolerations rendered for each storage
// owner Deployment. Every lane tolerates the canonical storage taint; the
// static lane additionally tolerates the site's non-blocking taints (shared
// clusters taint storage-role nodes, e.g. a NAS role) so the storage agent
// can schedule onto the owner node.
func storageOwnerTolerations() []corev1.Toleration {
	tolerations := []corev1.Toleration{storageNodeToleration()}
	if e2econfig.InfrastructureProvider() != "static" {
		return tolerations
	}
	seen := map[string]struct{}{tolerations[0].Key: {}}
	for _, key := range strings.Split(e2econfig.NonBlockingTaints(), ",") {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		tolerations = append(tolerations, corev1.Toleration{
			Key:      key,
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoSchedule,
		})
	}
	return tolerations
}

func writeHelmStorageOwnerValues(path string, owners []storageOwner) error {
	values, err := multiOwnerHelmValues(owners)
	if err != nil {
		return err
	}
	body, err := yaml.Marshal(values)
	if err != nil {
		return fmt.Errorf("marshal storage owner Helm values: %w", err)
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		return fmt.Errorf("write storage owner Helm values %q: %w", path, err)
	}
	return nil
}

func multiOwnerHelmValues(owners []storageOwner) (map[string]any, error) {
	values, err := helmStorageOwnerValues(owners)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"controller":    map[string]any{"enabled": true, "replicas": 2},
		"storage":       map[string]any{"enabled": true, "enableVolumeImports": true},
		"node":          map[string]any{"enabled": true, "networkDomainSource": "nodeLabel"},
		"storageOwners": values,
	}, nil
}

// installDriverFromChart deploys the driver into the workload cluster with a
// real `helm upgrade --install` of the chart reference (a local path for local
// runs, or the pushed OCI chart in CI). The storage node's identity and
// addresses come from the discovered CAPK storage node; storageNode.selector
// keeps its default canonical label so the storage-owning pods land on the one
// labelled+tainted storage node.
func installDriverFromChart(ctx context.Context, kubeconfig, chartRef, image string, node storageNode, extraOverrides ...map[string]string) error {
	repository, tag, digest, err := driverImageHelmValues(image)
	if err != nil {
		return err
	}
	// On the ECR lane the pods cannot pull until the pull secret is minted (done
	// after helm returns), so helm --wait would time out on ImagePullBackOff.
	// Skip helm's wait there and gate readiness on kubectl rollout status after
	// the secret is attached. Non-ECR (Harbor) keeps helm --wait as before.
	isECR := ecrRegistryRe.MatchString(repository)

	// The pool name is a single knob (E2E_ZPOOL, default "tank") that must be
	// consistent across three places or provisioning targets a non-existent pool:
	// (1) the pool-create pod runs `zpool create <pool>`, (2) the chart's
	// StorageClasses carry `pool: <pool>`, and (3) poolMountRoot is the pool's
	// default mountpoint /<pool> (the storage agent bind-mounts it for host-nfsd
	// visibility). PoolName() drives all three. The StorageClass *names*
	// (zfs-tank-nvme, ...) stay fixed — they are identifiers the smokes and
	// testdrivers reference by literal; only the pool parameter tracks the knob.
	pool := e2econfig.PoolName()

	args := []string{
		"upgrade", "--install", "zfs-csi", chartRef,
		"--kubeconfig", kubeconfig,
		"--namespace", zfsCSINamespace,
		"--create-namespace",
		"--set", "namespace=" + zfsCSINamespace,
		"--set", "image.repository=" + repository,
		"--set", "image.pullPolicy=Always",
		"--set", "controller.enabled=true",
		"--set", "storage.enabled=true",
		"--set", "storage.enableVolumeImports=true",
		"--set", "node.enabled=true",
		"--set", "storageClasses.tankNVMe.enabled=true",
		"--set", "storageNode.name=" + node.Name,
		"--set", "network.portalHost=" + node.PortalHost,
		"--set", "network.nfsServer=" + node.NFSServer,
		// Pool wiring: keep the chart's StorageClass pool params + the pool mount
		// root in lock-step with the zpool the harness actually creates.
		"--set", "storageClasses.tankNVMe.pool=" + pool,
		"--set", "storageClasses.tankNFS.pool=" + pool,
		"--set", "storageNode.poolMountRoot=/" + pool,
		"--set", "e2e.enableHealthRepairHold=true",
		// Release-level run marker: lets staticDown prove the zfs-csi release it
		// is about to uninstall belongs to this run before touching it.
		"--labels", runOwnershipLabelPair(),
	}
	if digest != "" {
		args = append(args, "--set", "image.digest="+digest)
	} else {
		args = append(args, "--set", "image.tag="+tag)
	}
	// Each provider fixture selects the consumer-node network explicitly; no
	// chart or driver default is permitted for NFS exports.
	args = append(args, "--set", "storageClasses.tankNFS.enabled=true",
		"--set-json", "storageClasses.tankNFS.nfsExportCIDRs="+mustJSON(e2econfig.NFSExportCIDRs()))
	// Encryption: render the zfs-tank-nvme-encrypted StorageClass and wire the
	// driver to the lane's dev-mode OpenBao (ensureOpenBaoInfra). The chart's
	// encryption.openbao.addr/transitMount defaults already resolve to
	// openbao.openbao.svc:8200 + the transit mount, so only enabled + the dev
	// token are set here. No-op when E2E_ENCRYPTION=0.
	if e2econfig.EncryptionEnabled() {
		args = append(args,
			"--set", "encryption.enabled=true",
			"--set", "encryption.openbao.token="+openBaoDevToken,
		)
	}
	if e2econfig.TransportTLSEnabled() {
		args = append(args,
			"--set", "network.tls.enabled=true",
			"--set", "storageClasses.tankNFSTLS.enabled=true",
			"--set", "storageClasses.tankNFSTLS.pool="+pool,
			"--set-json", "storageClasses.tankNFSTLS.nfsExportCIDRs="+mustJSON(e2econfig.NFSExportCIDRs()),
			"--set", "storageClasses.tankNVMeTLS.enabled=true",
			"--set", "storageClasses.tankNVMeTLS.pool="+pool,
		)
	} else {
		args = append(args,
			"--set", "network.tls.enabled=false",
			"--set", "storageClasses.tankNFSTLS.enabled=false",
			"--set", "storageClasses.tankNVMeTLS.enabled=false",
		)
	}
	for _, overrides := range extraOverrides {
		args = appendSortedHelmOverrides(args, overrides)
	}
	if !isECR {
		args = append(args, "--wait", "--timeout", "10m")
	}
	out, err := exec.CommandContext(ctx, "helm", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("helm upgrade --install zfs-csi (%s): %w\n%s", chartRef, err, string(out))
	}

	// AWS lane: the driver image lives in ECR, but the CAPA AMI ships no
	// ecr-credential-provider, so kubelet cannot exchange the node IAM role for
	// an ECR token. Mint a docker-registry pull secret and restart only workloads
	// Helm actually rendered before readiness performs the final rollout proof.
	if isECR {
		if err := ensureECRPullSecret(ctx, kubeconfig, repository); err != nil {
			return fmt.Errorf("ensure ECR pull secret: %w", err)
		}
	}

	return nil
}

func ensureMultiOwnerECRPullSecret(ctx context.Context, kubeconfig, repository string, owners []storageOwner) error {
	if !isECRImage(repository) {
		return nil
	}
	signingNamespace := zfsCSINamespace + "-signing"
	targets := []driverWorkload{
		{resource: "deployment/zfs-csi-controller", serviceAccount: "zfs-csi-controller"},
		{resource: "daemonset/zfs-csi-node", serviceAccount: "zfs-csi-node"},
	}
	for _, owner := range owners {
		name := storageOwnerDeploymentName(owner.Node.Name)
		target := driverWorkload{resource: "deployment/" + name, serviceAccount: "zfs-csi-storage"}
		targets = append(targets, target)
	}
	serviceAccounts := make([]string, 0, len(targets))
	for _, target := range targets {
		serviceAccounts = append(serviceAccounts, target.serviceAccount)
	}
	if err := mintECRSecretAndPatchSAs(ctx, kubeconfig, repository, zfsCSINamespace, serviceAccounts); err != nil {
		return err
	}
	if err := mintECRSecretAndPatchSAs(ctx, kubeconfig, repository, signingNamespace, []string{"zfs-csi-tls-signer"}); err != nil {
		return fmt.Errorf("signer namespace: %w", err)
	}
	for _, target := range targets {
		out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig, "rollout", "restart", target.resource, "--namespace", zfsCSINamespace).CombinedOutput()
		if err != nil {
			return fmt.Errorf("rollout restart %s: %w\n%s", target.resource, err, string(out))
		}
	}
	out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig, "rollout", "restart", "statefulset/zfs-csi-tls-signer", "--namespace", signingNamespace).CombinedOutput()
	if err != nil {
		return fmt.Errorf("rollout restart statefulset/zfs-csi-tls-signer: %w\n%s", err, string(out))
	}
	for _, target := range targets {
		out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig, "rollout", "status", target.resource, "--namespace", zfsCSINamespace, "--timeout", "5m").CombinedOutput()
		if err != nil {
			return fmt.Errorf("rollout status %s: %w\n%s", target.resource, err, string(out))
		}
	}
	out, err = exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig, "rollout", "status", "statefulset/zfs-csi-tls-signer", "--namespace", signingNamespace, "--timeout", "5m").CombinedOutput()
	if err != nil {
		return fmt.Errorf("rollout status statefulset/zfs-csi-tls-signer: %w\n%s", err, string(out))
	}
	return nil
}

func installMultiOwnerDriverFromChart(ctx context.Context, kubeconfig, chartRef, image, valuesPath string, owners []storageOwner, extraOverrides ...map[string]string) error {
	repository, tag, digest, err := driverImageHelmValues(image)
	if err != nil {
		return err
	}
	if err := writeHelmStorageOwnerValues(valuesPath, owners); err != nil {
		return err
	}
	if err := installChartCRDs(ctx, kubeconfig, chartRef); err != nil {
		return err
	}
	if err := pruneObsoleteHelmStorageNodes(ctx, kubeconfig, owners); err != nil {
		return err
	}
	controllerSelector, err := controllerNodeSelector()
	if err != nil {
		return err
	}
	args := []string{
		"upgrade", "--install", "zfs-csi", chartRef,
		"--kubeconfig", kubeconfig,
		"--namespace", zfsCSINamespace,
		"--create-namespace",
		"--values", valuesPath,
		"--set", "namespace=" + zfsCSINamespace,
		"--set", "image.repository=" + repository,
		"--set", "image.pullPolicy=Always",
		"--set", "controller.enabled=true",
		"--set", "controller.replicas=2",
		"--set", "storage.enabled=true",
		"--set", "storage.enableVolumeImports=true",
		"--set", "node.enabled=true",
		"--set", "node.networkDomainSource=nodeLabel",
		"--set", "storageClasses.tankNVMe.enabled=true",
		"--set", "storageClasses.tankNVMe.pool=" + e2econfig.PoolName(),
		"--set", "storageClasses.tankNFS.enabled=true",
		"--set", "storageClasses.tankNFS.pool=" + e2econfig.PoolName(),
		"--set-json", "storageClasses.tankNFS.nfsExportCIDRs=" + mustJSON(e2econfig.NFSExportCIDRs()),
		"--set", "e2e.enableHealthRepairHold=true",
		// Release-level run marker (see installDriverFromChart).
		"--labels", runOwnershipLabelPair(),
	}
	for key, value := range controllerSelector {
		args = append(args, "--set-string", "controller.nodeSelector."+escapeHelmMapKey(key)+"="+value)
	}
	if e2econfig.InfrastructureProvider() == "static" {
		// Static lanes share the cluster with other workloads and may span
		// mixed architectures or carry NotReady nodes. Restrict the node
		// plugin DaemonSet to the consumer group so helm --wait only blocks
		// on nodes the smokes actually use.
		for key, value := range controllerSelector {
			args = append(args, "--set-string", "node.nodeSelector."+escapeHelmMapKey(key)+"="+value)
		}
	}
	if digest != "" {
		args = append(args, "--set", "image.digest="+digest)
	} else {
		args = append(args, "--set", "image.tag="+tag)
	}
	if e2econfig.EncryptionEnabled() {
		args = append(args, "--set", "encryption.enabled=true", "--set", "encryption.openbao.token="+openBaoDevToken)
	}
	if e2econfig.TransportTLSEnabled() {
		args = append(args,
			"--set", "network.tls.enabled=true",
			"--set", "storageClasses.tankNFSTLS.enabled=true",
			"--set", "storageClasses.tankNFSTLS.pool="+e2econfig.PoolName(),
			"--set-json", "storageClasses.tankNFSTLS.nfsExportCIDRs="+mustJSON(e2econfig.NFSExportCIDRs()),
			"--set", "storageClasses.tankNVMeTLS.enabled=true",
			"--set", "storageClasses.tankNVMeTLS.pool="+e2econfig.PoolName(),
		)
	} else {
		args = append(args,
			"--set", "network.tls.enabled=false",
			"--set", "storageClasses.tankNFSTLS.enabled=false",
			"--set", "storageClasses.tankNVMeTLS.enabled=false",
		)
	}
	for _, overrides := range extraOverrides {
		args = appendSortedHelmOverrides(args, overrides)
	}
	isECR := ecrRegistryRe.MatchString(repository)
	if !isECR {
		args = append(args, "--wait", "--timeout", "10m")
	}
	out, err := exec.CommandContext(ctx, "helm", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("helm upgrade --install multi-owner zfs-csi (%s): %w\n%s", chartRef, err, string(out))
	}
	if isECR {
		if err := ensureMultiOwnerECRPullSecret(ctx, kubeconfig, repository, owners); err != nil {
			return fmt.Errorf("ensure ECR pull secret: %w", err)
		}
	}
	return nil
}

func controllerNodeSelector() (map[string]string, error) {
	workers, err := e2econfig.ConsumerWorkers()
	if err != nil {
		return nil, fmt.Errorf("resolve controller worker placement: %w", err)
	}
	if len(workers) == 0 || len(workers[0].NodeSelector) == 0 {
		return nil, fmt.Errorf("resolve controller worker placement: first consumer group has no node selector")
	}
	return workers[0].NodeSelector, nil
}

func escapeHelmMapKey(key string) string {
	return strings.ReplaceAll(key, ".", `\.`)
}

func pruneObsoleteHelmStorageNodes(ctx context.Context, kubeconfig string, owners []storageOwner) error {
	wanted := make(map[string]struct{}, len(owners))
	for _, owner := range owners {
		wanted[owner.Node.Name] = struct{}{}
	}
	out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig, "get", "storagenodes.zfs.csi.randomvariable.co.uk", "-o", "json").Output()
	if err != nil {
		return fmt.Errorf("list StorageNodes before chart upgrade: %w", err)
	}
	var list struct {
		Items []struct {
			Metadata struct {
				Name        string            `json:"name"`
				Labels      map[string]string `json:"labels"`
				Annotations map[string]string `json:"annotations"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal(out, &list); err != nil {
		return fmt.Errorf("decode StorageNodes before chart upgrade: %w", err)
	}
	for _, item := range list.Items {
		if _, keep := wanted[item.Metadata.Name]; keep {
			continue
		}
		if item.Metadata.Labels["app.kubernetes.io/managed-by"] != "Helm" || item.Metadata.Annotations["meta.helm.sh/release-name"] != "zfs-csi" {
			return fmt.Errorf("refuse deleting obsolete unowned StorageNode %q", item.Metadata.Name)
		}
		cmd := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig, "delete", "storagenodes.zfs.csi.randomvariable.co.uk", item.Metadata.Name)
		if output, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("delete obsolete owned StorageNode %q: %w\n%s", item.Metadata.Name, err, string(output))
		}
	}
	return nil
}

// installChartCRDs establishes discovery before Helm creates StorageNode
// instances from the same chart. CRDs under templates/ cannot use Helm's
// pre-install ordering because REST mapping happens before any object is sent.
func installChartCRDs(ctx context.Context, kubeconfig, chartRef string) error {
	if strings.HasPrefix(chartRef, "oci://") {
		return fmt.Errorf("multi-owner E2E requires a local chart reference to install template CRDs first, got %q", chartRef)
	}
	crdPath := filepath.Join(chartRef, "templates", "crd")
	out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig, "apply", "--server-side", "--field-manager=zfs-csi-e2e", "-f", crdPath).CombinedOutput()
	if err != nil {
		return fmt.Errorf("install zfs-csi CRDs from %s: %w\n%s", crdPath, err, string(out))
	}
	out, err = exec.CommandContext(
		ctx,
		"kubectl", "--kubeconfig", kubeconfig,
		"label", "--overwrite", "customresourcedefinition",
		"volumes.zfs.csi.randomvariable.co.uk",
		"snapshots.zfs.csi.randomvariable.co.uk",
		"storagenodes.zfs.csi.randomvariable.co.uk",
		"volumeimports.zfs.csi.randomvariable.co.uk",
		"nvmeexports.nvmet.randomvariable.co.uk",
		"app.kubernetes.io/managed-by=Helm",
	).CombinedOutput()
	if err != nil {
		return fmt.Errorf("adopt zfs-csi CRD labels for Helm: %w\n%s", err, string(out))
	}
	out, err = exec.CommandContext(
		ctx,
		"kubectl", "--kubeconfig", kubeconfig,
		"annotate", "--overwrite", "customresourcedefinition",
		"volumes.zfs.csi.randomvariable.co.uk",
		"snapshots.zfs.csi.randomvariable.co.uk",
		"storagenodes.zfs.csi.randomvariable.co.uk",
		"volumeimports.zfs.csi.randomvariable.co.uk",
		"nvmeexports.nvmet.randomvariable.co.uk",
		"meta.helm.sh/release-name=zfs-csi",
		"meta.helm.sh/release-namespace="+zfsCSINamespace,
	).CombinedOutput()
	if err != nil {
		return fmt.Errorf("adopt zfs-csi CRD annotations for Helm: %w\n%s", err, string(out))
	}
	return nil
}

func appendSortedHelmOverrides(args []string, overrides map[string]string) []string {
	keys := make([]string, 0, len(overrides))
	for key := range overrides {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		args = append(args, "--set-string", key+"="+overrides[key])
	}
	return args
}

func mustJSON(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return string(raw)
}

// ecrRegistryRe matches an ECR registry host: <account>.dkr.ecr.<region>.amazonaws.com.
var ecrRegistryRe = regexp.MustCompile(`^([0-9]{12})\.dkr\.ecr\.([a-z0-9-]+)\.amazonaws\.com`)

const ecrSecretName = "ecr-creds"

// isECRImage reports whether repository is an ECR registry host.
func isECRImage(repository string) bool {
	return ecrRegistryRe.MatchString(repository)
}

// mintECRSecretAndPatchSAs mints an ECR docker-registry pull secret named
// ecr-creds in namespace and attaches it to each named ServiceAccount. It does
// NOT restart any workloads (callers that need a re-pull handle that). No-op
// unless repository is an ECR host. Idempotent: the secret is applied
// (create-or-replace, refreshing the short-lived token) and SA patches replace
// imagePullSecrets with the single ecr-creds entry.
func mintECRSecretAndPatchSAs(ctx context.Context, kubeconfig, repository, namespace string, saNames []string) error {
	m := ecrRegistryRe.FindStringSubmatch(repository)
	if m == nil {
		return nil // not ECR; nothing to do
	}
	registry, region := m[0], m[2]

	pw, err := exec.CommandContext(ctx, "aws", "ecr", "get-login-password", "--region", region).Output()
	if err != nil {
		return fmt.Errorf("aws ecr get-login-password (%s): %w", region, err)
	}

	// Build the docker-registry secret via kubectl create --dry-run, then apply
	// (create-or-replace) so a re-run refreshes the (short-lived) token.
	mkSecret := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig,
		"create", "secret", "docker-registry", ecrSecretName,
		"--namespace", namespace,
		"--docker-server", registry,
		"--docker-username", "AWS",
		"--docker-password", strings.TrimSpace(string(pw)),
		"--dry-run=client", "-o", "yaml",
	)
	secretYAML, err := mkSecret.Output()
	if err != nil {
		return fmt.Errorf("kubectl create secret (dry-run): %w", err)
	}
	apply := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig, "apply", "-f", "-")
	apply.Stdin = bytes.NewReader(secretYAML)
	if out, err := apply.CombinedOutput(); err != nil {
		return fmt.Errorf("kubectl apply ecr secret in %s: %w\n%s", namespace, err, string(out))
	}

	patch := fmt.Sprintf(`{"imagePullSecrets":[{"name":%q}]}`, ecrSecretName)
	for _, sa := range saNames {
		out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig,
			"patch", "serviceaccount", sa, "--namespace", namespace,
			"-p", patch,
		).CombinedOutput()
		if err != nil {
			return fmt.Errorf("patch serviceaccount %s/%s: %w\n%s", namespace, sa, err, string(out))
		}
	}

	return nil
}

// ensureECRPullSecretForNamespace mints the ECR pull secret in namespace and
// attaches it to that namespace's `default` ServiceAccount. Used before running
// setup pods (e.g. the ZFS pool-create pod) that use the ECR driver/preflight
// image in a namespace other than the driver namespace. No-op for non-ECR.
func ensureECRPullSecretForNamespace(ctx context.Context, kubeconfig, repository, namespace string) error {
	return mintECRSecretAndPatchSAs(ctx, kubeconfig, repository, namespace, []string{"default"})
}

// ensureECRPullSecret mints the ECR pull secret in the driver namespace, attaches
// it to the four driver ServiceAccounts, then restarts the driver workloads so
// their pods re-pull with credentials. No-op unless repository is an ECR host.
func ensureECRPullSecret(ctx context.Context, kubeconfig, repository string) error {
	if !isECRImage(repository) {
		return nil
	}
	targets, err := activeDriverRolloutTargets(ctx, kubeconfig)
	if err != nil {
		return err
	}
	serviceAccounts := make([]string, 0, len(targets))
	for _, target := range targets {
		serviceAccounts = append(serviceAccounts, target.serviceAccount)
	}
	if err := mintECRSecretAndPatchSAs(ctx, kubeconfig, repository, zfsCSINamespace, serviceAccounts); err != nil {
		return err
	}

	// The optional nvmet DaemonSet is absent with the default chart values. Only
	// restart resources that Helm actually rendered, using the same workload
	// definitions as readiness verification.
	for _, target := range targets {
		out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig,
			"rollout", "restart", target.resource, "--namespace", zfsCSINamespace,
		).CombinedOutput()
		if err != nil {
			return fmt.Errorf("rollout restart %s: %w\n%s", target.resource, err, string(out))
		}
	}
	for _, target := range targets {
		out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig,
			"rollout", "status", target.resource, "--namespace", zfsCSINamespace, "--timeout", "5m",
		).CombinedOutput()
		if err != nil {
			return fmt.Errorf("rollout status %s: %w\n%s", target.resource, err, string(out))
		}
	}

	return nil
}

// runOwnershipLabelPair renders the e2e-run-id ownership label as a helm
// --labels key=value pair so the release itself is marked as harness-owned.
func runOwnershipLabelPair() string {
	return fmt.Sprintf("zfs-csi.randomvariable.co.uk/e2e-run-id=%s", e2econfig.RunID())
}

// snapshotClassName is the VolumeSnapshotClass the testdrivers reference
// (SnapshotClass.FromExistingClassName). It must match the name in both
// test/e2e/data/testdriver/*.yaml.
const snapshotClassName = "zfs-tank-snapclass"

// volumeAttributesClassName is copied by each external-storage test namespace.
// Its compression parameter is valid for filesystem and zvol backends, including
// encrypted datasets, so all testdrivers can exercise ModifyVolume.
const volumeAttributesClassName = "zfs-csi-e2e-compression-zstd-3"

// ensureSnapshotInfra deploys the cluster-shared external-snapshotter stack
// (VolumeSnapshot CRDs + snapshot-controller) from the vendored bundle, waits for
// the controller, then creates the driver's VolumeSnapshotClass. The
// csi-snapshotter sidecar itself ships in the driver chart; this provides the
// cluster-scoped plumbing the snapshottable conformance suites require. Applied
// server-side because the vendored CRDs exceed the client-side apply annotation
// size limit. Idempotent (apply + create-or-replace).
func ensureSnapshotInfra(ctx context.Context, kubeconfig string) error {
	// Shared-cluster guard (static provider / E2E_SKIP_SNAPSHOT_BUNDLE=1): a
	// pre-existing cluster typically owns its own snapshot-controller + CRDs.
	// Force-applying the vendored bundle there could downgrade or steal SSA
	// ownership of foreign kube-system objects — skip the bundle when the
	// VolumeSnapshot CRDs already exist and only ensure the VolumeSnapshotClass.
	// When the CRDs are absent even on a skip-requested lane, fall through and
	// apply the bundle: snapshot conformance would otherwise hard-fail mid-run.
	applyBundle := true
	if e2econfig.SkipSnapshotBundle() {
		// Distinguish NotFound (CRD absent → bundle required) from transport or
		// auth failures (must not silently fall through to force-applying the
		// bundle onto a shared cluster we could not even inspect).
		out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig,
			"get", "crd", "volumesnapshots.snapshot.storage.k8s.io",
		).CombinedOutput()
		if err == nil {
			applyBundle = false
		} else if !strings.Contains(string(out), "not found") && !strings.Contains(string(out), "NotFound") {
			return fmt.Errorf("probe volumesnapshot CRD presence (refusing to guess on a shared cluster): %w\n%s", err, string(out))
		}
	}
	if applyBundle {
		bundle, err := filepath.Abs(filepath.Join("test", "e2e", "data", "snapshot", "external-snapshotter-v8.2.0.yaml"))
		if err != nil {
			return fmt.Errorf("resolve snapshot bundle path: %w", err)
		}
		out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig,
			"apply", "--server-side", "--force-conflicts", "-f", bundle,
		).CombinedOutput()
		if err != nil {
			return fmt.Errorf("apply external-snapshotter bundle: %w\n%s", err, string(out))
		}

		if out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig,
			"-n", "kube-system", "rollout", "status", "deployment/snapshot-controller", "--timeout", "3m",
		).CombinedOutput(); err != nil {
			return fmt.Errorf("wait snapshot-controller: %w\n%s", err, string(out))
		}
	}

	snapClass := fmt.Sprintf(`apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata:
  name: %s
  labels:
%s
driver: zfs.csi.randomvariable.co.uk
deletionPolicy: Delete
`, snapshotClassName, indentedOwnershipLabels("    "))
	apply := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig, "apply", "-f", "-")
	apply.Stdin = strings.NewReader(snapClass)
	if out, err := apply.CombinedOutput(); err != nil {
		return fmt.Errorf("create VolumeSnapshotClass %s: %w\n%s", snapshotClassName, err, string(out))
	}

	return nil
}

// indentedOwnershipLabels renders the run ownership labels as YAML map entries
// at the given indent so cluster-scoped classes (VolumeSnapshotClass,
// VolumeAttributesClass) created outside Helm are still reachable by the
// label-scoped static-lane cleanup.
func indentedOwnershipLabels(indent string) string {
	labels := smokeOwnershipLabels()
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, key := range keys {
		fmt.Fprintf(&b, "%s%s: %q\n", indent, key, labels[key])
	}
	return strings.TrimRight(b.String(), "\n")
}

// ensureVolumeAttributesClassInfra verifies that the cluster exposes the stable
// storage.k8s.io/v1 API and creates the class copied by external-storage tests.
// Unlike snapshots, VAC is a native Kubernetes API here; the driver installs no
// CRD and makes no cluster feature-gate claim.
func ensureVolumeAttributesClassInfra(ctx context.Context, kubeconfig string) error {
	out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig,
		"get", "--raw=/apis/storage.k8s.io/v1",
	).CombinedOutput()
	if err != nil {
		return fmt.Errorf("read stable storage.k8s.io/v1 API discovery: %w\n%s", err, string(out))
	}
	if !strings.Contains(string(out), "volumeattributesclasses") {
		return fmt.Errorf("stable storage.k8s.io/v1 VolumeAttributesClass resource is unavailable")
	}

	manifest, err := os.ReadFile(filepath.Join("test", "e2e", "data", "vac", "compression-zstd-3.yaml"))
	if err != nil {
		return fmt.Errorf("read external-storage VolumeAttributesClass manifest: %w", err)
	}
	apply := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig, "apply", "-f", "-")
	apply.Stdin = strings.NewReader(string(manifest))
	if out, err := apply.CombinedOutput(); err != nil {
		return fmt.Errorf("create VolumeAttributesClass %s: %w\n%s", volumeAttributesClassName, err, string(out))
	}
	// The VAC manifest is a static file with no run labels; stamp them after
	// apply so the label-scoped cleanup reaps this cluster-scoped object.
	for key, value := range smokeOwnershipLabels() {
		labelArg := fmt.Sprintf("%s=%s", key, value)
		if out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig,
			"label", "--overwrite", "volumeattributesclass", volumeAttributesClassName, labelArg,
		).CombinedOutput(); err != nil {
			return fmt.Errorf("label VolumeAttributesClass %s: %w\n%s", volumeAttributesClassName, err, string(out))
		}
	}

	return nil
}

// driverChartRef returns the chart reference for the install: the pushed OCI
// chart in CI (--e2e-chart-ref / E2E_CHART_REF), or the in-repo chart path for
// local runs (default "charts/zfs-csi").
func driverChartRef() string {
	return e2econfig.ChartReference()
}

type driverWorkload struct {
	object         client.Object
	resource       string
	serviceAccount string
	component      string
	containers     []string
	optional       bool
}

// driverWorkloads is the one source of truth for installation recovery,
// readiness, and digest proof. Optional means Helm may omit it.
func driverWorkloads() []driverWorkload {
	return []driverWorkload{
		{
			object:         &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "zfs-csi-controller", Namespace: zfsCSINamespace}},
			resource:       "deployment/zfs-csi-controller",
			serviceAccount: "zfs-csi-controller",
			component:      "controller",
			containers:     []string{"driver"},
		},
		{
			object:         &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "zfs-csi-storage", Namespace: zfsCSINamespace}},
			resource:       "daemonset/zfs-csi-storage",
			serviceAccount: "zfs-csi-storage",
			component:      "storage",
			containers:     []string{"storage"},
		},
		{
			object:         &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "zfs-csi-node", Namespace: zfsCSINamespace}},
			resource:       "daemonset/zfs-csi-node",
			serviceAccount: "zfs-csi-node",
			component:      "node",
			containers:     []string{"driver", "nvmet-stage", "nfs-stage"},
		},
		{
			object:         &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "nvmet-controller", Namespace: zfsCSINamespace}},
			resource:       "daemonset/nvmet-controller",
			serviceAccount: "nvmet-controller",
			component:      "nvmet",
			containers:     []string{"nvmet-controller"},
			optional:       true,
		},
	}
}

func activeDriverWorkloads(ctx context.Context, c client.Reader) ([]driverWorkload, error) {
	active := make([]driverWorkload, 0, len(driverWorkloads()))
	for _, workload := range driverWorkloads() {
		if err := c.Get(ctx, client.ObjectKeyFromObject(workload.object), workload.object); err != nil {
			// Multi-owner installs deploy storage as per-owner Deployments
			// (zfs-csi-storage-<owner>-<hash>) instead of the legacy single-owner
			// DaemonSet. Discover them by component label.
			if workload.component == "storage" && apierrors.IsNotFound(err) {
				discovered, derr := discoverStorageWorkloadObjects(ctx, c)
				if derr != nil {
					return nil, derr
				}
				if len(discovered) > 0 {
					active = append(active, discovered...)
					continue
				}
			}
			if workload.optional && apierrors.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("get driver workload %s: %w", workload.resource, err)
		}
		active = append(active, workload)
	}
	return active, nil
}

// discoverStorageWorkloadObjects lists per-owner storage Deployments (and any
// storage DaemonSet) in the driver namespace by component label, for
// multi-owner installs where daemonset/zfs-csi-storage does not exist.
func discoverStorageWorkloadObjects(ctx context.Context, c client.Reader) ([]driverWorkload, error) {
	targets := []driverWorkload{}
	deployments := &appsv1.DeploymentList{}
	if err := c.List(ctx, deployments, client.InNamespace(zfsCSINamespace), client.MatchingLabels{"app.kubernetes.io/component": "storage"}); err != nil {
		return nil, fmt.Errorf("list storage deployments by label: %w", err)
	}
	for i := range deployments.Items {
		d := &deployments.Items[i]
		targets = append(targets, driverWorkload{
			object:    d,
			resource:  "deployment/" + d.Name,
			component: "storage",
		})
	}
	daemonsets := &appsv1.DaemonSetList{}
	if err := c.List(ctx, daemonsets, client.InNamespace(zfsCSINamespace), client.MatchingLabels{"app.kubernetes.io/component": "storage"}); err != nil {
		return nil, fmt.Errorf("list storage daemonsets by label: %w", err)
	}
	for i := range daemonsets.Items {
		ds := &daemonsets.Items[i]
		targets = append(targets, driverWorkload{
			object:    ds,
			resource:  "daemonset/" + ds.Name,
			component: "storage",
		})
	}
	return targets, nil
}

func activeDriverRolloutTargets(ctx context.Context, kubeconfig string) ([]driverWorkload, error) {
	active := make([]driverWorkload, 0, len(driverWorkloads()))
	for _, workload := range driverWorkloads() {
		out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig,
			"get", workload.resource, "--namespace", zfsCSINamespace, "--ignore-not-found", "-o", "name",
		).CombinedOutput()
		if err != nil {
			return nil, fmt.Errorf("get driver workload %s: %w\n%s", workload.resource, err, string(out))
		}
		if strings.TrimSpace(string(out)) == "" {
			// Multi-owner installs deploy storage as per-owner Deployments
			// (deployment/zfs-csi-storage-<owner>-<hash>) instead of the legacy
			// single-owner DaemonSet. Discover them by component label rather
			// than failing on the fixed legacy name.
			if workload.component == "storage" {
				discovered, derr := discoverStorageRolloutTargets(ctx, kubeconfig)
				if derr != nil {
					return nil, derr
				}
				if len(discovered) > 0 {
					active = append(active, discovered...)
					continue
				}
			}
			if workload.optional {
				continue
			}
			return nil, fmt.Errorf("required driver workload %s is missing", workload.resource)
		}
		active = append(active, workload)
	}
	return active, nil
}

// discoverStorageRolloutTargets lists every storage workload (DaemonSet or
// per-owner Deployment) in the driver namespace by component label. Used when
// the legacy single-owner daemonset/zfs-csi-storage is absent.
func discoverStorageRolloutTargets(ctx context.Context, kubeconfig string) ([]driverWorkload, error) {
	out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig,
		"get", "daemonset,deployment", "--namespace", zfsCSINamespace,
		"--selector", "app.kubernetes.io/component=storage",
		"--ignore-not-found", "-o", "name",
	).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("discover storage workloads by label: %w\n%s", err, string(out))
	}
	targets := []driverWorkload{}
	for _, resource := range strings.Fields(string(out)) {
		targets = append(targets, driverWorkload{resource: resource, component: "storage"})
	}
	return targets, nil
}

func waitForDriverReady(ctx context.Context, c client.Client) error {
	workloads, err := activeDriverWorkloads(ctx, c)
	if err != nil {
		return err
	}
	for _, workload := range workloads {
		if err := driverWorkloadReady(workload.object); err != nil {
			return err
		}
	}
	return nil
}

func waitForMultiOwnerDriverReady(ctx context.Context, c client.Client, owners []storageOwner) error {
	controller := &appsv1.Deployment{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: zfsCSINamespace, Name: "zfs-csi-controller"}, controller); err != nil {
		return err
	}
	if controller.Spec.Replicas == nil || *controller.Spec.Replicas < 2 || controller.Status.ReadyReplicas != *controller.Spec.Replicas {
		return fmt.Errorf("controller has %d/%d ready replicas; multi-owner install requires all active-active replicas ready", controller.Status.ReadyReplicas, ptr.Deref(controller.Spec.Replicas, 0))
	}
	if err := workloadContainersReady(controller); err != nil {
		return err
	}
	for _, workload := range []struct {
		object   client.Object
		optional bool
	}{
		{object: &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "zfs-csi-node", Namespace: zfsCSINamespace}}},
		{object: &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "zfs-csi-snapshot-controller", Namespace: zfsCSINamespace}}, optional: true},
	} {
		if err := c.Get(ctx, client.ObjectKeyFromObject(workload.object), workload.object); err != nil {
			if workload.optional && apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		if err := driverWorkloadReady(workload.object); err != nil {
			return err
		}
		if err := workloadContainersReady(workload.object); err != nil {
			return err
		}
	}
	storageNodes := &zfscsiv1.StorageNodeList{}
	if err := c.List(ctx, storageNodes); err != nil {
		return fmt.Errorf("list StorageNodes: %w", err)
	}
	if len(storageNodes.Items) != len(owners) {
		return fmt.Errorf("found %d StorageNodes, want %d enabled owners", len(storageNodes.Items), len(owners))
	}
	byName := make(map[string]*zfscsiv1.StorageNode, len(storageNodes.Items))
	for i := range storageNodes.Items {
		byName[storageNodes.Items[i].Name] = &storageNodes.Items[i]
	}
	for _, owner := range owners {
		deployment := &appsv1.Deployment{}
		name := storageOwnerDeploymentName(owner.Node.Name)
		if err := c.Get(ctx, client.ObjectKey{Namespace: zfsCSINamespace, Name: name}, deployment); err != nil {
			return fmt.Errorf("get storage-agent Deployment %q: %w", name, err)
		}
		if deployment.Status.ReadyReplicas != 1 {
			return fmt.Errorf("storage-agent Deployment %q has %d ready replicas, want 1", name, deployment.Status.ReadyReplicas)
		}
		if err := workloadContainersReady(deployment); err != nil {
			return err
		}
		inventory := byName[owner.Node.Name]
		if inventory == nil {
			return fmt.Errorf("StorageNode %q is missing", owner.Node.Name)
		}
		if !meta.IsStatusConditionTrue(inventory.Status.Conditions, zfscsiv1.StorageNodeConditionReady) {
			return fmt.Errorf("StorageNode %q Ready condition is not true", owner.Node.Name)
		}
		if !slices.Equal(inventory.Spec.AuthoritativePoolGUIDs, []string{owner.PoolGUID}) {
			return fmt.Errorf("StorageNode %q authoritative GUIDs %v do not match %q", owner.Node.Name, inventory.Spec.AuthoritativePoolGUIDs, owner.PoolGUID)
		}
		if inventory.Spec.NetworkDomain != owner.NetworkDomain || !sameStringSet(inventory.Status.ReachableFrom, owner.ReachableFrom) {
			return fmt.Errorf("StorageNode %q reachability inventory does not match configured domain contract", owner.Node.Name)
		}
		if !inventoryHasEndpoint(inventory, zfscsiv1.StorageProtocolNFS, owner.Node.NFSServer, int32(owner.NFSPort)) ||
			!inventoryHasEndpoint(inventory, zfscsiv1.StorageProtocolNVMeTCP, owner.Node.PortalHost, int32(owner.NVMePort)) {
			return fmt.Errorf("StorageNode %q does not report both configured endpoints", owner.Node.Name)
		}
		if !inventoryHasReadyPool(inventory, owner.PoolGUID, owner.PoolName) {
			return fmt.Errorf("StorageNode %q lacks ready authoritative pool %q", owner.Node.Name, owner.PoolGUID)
		}
	}
	return nil
}

func workloadContainersReady(object client.Object) error {
	var containers []corev1.Container
	switch typed := object.(type) {
	case *appsv1.Deployment:
		containers = typed.Spec.Template.Spec.Containers
	case *appsv1.DaemonSet:
		containers = typed.Spec.Template.Spec.Containers
	default:
		return fmt.Errorf("unsupported driver workload %T", object)
	}
	if len(containers) == 0 {
		return fmt.Errorf("driver workload %s has no containers", client.ObjectKeyFromObject(object))
	}
	for _, container := range containers {
		if container.Name == "" {
			return fmt.Errorf("driver workload %s contains an unnamed sidecar", client.ObjectKeyFromObject(object))
		}
	}
	return nil
}

func storageOwnerDeploymentName(ownerName string) string {
	slug := strings.ToLower(ownerName)
	slug = regexp.MustCompile(`[^a-z0-9-]+`).ReplaceAllString(slug, "-")
	slug = strings.Trim(slug, "-")
	if len(slug) > 40 {
		slug = slug[:40]
	}
	sum := fmt.Sprintf("%x", sha256.Sum256([]byte(ownerName)))[:8]
	return strings.TrimSuffix("zfs-csi-storage-"+slug+"-"+sum, "-")
}

func inventoryHasEndpoint(node *zfscsiv1.StorageNode, protocol zfscsiv1.StorageProtocol, host string, port int32) bool {
	for _, endpoint := range node.Status.Endpoints {
		if endpoint.Protocol == protocol && endpoint.Host == host && endpoint.Port == port {
			return true
		}
	}
	return false
}

func inventoryHasReadyPool(node *zfscsiv1.StorageNode, guid, name string) bool {
	for _, pool := range node.Status.Pools {
		if pool.GUID == guid && pool.Name == name && pool.Ready {
			return true
		}
	}
	return false
}

func sameStringSet(left, right []string) bool {
	left = append([]string(nil), left...)
	right = append([]string(nil), right...)
	sort.Strings(left)
	sort.Strings(right)
	return slices.Equal(left, right)
}

func driverWorkloadReady(obj client.Object) error {
	switch typed := obj.(type) {
	case *appsv1.Deployment:
		if typed.Status.ReadyReplicas < 1 {
			return fmt.Errorf("deployment %s/%s has %d ready replicas", typed.Namespace, typed.Name, typed.Status.ReadyReplicas)
		}
	case *appsv1.DaemonSet:
		if typed.Status.NumberReady < 1 {
			return fmt.Errorf("daemonset %s/%s has %d ready pods", typed.Namespace, typed.Name, typed.Status.NumberReady)
		}
	default:
		return fmt.Errorf("unsupported driver workload %T", obj)
	}
	return nil
}

type diagnosticList struct {
	name    string
	list    client.ObjectList
	options []client.ListOption
}

func collectDriverDiagnosticsWithLogs(ctx context.Context, c client.Client, kubeconfig string) string {
	return collectDriverDiagnosticsWithRunner(ctx, c, kubeconfig, runKubectlDiagnostic)
}

type diagnosticRunner func(context.Context, string, ...string) ([]byte, error)

func collectDriverDiagnosticsWithRunner(ctx context.Context, c client.Client, kubeconfig string, runner diagnosticRunner) string {
	lists := []diagnosticList{
		{name: "driver pods", list: &corev1.PodList{}, options: []client.ListOption{client.InNamespace(zfsCSINamespace)}},
		{name: "driver events", list: &corev1.EventList{}, options: []client.ListOption{client.InNamespace(zfsCSINamespace)}},
		{name: "all pods", list: &corev1.PodList{}},
		{name: "all events", list: &corev1.EventList{}},
		{name: "persistent volume claims", list: &corev1.PersistentVolumeClaimList{}},
		{name: "persistent volumes", list: &corev1.PersistentVolumeList{}},
		{name: "volume attachments", list: &storagev1.VolumeAttachmentList{}},
		{name: "CSI nodes", list: &storagev1.CSINodeList{}},
		{name: "storage classes", list: &storagev1.StorageClassList{}},
		{name: "driver deployments", list: &appsv1.DeploymentList{}, options: []client.ListOption{client.InNamespace(zfsCSINamespace)}},
		{name: "driver daemonsets", list: &appsv1.DaemonSetList{}, options: []client.ListOption{client.InNamespace(zfsCSINamespace)}},
	}
	var b strings.Builder
	for _, item := range lists {
		if err := c.List(ctx, item.list, item.options...); err != nil {
			fmt.Fprintf(&b, "## %s\nlist %T: %v\n", item.name, item.list, err)
			continue
		}
		fmt.Fprintf(&b, "## %s\n%s\n", item.name, objectListYAML(item.list))
	}

	for _, gvk := range []struct{ name, apiVersion, kind, namespace string }{
		{"driver volumes", "zfs.csi.randomvariable.co.uk/v1alpha1", "VolumeList", zfsCSINamespace},
		{"NVMe exports", "nvmet.randomvariable.co.uk/v1alpha1", "NvmeExportList", zfsCSINamespace},
		{"volume snapshots", "snapshot.storage.k8s.io/v1", "VolumeSnapshotList", ""},
		{"volume snapshot contents", "snapshot.storage.k8s.io/v1", "VolumeSnapshotContentList", ""},
		{"volume snapshot classes", "snapshot.storage.k8s.io/v1", "VolumeSnapshotClassList", ""},
	} {
		list := &unstructured.UnstructuredList{}
		list.SetAPIVersion(gvk.apiVersion)
		list.SetKind(gvk.kind)
		var options []client.ListOption
		if gvk.namespace != "" {
			options = append(options, client.InNamespace(gvk.namespace))
		}
		if err := c.List(ctx, list, options...); err != nil {
			fmt.Fprintf(&b, "## %s\nlist %s: %v\n", gvk.name, gvk.kind, err)
			continue
		}
		fmt.Fprintf(&b, "## %s\n%s\n", gvk.name, objectListYAML(list))
	}

	if runner != nil && kubeconfig != "" {
		pods := &corev1.PodList{}
		if err := c.List(ctx, pods, client.InNamespace(zfsCSINamespace)); err != nil {
			fmt.Fprintf(&b, "## driver logs\nlist pods: %v\n", err)
		} else {
			appendDriverLogs(ctx, &b, kubeconfig, pods.Items, runner)
		}
	}

	return b.String()
}

func appendDriverLogs(ctx context.Context, b *strings.Builder, kubeconfig string, pods []corev1.Pod, runner diagnosticRunner) {
	fmt.Fprintln(b, "## driver logs")
	for i := range pods {
		pod := &pods[i]
		containers := make([]corev1.Container, 0, len(pod.Spec.InitContainers)+len(pod.Spec.Containers))
		containers = append(containers, pod.Spec.InitContainers...)
		containers = append(containers, pod.Spec.Containers...)
		for _, container := range containers {
			args := []string{"--kubeconfig", kubeconfig, "-n", pod.Namespace, "logs", pod.Name, "-c", container.Name, "--timestamps=true", "--tail=-1"}
			out, err := runner(ctx, "kubectl", args...)
			fmt.Fprintf(b, "### %s/%s container=%s\n", pod.Namespace, pod.Name, container.Name)
			if err != nil {
				fmt.Fprintf(b, "log collection failed: %v\n%s\n", err, out)
				continue
			}
			fmt.Fprintf(b, "%s\n", out)
		}
	}
}

func runKubectlDiagnostic(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

func objectListYAML(obj runtime.Object) string {
	copy := obj.DeepCopyObject()
	info, err := meta.ExtractList(copy)
	if err != nil {
		return err.Error()
	}
	encoder := kjson.NewYAMLSerializer(kjson.DefaultMetaFactory, nil, nil)
	var b strings.Builder
	for _, item := range info {
		redacted, err := sanitizedDiagnosticObject(item)
		if err != nil {
			fmt.Fprintf(&b, "sanitize %T: %v\n", item, err)
			continue
		}
		if err := encoder.Encode(redacted, &b); err != nil {
			fmt.Fprintf(&b, "encode %T: %v\n", item, err)
		}
	}
	return b.String()
}

const diagnosticRedaction = "<redacted>"

var secretEnvNamePart = regexp.MustCompile(`(^|_)(TOKEN|PASSWORD|SECRET|KEY|CREDENTIAL|AUTH|OPENBAO)($|_)`)

// sanitizedDiagnosticObject converts an object to an unstructured copy and
// redacts secret-bearing fields. It never mutates client or cache objects.
func sanitizedDiagnosticObject(obj runtime.Object) (runtime.Object, error) {
	raw, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	value := map[string]any{}
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	if _, ok := obj.(*corev1.Secret); ok {
		redactMapField(value, "data")
		redactMapField(value, "stringData")
	}
	sanitizeDiagnosticValue(value, "")
	return &unstructured.Unstructured{Object: value}, nil
}

func sanitizeDiagnosticValue(value any, parentKey string) {
	switch typed := value.(type) {
	case map[string]any:
		kind, _ := typed["kind"].(string)
		if strings.EqualFold(kind, "Secret") {
			redactMapField(typed, "data")
			redactMapField(typed, "stringData")
		}
		if strings.EqualFold(parentKey, "secretKeyRef") {
			redactMapField(typed, "name")
			redactMapField(typed, "key")
		}
		for key, child := range typed {
			switch key {
			case "env":
				sanitizeDiagnosticEnv(child)
			case "imagePullSecrets":
				redactImagePullSecrets(child)
			default:
				sanitizeDiagnosticValue(child, key)
			}
		}
	case []any:
		for _, child := range typed {
			sanitizeDiagnosticValue(child, parentKey)
		}
	}
}

func sanitizeDiagnosticEnv(value any) {
	env, ok := value.([]any)
	if !ok {
		return
	}
	for _, item := range env {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name, _ := entry["name"].(string)
		if secretEnvNamePart.MatchString(strings.ToUpper(name)) {
			if _, exists := entry["value"]; exists {
				entry["value"] = diagnosticRedaction
			}
		}
		sanitizeDiagnosticValue(entry, "")
	}
}

func redactImagePullSecrets(value any) {
	refs, ok := value.([]any)
	if !ok {
		return
	}
	for _, item := range refs {
		if ref, ok := item.(map[string]any); ok {
			redactMapField(ref, "name")
		}
	}
}

func redactMapField(value map[string]any, key string) {
	if _, exists := value[key]; exists {
		value[key] = diagnosticRedaction
	}
}

func addDriverTypes(scheme *runtime.Scheme) error {
	if err := appsv1.AddToScheme(scheme); err != nil {
		return err
	}
	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		return err
	}
	if err := zfscsiv1.AddToScheme(scheme); err != nil {
		return err
	}
	if err := nvmetv1.AddToScheme(scheme); err != nil {
		return err
	}

	return nil
}
