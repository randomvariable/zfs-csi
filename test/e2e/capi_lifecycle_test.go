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
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	certificatesv1beta1 "k8s.io/api/certificates/v1beta1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	bootstrapv1 "sigs.k8s.io/cluster-api/api/bootstrap/kubeadm/v1beta2"
	controlplanev1 "sigs.k8s.io/cluster-api/api/controlplane/kubeadm/v1beta2"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/test/framework"
	"sigs.k8s.io/cluster-api/test/framework/clusterctl"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/randomvariable/zfs-csi/test/e2e/e2econfig"
)

const (
	preTeardownInventoryTimeout = 2 * time.Minute
	clusterDeleteTimeout        = 35 * time.Minute
	awsOrphanScanTimeout        = 5 * time.Minute
)

func conformanceSSHBastion(ctx context.Context, c client.Client, namespace, clusterName, provider string) (string, error) {
	if provider != "aws" {
		return "", nil
	}
	awsCluster := &unstructured.Unstructured{}
	awsCluster.SetAPIVersion("infrastructure.cluster.x-k8s.io/v1beta2")
	awsCluster.SetKind("AWSCluster")
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: clusterName}, awsCluster); err != nil {
		return "", fmt.Errorf("get AWSCluster %s/%s for conformance bastion: %w", namespace, clusterName, err)
	}
	publicIP, found, err := unstructured.NestedString(awsCluster.Object, "status", "bastion", "publicIp")
	if err != nil {
		return "", fmt.Errorf("read AWSCluster %s/%s status.bastion.publicIp: %w", namespace, clusterName, err)
	}
	if !found || strings.TrimSpace(publicIP) == "" {
		return "", fmt.Errorf("AWSCluster %s/%s has no status.bastion.publicIp; CAPA bastion must be ready before conformance", namespace, clusterName)
	}
	return publicIP + ":22", nil
}

var _ = Describe("CAPI/CAPK lifecycle", Ordered, func() {
	var (
		ctx                         context.Context
		cancel                      context.CancelFunc
		artifactDir                 string
		clusterName                 string
		clusterProxy                framework.ClusterProxy
		clusterctlConfigPath        string
		cleanupOnly                 bool
		skipCleanup                 bool
		runID                       string
		result                      *clusterctl.ApplyClusterTemplateAndWaitResult
		workloadCluster             framework.ClusterProxy
		workloadClient              client.Client
		storage                     storageNode
		storageOwners               []storageOwner
		multiOwner                  bool
		staticProvider              bool
		driverImage                 string
		staticDefaultStorageClasses []string
		staticZFSDefaultClasses     []string
		// deleteClusterIntervals is the [timeout, poll] pair for cluster-deletion
		// waits, captured from the e2e-config's wait-cluster interval. Without it,
		// the framework's Delete*AndWait default to a ~1s Eventually that times out
		// instantly on AWS (EC2 terminate + ELB + VPC teardown takes minutes).
		deleteClusterIntervals []interface{}
	)

	BeforeAll(func() {
		runID = requireE2EConfig()
		var err error
		cleanupOnly, err = validateCAPILifecycleSubstrate()
		Expect(err).NotTo(HaveOccurred())
		suiteTimeout := 90 * time.Minute
		if e2econfig.PodCertificateAcceptanceEnabled() {
			suiteTimeout = 10 * time.Hour
		}
		ctx, cancel = context.WithTimeout(context.Background(), suiteTimeout)
		clusterName = perRunName(runID)
		// clusterctl's local-filesystem repository client requires an absolute
		// RepositoryFolder, so the artifact dir must be absolute too.
		absArtifacts, err := filepath.Abs(filepath.Join("_artifacts", runID))
		Expect(err).NotTo(HaveOccurred())
		artifactDir = absArtifacts
		skipCleanup = e2econfig.IsSkipCleanup()
		multiOwner = strings.TrimSpace(os.Getenv(e2econfig.Env[e2econfig.InfrastructureConfigKey])) != ""
		staticProvider = e2econfig.InfrastructureProvider() == "static"
		Expect(os.MkdirAll(artifactDir, 0o750)).To(Succeed())

		By(fmt.Sprintf("e2e run %s: cluster=%s namespace=%s artifacts=%s", runID, clusterName, e2eNamespace, artifactDir))

		if staticProvider {
			// Static provider: the workload cluster pre-exists and is reached via
			// its own kubeconfig. There is no management cluster, no clusterctl
			// repository, no cluster template, and no provider fabric — the whole
			// CAPI provisioning path below is skipped. All infrastructure identity
			// (owner→node mapping, endpoints, pool names) comes from the
			// gitignored InfrastructureConfig at runtime.
			Expect(multiOwner).To(BeTrue(), "%s is required for the static provider (explicit owner→node mapping)", e2econfig.Env[e2econfig.InfrastructureConfigKey])
			By("static provider: connecting to the pre-existing workload cluster")
			workloadCluster = framework.NewClusterProxy("zfs-csi-e2e-workload", e2econfig.WorkloadKubeconfigPath(), newScheme())
			workloadClient = workloadCluster.GetClient()
			return
		}

		e2eConfigPath, err := e2econfig.ConfigPath()
		Expect(err).NotTo(HaveOccurred())
		By("loading e2e config and creating clusterctl repository")
		e2eConfig := clusterctl.LoadE2EConfig(ctx, clusterctl.LoadE2EConfigInput{ConfigPath: e2eConfigPath})
		clusterctlConfigPath = clusterctl.CreateRepository(ctx, clusterctl.CreateRepositoryInput{
			RepositoryFolder: filepath.Join(artifactDir, "clusterctl-repository"),
			E2EConfig:        e2eConfig,
		})
		// Capture the deletion wait interval now (config is loaded before the
		// cleanup-only early return, so both teardown paths in AfterAll get it).
		deleteClusterIntervals = e2eConfig.GetIntervals("default", "wait-cluster")

		By("creating management cluster proxy")
		clusterProxy = framework.NewClusterProxy("zfs-csi-e2e-management", e2econfig.HostKubeconfigPath(), newScheme())
		if cleanupOnly {
			By("cleanup-only mode, skipping cluster creation")
			return
		}

		By(fmt.Sprintf("ensuring shared namespace %s exists", e2eNamespace))
		ensureOwnedNamespace(ctx, clusterProxy.GetClient(), e2eNamespace, runID)

		// Golden DataSource lives in the same namespace — no cross-namespace
		// CDI clone RBAC needed (same-namespace csi-clone is transparent).

		// Fabric setup is provider-specific. The KubeVirt lane needs the
		// ovs-cni vlan200 NAD created before ApplyClusterTemplateAndWait so the
		// VMs can attach at creation time; the AWS lane uses the real VPC and
		// needs no pre-create network object. Gated on the configured provider
		// (E2E_INFRASTRUCTURE_PROVIDER, default kubevirt) so the AWS lane is a
		// clean no-op here rather than a second condition piled inline.
		By(fmt.Sprintf("ensuring provider fabric for %s", e2econfig.InfrastructureProvider()))
		ensureFabric(ctx, clusterProxy.GetClient(), e2eNamespace, runID)
		if e2econfig.InfrastructureProvider() == "kubevirt" {
			By("ensuring control-plane LoadBalancer source routing on the VM fabric")
			ensureKubeVirtControlPlaneLBRoutes(ctx, clusterProxy.GetClient())
		}

		result = &clusterctl.ApplyClusterTemplateAndWaitResult{}
		existingCluster, createCluster, err := retainedClusterCreateDecision(
			ctx,
			clusterProxy.GetClient(),
			types.NamespacedName{Namespace: e2eNamespace, Name: clusterName},
		)
		Expect(err).NotTo(HaveOccurred())
		if !createCluster {
			By(fmt.Sprintf("reusing healthy retained cluster %s/%s", existingCluster.Namespace, existingCluster.Name))
			result.Cluster = existingCluster
			return
		}

		controlPlaneCount := int64(1)
		workerCount, err := capiWorkerMachineCount(multiOwner, e2econfig.InfrastructureProvider())
		Expect(err).NotTo(HaveOccurred())
		// The CNI manifest path in the e2e-config is written relative to the
		// config file's directory (test/e2e/), but the CAPI framework os.ReadFile's
		// it raw against the process CWD — which the mage wrapper sets to the repo
		// root. Resolve it against the config dir so it works regardless of CWD,
		// for both the AWS and KubeVirt lanes.
		cniManifestPath := e2eConfig.MustGetVariable(cniPathVariable)
		if !filepath.IsAbs(cniManifestPath) {
			cniManifestPath = filepath.Join(filepath.Dir(e2eConfigPath), cniManifestPath)
		}
		By(fmt.Sprintf("applying clusterctl template: cluster=%s namespace=%s (CP=%d workers=%d)", clusterName, e2eNamespace, controlPlaneCount, workerCount))
		clusterctl.ApplyClusterTemplateAndWait(ctx, clusterctl.ApplyClusterTemplateAndWaitInput{
			ClusterProxy: clusterProxy,
			ConfigCluster: clusterctl.ConfigClusterInput{
				LogFolder:                filepath.Join(artifactDir, "clusterctl"),
				ClusterctlConfigPath:     clusterctlConfigPath,
				KubeconfigPath:           clusterProxy.GetKubeconfigPath(),
				InfrastructureProvider:   e2econfig.InfrastructureProvider(),
				Flavor:                   e2econfig.Flavor(),
				Namespace:                e2eNamespace,
				ClusterName:              clusterName,
				KubernetesVersion:        e2econfig.KubernetesVersion(),
				ControlPlaneMachineCount: &controlPlaneCount,
				WorkerMachineCount:       &workerCount,
			},
			CNIManifestPath:              cniManifestPath,
			WaitForClusterIntervals:      e2eConfig.GetIntervals("default", "wait-cluster"),
			WaitForControlPlaneIntervals: e2eConfig.GetIntervals("default", "wait-control-plane"),
			WaitForMachineDeployments:    e2eConfig.GetIntervals("default", "wait-worker-nodes"),
		}, result)
	})

	AfterAll(func() {
		if cancel != nil {
			defer cancel()
		}
		if clusterProxy != nil {
			defer disposeClusterProxy(clusterProxy)
		}
		if workloadCluster != nil {
			defer disposeClusterProxy(workloadCluster)
		}

		if staticProvider {
			// Static provider cleanup safety: the cluster is NOT ours. NEVER call
			// framework.DeleteClusterAndWait / DeleteAllClustersAndWait /
			// cleanupAWSCRSAddons here — only the read-only pre-teardown inventory
			// runs. Driver release + run-labeled object removal is the explicit
			// `mage e2e:staticDown` target, never an implicit suite teardown.
			if inventory := preTeardownInventoryOperation(artifactDir, workloadCluster, workloadClient); inventory != nil {
				if err := runCAPIWorkloadCleanup(capiCleanupOperations{inventory: inventory}); err != nil {
					Fail(fmt.Sprintf("static workload inventory completed with errors: %v", err))
				}
			}
			return
		}

		var cleanupErr error
		if clusterProxy != nil && cleanupOnly {
			cleanupErr = errors.Join(cleanupErr, runCAPIWorkloadCleanup(capiCleanupOperations{
				inventory: preTeardownInventoryOperation(artifactDir, workloadCluster, workloadClient),
				delete: func(ctx context.Context) error {
					By("destroying per-run workload clusters through the CAPI E2E framework")
					return runFrameworkCleanup(func() {
						framework.DeleteAllClustersAndWait(ctx, framework.DeleteAllClustersAndWaitInput{
							ClusterProxy:         clusterProxy,
							ClusterctlConfigPath: clusterctlConfigPath,
							Namespace:            e2eNamespace,
							ArtifactFolder:       artifactDir,
						}, deleteClusterIntervals...)
					})
				},
				orphanScan: awsOrphanScanOperation(artifactDir, clusterProxy.GetKubeconfigPath()),
			}))
			// Do NOT delete e2eNamespace — it is shared across runs and holds
			// the golden DataSource. Cluster resources are deleted by name above.
		}
		if result != nil && result.Cluster != nil && !skipCleanup {
			cleanupErr = errors.Join(cleanupErr, runCAPIWorkloadCleanup(capiCleanupOperations{
				inventory: preTeardownInventoryOperation(artifactDir, workloadCluster, workloadClient),
				delete: func(ctx context.Context) error {
					By("destroying the per-run workload cluster through the CAPI E2E framework")
					return runFrameworkCleanup(func() {
						framework.DeleteClusterAndWait(ctx, framework.DeleteClusterAndWaitInput{
							ClusterProxy:         clusterProxy,
							ClusterctlConfigPath: clusterctlConfigPath,
							Cluster:              result.Cluster,
							ArtifactFolder:       artifactDir,
						}, deleteClusterIntervals...)
					})
				},
				orphanScan: awsOrphanScanOperation(artifactDir, clusterProxy.GetKubeconfigPath()),
			}))
		}
		if cleanupErr != nil {
			Fail(fmt.Sprintf("workload cleanup completed with errors: %v", cleanupErr))
		}
	})

	It("retrieves the workload cluster kubeconfig", func() {
		if cleanupOnly {
			return
		}
		if staticProvider {
			Expect(workloadCluster).NotTo(BeNil())
			Expect(workloadCluster.GetKubeconfigPath()).NotTo(BeEmpty())
			Expect(workloadClient.List(ctx, &corev1.NodeList{})).To(Succeed())
			return
		}
		Expect(result).NotTo(BeNil())
		Expect(result.Cluster).NotTo(BeNil())
		workloadCluster = clusterProxy.GetWorkloadCluster(ctx, result.Cluster.Namespace, result.Cluster.Name)
		workloadClient = workloadCluster.GetClient()
		Expect(workloadCluster.GetKubeconfigPath()).NotTo(BeEmpty())
		Expect(workloadClient.List(ctx, &corev1.NodeList{})).To(Succeed())
	})

	It("discovers storage owners and prepares their pools", func() {
		if cleanupOnly {
			return
		}
		Expect(workloadClient).NotTo(BeNil())
		if multiOwner {
			By(fmt.Sprintf("discovering configured storage owners for cluster %s/%s", e2eNamespace, clusterName))
			By("ensuring an ECR pull secret in the default namespace for owner discovery pods")
			Expect(ensureECRPullSecretForNamespace(ctx, workloadCluster.GetKubeconfigPath(), preflightImageFromEnv(), "default")).To(Succeed())
			var diskResolver ownerDiskResolver = staticDeviceResolver{}
			if e2econfig.InfrastructureProvider() == "aws" {
				var err error
				diskResolver, err = newAWSAttachmentResolver(ctx)
				Expect(err).NotTo(HaveOccurred())
			}
			// Owner→node resolution is provider-specific: CAPI lanes walk Machine
			// objects on the management cluster; the static provider resolves the
			// owner's nodeSelector label directly against the WORKLOAD cluster (no
			// Machines exist). The management client is unused by the static
			// resolver, so the workload client stands in for it there.
			var machineResolver ownerMachineResolver = capiOwnerMachineResolver{}
			mgmtClient := workloadClient
			if staticProvider {
				machineResolver = staticNodeResolver{workload: workloadClient}
			} else {
				mgmtClient = clusterProxy.GetClient()
			}
			runner := kubectlNodeRunner{kubeconfig: workloadCluster.GetKubeconfigPath(), namespace: "default", image: preflightImageFromEnv()}
			Eventually(func() error {
				var err error
				storageOwners, err = resolveStorageOwners(ctx, mgmtClient, workloadClient, e2eNamespace, clusterName, machineResolver, diskResolver, runner)
				return err
			}, 5*time.Minute, 10*time.Second).Should(Succeed())
			workers, err := e2econfig.ConsumerWorkers()
			Expect(err).NotTo(HaveOccurred())
			Expect(applyOwnerRolesAndConsumerDomains(ctx, workloadClient, storageOwners, workers)).To(Succeed())
			Expect(prepareOwnerPools(ctx, workloadClient, "default", storageOwners, runner, e2eOwnershipLabels(runID))).To(Succeed())
			storage = storageOwners[0].Node
			driverImage, err = driverImageFromEnv()
			Expect(err).NotTo(HaveOccurred())
			return
		}
		By(fmt.Sprintf("discovering storage node for cluster %s/%s", e2eNamespace, clusterName))
		var storageNodeName string
		Eventually(func() error {
			var derr error
			storageNodeName, derr = discoverStorageNodeName(ctx, clusterProxy.GetClient(), e2eNamespace, clusterName)
			return derr
		}, 5*time.Minute, 10*time.Second).Should(Succeed())
		By(fmt.Sprintf("labeling+tainting storage node %s", storageNodeName))
		Expect(applyStorageRole(ctx, workloadClient, storageNodeName)).To(Succeed())
		var err error
		storage, err = discoverStorageNode(ctx, workloadClient)
		Expect(err).NotTo(HaveOccurred())
		driverImage, err = driverImageFromEnv()
		Expect(err).NotTo(HaveOccurred())

		pool := storagePoolFromEnv()

		// The pool-create + preflight pods run in the default namespace using the
		// (ECR) preflight/driver image, before the driver install mints the ECR
		// secret in the driver namespace. On the AWS lane the CAPA AMI has no
		// ecr-credential-provider, so without a pull secret here the pool pod sits
		// in ImagePullBackOff. Mint it in default/default up front (no-op off ECR).
		By("ensuring an ECR pull secret in the default namespace for setup pods")
		Expect(ensureECRPullSecretForNamespace(ctx, workloadCluster.GetKubeconfigPath(), preflightImageFromEnv(), "default")).To(Succeed())

		By(fmt.Sprintf("creating ZFS pool %s on node %s", pool, storage.Name))
		create := createPoolPod("default", storage, pool)
		Expect(deleteIfExists(ctx, workloadClient, create)).To(Succeed())
		Expect(workloadClient.Create(ctx, create)).To(Succeed())
		Expect(waitForPodSucceeded(ctx, workloadClient, types.NamespacedName{Namespace: create.Namespace, Name: create.Name}, 5*time.Minute)).To(Succeed())

		By(fmt.Sprintf("preflight: asserting pool %s healthy", pool))
		preflight := poolPreflightPod("default", storage, pool)
		Expect(deleteIfExists(ctx, workloadClient, preflight)).To(Succeed())
		Expect(workloadClient.Create(ctx, preflight)).To(Succeed())
		Expect(waitForPodSucceeded(ctx, workloadClient, types.NamespacedName{Namespace: preflight.Namespace, Name: preflight.Name}, 5*time.Minute)).To(Succeed())
	})

	It("deploys the real driver image and runs NFS and NVMe-TCP PVC smokes", func() {
		if cleanupOnly {
			return
		}
		Expect(workloadClient).NotTo(BeNil())
		Expect(storage.Name).NotTo(BeEmpty())
		if driverImage == "" {
			var err error
			driverImage, err = driverImageFromEnv()
			Expect(err).NotTo(HaveOccurred())
		}

		if e2econfig.EncryptionEnabled() {
			By("deploying OpenBao (dev-mode Transit) for per-volume ZFS encryption")
			Expect(ensureOpenBaoInfra(ctx, workloadCluster.GetKubeconfigPath())).To(Succeed())
		}
		if staticProvider {
			var classes storagev1.StorageClassList
			err := workloadClient.List(ctx, &classes)
			Expect(err).NotTo(HaveOccurred())
			staticDefaultStorageClasses = defaultStorageClassNames(classes.Items)
			staticZFSDefaultClasses = zfsDefaultStorageClassNames(classes.Items)
		}

		// SC-name overrides (shared clusters): rename chart classes so they can
		// never collide with pre-existing same-named classes owned by another
		// driver. Empty when unset (identity, current behaviour).
		scOverrides, err := storageClassHelmOverrides()
		Expect(err).NotTo(HaveOccurred())

		if multiOwner {
			valuesPath := filepath.Join(artifactDir, "storage-owners-values.yaml")
			Expect(installMultiOwnerDriverFromChart(ctx, workloadCluster.GetKubeconfigPath(), driverChartRef(), driverImage, valuesPath, storageOwners, scOverrides)).To(Succeed())
			Eventually(func() error { return waitForMultiOwnerDriverReady(ctx, workloadClient, storageOwners) }, 10*time.Minute, 10*time.Second).Should(Succeed())
		} else {
			Expect(installDriverFromChart(ctx, workloadCluster.GetKubeconfigPath(), driverChartRef(), driverImage, storage, scOverrides)).To(Succeed())
			Expect(waitForDriverReady(ctx, workloadClient)).To(Succeed())
		}
		if staticProvider {
			workers, err := e2econfig.ConsumerWorkers()
			Expect(err).NotTo(HaveOccurred())
			Expect(workers).NotTo(BeEmpty())
			workers = workers[:1]
			By("reconciling zfs-csi topology labels with static node-plugin placement")
			Eventually(func() error { return reconcileStaticConsumerTopology(ctx, workloadClient, workers) }, 2*time.Minute, 5*time.Second).Should(Succeed())
			By("constraining Helm-owned zfs-csi StorageClasses to the node-plugin network domain")
			Expect(constrainStaticStorageClasses(ctx, workloadClient, []string{workers[0].NetworkDomain})).To(Succeed())
			By("verifying zfs-csi did not claim or change default StorageClasses")
			Expect(assertStaticStorageClassDefaults(ctx, workloadClient, staticDefaultStorageClasses, staticZFSDefaultClasses)).To(Succeed())
		}
		By("deploying external-snapshotter + VolumeSnapshotClass (snapshot infra)")
		Expect(ensureSnapshotInfra(ctx, workloadCluster.GetKubeconfigPath())).To(Succeed())

		nfsSC, err := smokeStorageClassName("zfs-tank-nfs")
		Expect(err).NotTo(HaveOccurred())
		nvmeSC, err := smokeStorageClassName("zfs-tank-nvme")
		Expect(err).NotTo(HaveOccurred())

		By("proving the NFS RWX path: concurrent cross-node writer+reader")
		Expect(nfsRwxSmoke(ctx, workloadClient, "default", nfsSC)).To(Succeed())

		By("proving the NVMe-TCP RWO path: attach + write/read on a non-storage node")
		Expect(nvmeSmoke(ctx, workloadClient, "default", nvmeSC)).To(Succeed())

		if e2econfig.TransportTLSEnabled() {
			nfsTLSSC, err := smokeStorageClassName("zfs-tank-nfs-tls")
			Expect(err).NotTo(HaveOccurred())
			nvmeTLSSC, err := smokeStorageClassName("zfs-tank-nvme-tls")
			Expect(err).NotTo(HaveOccurred())
			By("proving the transport-TLS NFS RWX path")
			Expect(nfsRwxSmoke(ctx, workloadClient, "default", nfsTLSSC)).To(Succeed())
			By("proving the transport-TLS NVMe-TCP RWO path")
			Expect(nvmeSmoke(ctx, workloadClient, "default", nvmeTLSSC)).To(Succeed())
		}

	})

	It("proves stable VolumeAttributesClass mutation, no-op compatibility, and deletion protection", func() {
		if cleanupOnly {
			return
		}
		Expect(workloadClient).NotTo(BeNil())
		Expect(workloadCluster).NotTo(BeNil())
		By("verifying stable VolumeAttributesClass API registration")
		Expect(ensureVolumeAttributesClassInfra(ctx, workloadCluster.GetKubeconfigPath())).To(Succeed())
		By("changing live ZFS compression through a VolumeAttributesClass")
		vacSC, err := smokeStorageClassName("zfs-tank-nvme")
		Expect(err).NotTo(HaveOccurred())
		Expect(runVolumeAttributesClassScenario(ctx, workloadClient, workloadCluster.GetKubeconfigPath(), "default", vacSC)).To(Succeed())
	})

	It("proves scheduler volume limits and same-node NVMe access semantics", func() {
		if cleanupOnly {
			return
		}
		if e2econfig.InfrastructureProvider() == "static" {
			// The scenario reinstaller performs a single-owner helm upgrade that
			// would replace the static lane's multi-owner release values and
			// re-render default-named StorageClasses on the shared cluster.
			Skip("volume-limit scenario reinstalls the driver with single-owner values; unsafe on a shared static cluster")
		}
		Expect(workloadClient).NotTo(BeNil())
		Expect(workloadCluster).NotTo(BeNil())
		Expect(runStorageFeatureScenarios(
			ctx,
			workloadClient,
			"default",
			func(installCtx context.Context, overrides map[string]string) error {
				if err := installDriverFromChart(installCtx, workloadCluster.GetKubeconfigPath(), driverChartRef(), driverImage, storage, overrides); err != nil {
					return err
				}
				return waitForDriverReady(installCtx, workloadClient)
			},
		)).To(Succeed())
	})

	It("imports pre-existing ZFS filesystem and block backends", func() {
		if cleanupOnly {
			return
		}
		Expect(workloadClient).NotTo(BeNil())
		Expect(runVolumeImportScenario(ctx, workloadClient, workloadCluster.GetKubeconfigPath(), storage)).To(Succeed())
	})

	// It("persists backend health across target repair", Label("health"), func() {
	// 	if cleanupOnly {
	// 		return
	// 	}
	// 	Expect(workloadClient).NotTo(BeNil())
	// 	Expect(workloadCluster).NotTo(BeNil())
	// 	Expect(runDurableHealthScenario(ctx, workloadClient, workloadCluster.GetKubeconfigPath(), "default", storage)).To(Succeed())
	// })

	It("runs the upstream external-storage conformance suite", func() {
		if cleanupOnly {
			return
		}
		if !e2econfig.RunConformance() {
			Skip("conformance is opt-in; set E2E_RUN_CONFORMANCE=1 to run the external-storage suite (~30-60m)")
		}
		Expect(workloadCluster).NotTo(BeNil())
		if staticProvider {
			By("rechecking shared-cluster StorageClass defaults immediately before conformance")
			Expect(assertStaticStorageClassDefaults(ctx, workloadClient, staticDefaultStorageClasses, staticZFSDefaultClasses)).To(Succeed())
		}
		// The preceding VAC scenario removes its test classes during cleanup. Restore
		// the shared class here because the external suite copies it per namespace.
		Expect(ensureVolumeAttributesClassInfra(ctx, workloadCluster.GetKubeconfigPath())).To(Succeed())

		// Resolve baseline testdriver manifests to absolute host paths; the runner
		// mounts them into the conformance container. CWD is the repo root (mage
		// builds + runs e2e.test from there).
		driverFiles, err := conformanceTestDriverFiles(e2econfig.ConformanceTLSOnly(), e2econfig.EncryptionEnabled(), e2econfig.TransportTLSEnabled())
		Expect(err).NotTo(HaveOccurred())
		testDrivers := make([]string, 0, len(driverFiles))
		for _, driverFile := range driverFiles {
			driverPath, err := filepath.Abs(filepath.Join("test", "e2e", "data", "testdriver", driverFile))
			Expect(err).NotTo(HaveOccurred())
			testDrivers = append(testDrivers, driverPath)
		}
		// With SC-name overrides active, generate rewritten testdriver copies into
		// _artifacts so FromExistingClassName matches the renamed chart classes.
		// Identity (baseline manifests) when no overrides are configured.
		testDrivers, err = materializeTestDrivers(artifactDir, testDrivers)
		Expect(err).NotTo(HaveOccurred())
		// Conformance gets its OWN long-lived context, NOT the shared 90m
		// provisioning budget (ctx). The inner ginkgo --timeout is 360m; the
		// outer Go context must exceed it so the container runs to completion
		// and writes its JUnit report (otherwise a mid-run context cancel kills
		// docker before the report is emitted and every spec looks "lost").
		confCtx, confCancel := context.WithTimeout(context.Background(), 370*time.Minute)
		defer confCancel()
		// SSH key: optional for static non-disruptive runs (Validate enforces the
		// key everywhere else); when a path is configured it must be readable.
		var sshPrivateKey []byte
		if keyPath := e2econfig.SSHPrivateKeyPath(); keyPath != "" {
			sshPrivateKey, err = os.ReadFile(keyPath)
			Expect(err).NotTo(HaveOccurred())
		}
		sshBastion := ""
		if !staticProvider {
			sshBastion, err = conformanceSSHBastion(ctx, clusterProxy.GetClient(), e2eNamespace, clusterName, e2econfig.InfrastructureProvider())
			Expect(err).NotTo(HaveOccurred())
		}
		// Skip default: static runs on a shared cluster exclude Disruptive|Serial
		// unless explicitly opted in (E2E_CONFORMANCE_DISRUPTIVE=1). An explicit
		// E2E_CONFORMANCE_SKIP always wins.
		conformanceSkip := e2econfig.ConformanceSkip()
		if conformanceSkip == "" && staticProvider && !e2econfig.ConformanceDisruptive() {
			conformanceSkip = conformanceStaticDefaultSkip
		}
		allowedNotReady, err := e2econfig.AllowedNotReadyNodes()
		Expect(err).NotTo(HaveOccurred())

		repoRoot, err := filepath.Abs(".")
		Expect(err).NotTo(HaveOccurred())
		metadata, err := newRunMetadata(runID, clusterName, driverImage, testDrivers, storage, GinkgoRandomSeed(), repoRoot)
		Expect(err).NotTo(HaveOccurred())
		Expect(writeRunMetadata(filepath.Join(artifactDir, "conformance-run-metadata.json"), metadata)).To(Succeed())

		By(fmt.Sprintf("running External.Storage conformance against %d testdrivers", len(testDrivers)))
		confInput := conformanceInput{
			ClusterProxy:         workloadCluster,
			KubernetesVersion:    e2econfig.KubernetesVersion(),
			ConformanceImage:     e2econfig.ConformanceImage(),
			ArtifactsDirectory:   artifactDir,
			ClusterName:          clusterName,
			TestDriverManifests:  testDrivers,
			Focus:                e2econfig.ConformanceFocus(),
			Skip:                 conformanceSkip,
			DryRun:               e2econfig.ConformanceDryRun(),
			SSHPrivateKey:        sshPrivateKey,
			SSHUser:              e2econfig.SSHUser(),
			SSHBastion:           sshBastion,
			NonBlockingTaints:    e2econfig.NonBlockingTaints(),
			AllowedNotReadyNodes: allowedNotReady,
			AfterRun: func(diagnosticsCtx context.Context) error {
				return writeConformanceDiagnostics(
					diagnosticsCtx,
					artifactDir,
					workloadCluster,
					workloadClient,
				)
			},
		}
		conformanceErr := runStorageConformance(confCtx, confInput)
		Expect(conformanceErr).To(Succeed())
	})

	It("accepts direct PodCertificate NFS mTLS", Label("pod-certificate-acceptance"), func() {
		if cleanupOnly {
			return
		}
		if !e2econfig.PodCertificateAcceptanceEnabled() {
			Skip("PodCertificate acceptance is opt-in; set E2E_POD_CERTIFICATE_ACCEPTANCE=1")
		}
		provider := e2econfig.InfrastructureProvider()
		Expect(provider).To(BeElementOf("aws", "static"))
		Expect(e2econfig.TransportTLSEnabled()).To(BeTrue(), "acceptance requires E2E_TRANSPORT_TLS=1")
		bootstrap, err := podCertificateAcceptanceBootstrap(provider, storageOwners)
		Expect(err).NotTo(HaveOccurred())
		if workloadCluster == nil {
			Expect(result).NotTo(BeNil())
			Expect(result.Cluster).NotTo(BeNil())
			workloadCluster = clusterProxy.GetWorkloadCluster(ctx, result.Cluster.Namespace, result.Cluster.Name)
			workloadClient = workloadCluster.GetClient()
		}
		if driverImage == "" {
			var err error
			driverImage, err = driverImageFromEnv()
			Expect(err).NotTo(HaveOccurred())
		}
		if bootstrap {
			diskResolver, err := newAWSAttachmentResolver(ctx)
			Expect(err).NotTo(HaveOccurred())
			storageOwners, err = resolveStorageOwners(ctx, clusterProxy.GetClient(), workloadClient, e2eNamespace, clusterName, capiOwnerMachineResolver{}, diskResolver, kubectlNodeRunner{kubeconfig: workloadCluster.GetKubeconfigPath(), namespace: "default", image: preflightImageFromEnv()})
			Expect(err).NotTo(HaveOccurred())
			workers, err := e2econfig.ConsumerWorkers()
			Expect(err).NotTo(HaveOccurred())
			Expect(applyOwnerRolesAndConsumerDomains(ctx, workloadClient, storageOwners, workers)).To(Succeed())
			Expect(prepareOwnerPools(ctx, workloadClient, "default", storageOwners, kubectlNodeRunner{kubeconfig: workloadCluster.GetKubeconfigPath(), namespace: "default", image: preflightImageFromEnv()}, e2eOwnershipLabels(runID))).To(Succeed())
			storage = storageOwners[0].Node

			scOverrides, err := storageClassHelmOverrides()
			Expect(err).NotTo(HaveOccurred())
			valuesPath := filepath.Join(artifactDir, "storage-owners-values.yaml")
			Expect(installMultiOwnerDriverFromChart(ctx, workloadCluster.GetKubeconfigPath(), driverChartRef(), driverImage, valuesPath, storageOwners, scOverrides)).To(Succeed())
			Eventually(func() error { return waitForMultiOwnerDriverReady(ctx, workloadClient, storageOwners) }, 10*time.Minute, 10*time.Second).Should(Succeed())
		}
		Expect(runPodCertificateAcceptance(ctx, workloadClient, workloadCluster.GetKubeconfigPath(), artifactDir, storage)).To(Succeed())
	})

	RegisterPerformanceAcceptance(func() error {
		if cleanupOnly {
			return nil
		}
		return RunPerformanceAcceptance(ctx, artifactDir, workloadCluster, workloadClient, storage, driverImage)
	})
})

// podCertificateAcceptanceBootstrap identifies whether acceptance must prepare
// AWS-only prerequisites. Static acceptance depends on the earlier Ordered
// lifecycle specs and must fail explicitly when focused in isolation: it may
// not guess owner mappings or mutate a shared driver's release.
func podCertificateAcceptanceBootstrap(provider string, owners []storageOwner) (bool, error) {
	switch provider {
	case "aws":
		return len(owners) == 0, nil
	case "static":
		if len(owners) == 0 {
			return false, fmt.Errorf("static PodCertificate acceptance requires the Ordered owner-discovery and driver-install specs; refusing standalone run with empty storage owners")
		}
		return false, nil
	default:
		return false, fmt.Errorf("PodCertificate acceptance unsupported for provider %q", provider)
	}
}

func validateCAPILifecycleSubstrate() (bool, error) {
	if err := e2econfig.ValidateStaticProviderSubstrate(); err != nil {
		return false, err
	}
	return e2econfig.IsCleanupOnly(), nil
}

func capiWorkerMachineCount(explicitInfrastructure bool, provider string) (int64, error) {
	if explicitInfrastructure {
		workers, err := e2econfig.ConsumerWorkers()
		if err != nil {
			return 0, err
		}
		var count int64
		for _, worker := range workers {
			if worker.Replicas < 1 {
				return 0, fmt.Errorf("consumer worker group %q has invalid replica count %d", worker.Name, worker.Replicas)
			}
			count += int64(worker.Replicas)
		}
		return count, nil
	}
	if provider == "aws" {
		return 2, nil
	}
	return 3, nil
}

func retainedClusterCreateDecision(
	ctx context.Context,
	c client.Client,
	key types.NamespacedName,
) (*clusterv1.Cluster, bool, error) {
	cluster := &clusterv1.Cluster{}
	if err := c.Get(ctx, key, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("get retained cluster %s: %w", key, err)
	}
	if !cluster.DeletionTimestamp.IsZero() {
		return nil, false, fmt.Errorf("retained cluster %s is deleting", key)
	}
	if cluster.Status.GetTypedPhase() != clusterv1.ClusterPhaseProvisioned {
		return nil, false, fmt.Errorf("retained cluster %s phase is %q, want %q", key, cluster.Status.Phase, clusterv1.ClusterPhaseProvisioned)
	}
	for _, condition := range cluster.Status.Conditions {
		if condition.Type == clusterv1.ClusterAvailableCondition && condition.Status == metav1.ConditionTrue {
			return cluster, false, nil
		}
	}
	return nil, false, fmt.Errorf("retained cluster %s is not Available", key)
}

func writeConformanceDiagnostics(
	ctx context.Context,
	artifactDir string,
	workload framework.ClusterProxy,
	workloadClient client.Client,
) error {
	return writePreTeardownInventory(
		ctx,
		filepath.Join(artifactDir, "post-conformance-workload-diagnostics.yaml"),
		workload.GetKubeconfigPath(),
		workloadClient,
	)
}

type capiCleanupOperations struct {
	inventory  func(context.Context) error
	delete     func(context.Context) error
	orphanScan func(context.Context) error
}

func runCAPIWorkloadCleanup(operations capiCleanupOperations) error {
	steps := []struct {
		name    string
		timeout time.Duration
		run     func(context.Context) error
	}{
		{name: "pre-teardown inventory", timeout: preTeardownInventoryTimeout, run: operations.inventory},
		{name: "cluster deletion", timeout: clusterDeleteTimeout, run: operations.delete},
		{name: "AWS orphan scan", timeout: awsOrphanScanTimeout, run: operations.orphanScan},
	}

	var errs []error
	for _, step := range steps {
		if step.run == nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), step.timeout)
		err := step.run(ctx)
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", step.name, err))
		}
	}

	return errors.Join(errs...)
}

func preTeardownInventoryOperation(artifactDir string, workload framework.ClusterProxy, workloadClient client.Client) func(context.Context) error {
	if workload == nil || workloadClient == nil {
		return nil
	}
	return func(ctx context.Context) error {
		By("capturing workload leak inventory before cluster teardown")
		return writePreTeardownInventory(
			ctx,
			filepath.Join(artifactDir, "pre-teardown-workload-inventory.yaml"),
			workload.GetKubeconfigPath(),
			workloadClient,
		)
	}
}

func awsOrphanScanOperation(artifactDir, kubeconfig string) func(context.Context) error {
	return func(ctx context.Context) error {
		By("scanning for AWS orphans after cluster teardown")
		return runAWSOrphanScan(ctx, artifactDir, kubeconfig)
	}
}

// runFrameworkCleanup turns Gomega failures inside Cluster API's void cleanup
// helpers into errors so the orphan scan still runs and artifacts are retained.
func runFrameworkCleanup(cleanup func()) error {
	failures := InterceptGomegaFailures(cleanup)
	if len(failures) == 0 {
		return nil
	}

	return errors.New(strings.Join(failures, "; "))
}

func disposeClusterProxy(proxy framework.ClusterProxy) {
	ctx, cancel := context.WithTimeout(context.Background(), preTeardownInventoryTimeout)
	defer cancel()
	proxy.Dispose(ctx)
}

func cleanupAWSCRSAddons(ctx context.Context, c client.Client, namespace, runID string) {
	ownership := e2eOwnershipLabels(runID)
	crs := &unstructured.Unstructured{}
	crs.SetAPIVersion("addons.cluster.x-k8s.io/v1beta1")
	crs.SetKind("ClusterResourceSet")
	crs.SetNamespace(namespace)
	crs.SetName("crs-ccm")
	if err := deleteOwnedObject(ctx, c, crs, ownership); err != nil {
		Expect(err).NotTo(HaveOccurred(), "pre-clean leftover crs-ccm ClusterResourceSet")
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "cloud-controller-manager-addon", Namespace: namespace},
	}
	if err := deleteOwnedObject(ctx, c, cm, ownership); err != nil {
		Expect(err).NotTo(HaveOccurred(), "pre-clean leftover cloud-controller-manager-addon ConfigMap")
	}
}

// Site-local network facts for the KubeVirt lane. These describe the lab the
// harness runs in, not the driver, so they are environment-overridable and the
// committed defaults are RFC 5737/RFC 1918 placeholders rather than any real
// site topology.
func siteEnvOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return fallback
}

func siteMgmtSubnet() string     { return siteEnvOr("E2E_MGMT_SUBNET", "198.51.100.0/24") }
func siteVLANRange() string      { return siteEnvOr("E2E_VLAN_RANGE", "192.0.2.0/24") }
func siteVLANGateway() string    { return siteEnvOr("E2E_VLAN_GATEWAY", "192.0.2.1") }
func siteVLANRangeStart() string { return siteEnvOr("E2E_VLAN_RANGE_START", "192.0.2.2") }
func siteVLANRangeEnd() string   { return siteEnvOr("E2E_VLAN_RANGE_END", "192.0.2.250") }
func siteDNSServer() string      { return siteEnvOr("E2E_DNS_SERVER", "192.0.2.1") }

// ensureVLAN200NAD creates the ovs-cni NetworkAttachmentDefinition "vlan200".
// KubeVirt uses it as the Multus primary network, and its bridge binding
// delegates the IPAM result to the guest over KubeVirt's built-in DHCP. Include
// the default route and DNS server here so the guest can reach kubeadm image
// registries immediately during cloud-init/kubeadm bootstrap.
func ensureVLAN200NAD(ctx context.Context, c client.Client, namespace, runID string) {
	spec := map[string]any{
		"config": fmt.Sprintf(
			`{"cniVersion":"0.4.0","type":"ovs","bridge":"bridge","vlan":200,"ipam":{"type":"whereabouts","range":%q,"gateway":%q,"routes":[{"dst":"0.0.0.0/0"}],"dns":{"nameservers":[%q]},"range_start":%q,"range_end":%q}}`,
			siteVLANRange(), siteVLANGateway(), siteDNSServer(), siteVLANRangeStart(), siteVLANRangeEnd(),
		),
	}
	upsertVLAN200NAD(ctx, c, "kube-system", runID, spec)
	upsertVLAN200NAD(ctx, c, namespace, runID, spec)
}

func upsertVLAN200NAD(ctx context.Context, c client.Client, namespace, runID string, spec map[string]any) {
	nad := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "k8s.cni.cncf.io/v1",
			"kind":       "NetworkAttachmentDefinition",
			"metadata": map[string]any{
				"name":      "vlan200",
				"namespace": namespace,
				"labels":    e2eOwnershipLabels(runID),
			},
			"spec": spec,
		},
	}
	err := c.Create(ctx, nad)
	if err == nil || !apierrors.IsAlreadyExists(err) {
		Expect(err).NotTo(HaveOccurred(), "create vlan200 NAD in %s", namespace)
		return
	}
	// Already exists (e.g. state-file reuse on a partial teardown): update the
	// spec in place so the config is authoritative.
	key := types.NamespacedName{Name: "vlan200", Namespace: namespace}
	existing := &unstructured.Unstructured{}
	existing.SetAPIVersion("k8s.cni.cncf.io/v1")
	existing.SetKind("NetworkAttachmentDefinition")
	Expect(c.Get(ctx, key, existing)).To(Succeed())
	base := existing.DeepCopy()
	existing.SetLabels(e2eOwnershipLabels(runID))
	existing.UnstructuredContent()["spec"] = spec
	Expect(c.Patch(ctx, existing, client.MergeFrom(base))).To(Succeed())
}

// ensureFabric runs provider-specific pre-create fabric setup. It is the seam
// between the CAPI framework (provider-agnostic) and the per-provider network
// prerequisites. The KubeVirt lane needs the ovs-cni vlan200 NAD in place
// before VMs attach; the AWS lane rides the real VPC and needs nothing here.
// Unknown providers are a hard Fail (not a silent default) so a typo in
// E2E_INFRASTRUCTURE_PROVIDER stops the run instead of creating the wrong
// lane's fabric. Validate() rejects unknowns too; this is the belt.
func ensureFabric(ctx context.Context, c client.Client, namespace, runID string) {
	switch e2econfig.InfrastructureProvider() {
	case "aws":
		if _, create, err := retainedClusterCreateDecision(
			ctx,
			c,
			types.NamespacedName{Namespace: namespace, Name: perRunName(runID)},
		); err != nil {
			Expect(err).NotTo(HaveOccurred())
		} else if !create {
			// Retained runs reuse their existing addon objects. Never pre-clean
			// those objects: old CAPA templates may not carry harness ownership.
			return
		}
		// AWS uses the CAPA-managed VPC directly, so there is no pre-create
		// network object. But the flavor's ClusterResourceSet (crs-ccm) and its
		// backing ConfigMap (cloud-controller-manager-addon) have NO ownerReference
		// to the Cluster — a CRS selects clusters by label independently — so
		// deleting the Cluster does not garbage-collect them. On a same-namespace
		// re-provision the framework does a plain create, which fails with
		// "already exists". Pre-delete them so re-provision is idempotent (the AWS
		// analogue of the KubeVirt lane's NAD upsert).
		cleanupAWSCRSAddons(ctx, c, namespace, runID)
		return
	case "kubevirt":
		ensureVLAN200NAD(ctx, c, namespace, runID)
	case "static":
		// The static provider rides a pre-existing cluster: no fabric objects
		// are created (and no management cluster exists to create them on).
		return
	default:
		Fail(fmt.Sprintf("unknown E2E_INFRASTRUCTURE_PROVIDER %q (want kubevirt, aws, or static)", e2econfig.InfrastructureProvider()))
	}
}

func ensureKubeVirtControlPlaneLBRoutes(ctx context.Context, c client.Client) {
	Expect(ensureKubeVirtControlPlaneLBRoutesObject(ctx, c)).To(Succeed())
	daemonSet := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "zfs-csi-e2e-control-plane-lb", Namespace: "kube-system"}}
	Eventually(func(g Gomega) {
		g.Expect(c.Get(ctx, client.ObjectKeyFromObject(daemonSet), daemonSet)).To(Succeed())
		g.Expect(daemonSet.Status.DesiredNumberScheduled).To(BeNumerically(">", 0))
		g.Expect(daemonSet.Status.NumberReady).To(Equal(daemonSet.Status.DesiredNumberScheduled))
	}, 5*time.Minute, 2*time.Second).Should(Succeed())
}

func ensureKubeVirtControlPlaneLBRoutesObject(ctx context.Context, c client.Client) error {
	const name = "zfs-csi-e2e-control-plane-lb"
	daemonSet := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "kube-system"}}
	mutate := func() {
		daemonSet.Labels = map[string]string{
			"app.kubernetes.io/name":       "zfs-csi-e2e",
			"app.kubernetes.io/component":  "control-plane-lb-route",
			"app.kubernetes.io/managed-by": "ginkgo-e2e",
		}
		daemonSet.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"app.kubernetes.io/name": name}}
		daemonSet.Spec.Template.ObjectMeta.Labels = map[string]string{"app.kubernetes.io/name": name}
		daemonSet.Spec.Template.Spec.HostNetwork = true
		daemonSet.Spec.Template.Spec.ServiceAccountName = "zfs-csi-e2e-control-plane-lb"
		daemonSet.Spec.Template.Spec.Tolerations = []corev1.Toleration{{Operator: corev1.TolerationOpExists}}
		daemonSet.Spec.Template.Spec.Containers = []corev1.Container{{
			Name:            "route",
			Image:           preflightImageFromEnv(),
			ImagePullPolicy: corev1.PullAlways,
			SecurityContext: &corev1.SecurityContext{Privileged: func() *bool { value := true; return &value }()},
			Command:         []string{"/bin/sh", "-ec"},
			Args: []string{fmt.Sprintf(`ip route replace %[1]s via %[2]s dev vlan200
trap 'ip route del %[1]s via %[2]s dev vlan200 2>/dev/null || true' EXIT TERM INT
while true; do sleep 3600; done`, siteMgmtSubnet(), siteVLANGateway())},
		}}
	}
	mutate()
	err := c.Create(ctx, daemonSet)
	if apierrors.IsAlreadyExists(err) {
		if err := c.Get(ctx, client.ObjectKeyFromObject(daemonSet), daemonSet); err != nil {
			return err
		}
		mutate()
		err = c.Update(ctx, daemonSet)
	}
	return err
}

// ensureOwnedNamespace ensures the shared e2e namespace exists. It is
// idempotent: on reuse it just verifies the namespace is present.
func ensureOwnedNamespace(ctx context.Context, c client.Client, name, runID string) {
	err := c.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: e2eOwnershipLabels(runID),
		},
	})
	if err == nil {
		return
	}
	Expect(apierrors.IsAlreadyExists(err)).To(BeTrue(), "create namespace %q", name)
}

func newScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
	Expect(certificatesv1beta1.AddToScheme(scheme)).To(Succeed())
	Expect(addDriverTypes(scheme)).To(Succeed())
	Expect(clusterv1.AddToScheme(scheme)).To(Succeed())
	Expect(bootstrapv1.AddToScheme(scheme)).To(Succeed())
	Expect(controlplanev1.AddToScheme(scheme)).To(Succeed())
	return scheme
}
