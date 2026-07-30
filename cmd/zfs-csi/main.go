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

// Command zfs-csi is the single binary for the zfs-csi CSI driver. It runs in
// three modes via --mode: controller, storage (server7 agent), node.
//
//	controller  → CSI Identity+Controller gRPC + reconciler manager (leader-elected)
//	storage     → server7 agent: Volume/Snapshot reconcilers (zfs + transport + crypto)
//	node        → CSI Identity+Node gRPC (kubelet-driven)
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/go-logr/logr"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	certificatesv1beta1 "k8s.io/api/certificates/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	mountutils "k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"

	nvmetv1 "github.com/randomvariable/zfs-csi/api/nvmet/v1alpha1"
	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/agent"
	"github.com/randomvariable/zfs-csi/internal/capacity"
	"github.com/randomvariable/zfs-csi/internal/crypto"
	"github.com/randomvariable/zfs-csi/internal/crypto/openbao"
	"github.com/randomvariable/zfs-csi/internal/driver"
	"github.com/randomvariable/zfs-csi/internal/inventory"
	"github.com/randomvariable/zfs-csi/internal/mount"
	mountimpl "github.com/randomvariable/zfs-csi/internal/mount/impl"
	"github.com/randomvariable/zfs-csi/internal/nfsexport"
	"github.com/randomvariable/zfs-csi/internal/nvmet"
	"github.com/randomvariable/zfs-csi/internal/observability"
	"github.com/randomvariable/zfs-csi/internal/podcertificatesigner"
	"github.com/randomvariable/zfs-csi/internal/reachability"
	"github.com/randomvariable/zfs-csi/internal/stage"
	stagepb "github.com/randomvariable/zfs-csi/internal/stagepb/stage"
	"github.com/randomvariable/zfs-csi/internal/tlsca"
	"github.com/randomvariable/zfs-csi/internal/transport"
	"github.com/randomvariable/zfs-csi/internal/zfs"
)

// Mode flags.
const (
	ModeController = "controller"
	ModeStorage    = "storage"
	ModeNode       = "node"
	ModeNVMet      = "nvmet"
	ModeNVMetStage = "nvmet-stage"
	ModeNFSStage   = "nfs-stage"
	ModeTLSSigner  = "tls-signer"

	exitCodeUsage    = 2
	grpcDrainTimeout = 5 * time.Second
	// version is the binary version surfaced via gRPC PluginInfo. Overridable
	// at build time via -ldflags; defaults to a dev sentinel.
	version = "dev"
)

func main() {
	mode := flag.String("mode", "", "operating mode: controller|storage|node|nvmet|nvmet-stage|nfs-stage|tls-signer")
	csiAddr := flag.String("csi-address", "/csi/csi.sock", "UNIX socket for CSI gRPC")
	stageSocket := flag.String("stage-socket", "/stage/stage.sock", "UNIX socket for StagePlugin gRPC (server modes)")
	nvmetStageSocket := flag.String(
		"nvmet-stage-socket",
		"/stage/nvmet.sock",
		"UNIX socket for the nvmet StagePlugin sidecar",
	)
	nfsStageSocket := flag.String("nfs-stage-socket", "/stage/nfs.sock", "UNIX socket for the nfs StagePlugin sidecar")
	namespace := flag.String("namespace", os.Getenv("NAMESPACE"), "driver namespace for namespaced supporting objects")
	portalHost := flag.String(
		"portal-host",
		"",
		"portal host for block transport (required for controller, storage, node, and nvmet modes)",
	)
	nfsServer := flag.String("nfs-server", "", "NFS server host for filesystem volumes (required for node mode)")
	networkDomain := flag.String("network-domain", os.Getenv("NETWORK_DOMAIN"), "consumer reachability domain reported by node/storage modes")
	networkDomainSource := flag.String("network-domain-source", reachability.NodeDomainSourceStatic, "node network domain source: static|nodeLabel")
	networkDomainLabel := flag.String("network-domain-label", reachability.DefaultNodeDomainLabelKey, "Kubernetes Node label used by nodeLabel domain source")
	reachableFrom := flag.String("reachable-from", os.Getenv("REACHABLE_FROM"), "comma-separated consumer domains reachable from storage mode")
	expectedOwner := flag.String("expected-owner", os.Getenv("EXPECTED_OWNER"), "expected Kubernetes Node name for storage mode")
	metricsAddr := flag.String("metrics-bind-address", ":8080", "metrics server bind address")
	healthAddr := flag.String(
		"health-probe-bind-address",
		":8082",
		"health/readiness probe bind address (empty disables probes)",
	)
	enableLeaderElection := flag.Bool("leader-elect", true, "leader election (controller mode only)")
	openbaoAddr := flag.String("openbao-addr", os.Getenv("OPENBAO_ADDR"), "OpenBao API address")
	openbaoToken := flag.String("openbao-token", os.Getenv("OPENBAO_TOKEN"), "OpenBao auth token")
	openbaoRole := flag.String(
		"openbao-role",
		envDefault("OPENBAO_ROLE", "zfs-csi-storage"),
		"OpenBao Kubernetes auth role",
	)
	openbaoJWTPath := flag.String("openbao-kubernetes-jwt-path",
		envDefault("OPENBAO_KUBERNETES_JWT_PATH", "/var/run/secrets/kubernetes.io/serviceaccount/token"),
		"Kubernetes ServiceAccount JWT path for OpenBao auth")
	openbaoMount := flag.String("openbao-mount", "transit", "OpenBao Transit mount path")
	keyTmpfsDir := flag.String("key-tmpfs-dir", "/run/zfs-keys", "tmpfs dir for DEK staging")
	maxConcurrentReconciles := flag.Int("max-concurrent-reconciles", 10, "volume reconciler parallelism (storage mode)")
	maxVolumesPerNode := flag.Int64(
		"max-volumes-per-node",
		128,
		"max attachable volumes per node reported in NodeGetInfo (node mode; 0 = no limit)",
	)
	nvmeCtrlLossTMO := flag.Int(
		"nvme-ctrl-loss-tmo",
		-1,
		"NVMe-oF ctrl_loss_tmo in seconds for the nvmet stage plugin (-1 = retry forever; the kernel default of 600 deletes the controller after a 10m outage)",
	)
	nvmeReconnectDelay := flag.Int(
		"nvme-reconnect-delay",
		10,
		"NVMe-oF reconnect_delay in seconds for the nvmet stage plugin",
	)
	syncPeriod := flag.Duration(
		"sync-period",
		10*time.Minute,
		"manager informer resync interval (periodic drift-correcting reconcile of every object)",
	)
	e2eEnableHealthRepairHold := flag.Bool(
		"e2e-enable-health-repair-hold",
		false,
		"enable the scoped E2E backend-health observation hold (storage mode only)",
	)
	enableVolumeImports := flag.Bool(
		"enable-volume-imports",
		false,
		"enable VolumeImport validation and materialization after all storage agents are retain-aware",
	)
	transportTLS := flag.Bool("transport-tls", false, "enable configured transport TLS runtime support (storage mode)")
	tlsSigningNamespace := flag.String("tls-signing-namespace", "", "namespace holding the private NFS TLS CA (required with transport TLS)")
	tlsServerLeaves := flag.String("tls-server-leaves", "", "comma-separated owner=endpoint server leaves managed by TLS signer")
	tracingEndpoint := flag.String(
		"tracing-otlp-endpoint",
		os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		"OTLP gRPC endpoint (host:port); empty disables tracing",
	)

	// Structured logging: zap JSON encoder by default (production mode, RFC3339
	// timestamps). Output goes to stderr so stdout stays a clean data channel.
	// Development=false selects the JSON encoder. This follows the Kubernetes
	// SIG Instrumentation convention (KEP-1602).
	//
	// NOTE: controller-runtime's zap flag set requires pflag; this binary uses
	// stdlib flag, so options are configured in-code rather than via --zap-*.
	zapOpts := zap.Options{
		Development: false,
		TimeEncoder: zapcore.RFC3339TimeEncoder,
	}

	flag.Parse()

	setupLog := ctrl.Log.WithName("setup")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Initialise the global logr logger BEFORE any component reads it; otherwise
	// controller-runtime's default logger silently discards every record.
	// Development=false selects the JSON encoder.
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))

	// Initialise OpenTelemetry tracing (OTLP exporter). No-op when no endpoint
	// is configured; shutdown MUST be called on exit to flush buffered spans.
	tracerShutdown, err := observability.InitTracer(ctx, setupLog, observability.TracingConfig{
		Endpoint:    *tracingEndpoint,
		ServiceName: "zfs-csi",
	})
	if err != nil {
		setupLog.Error(err, "tracer init failed")
		os.Exit(1)
	}
	defer func() { _ = tracerShutdown(context.Background()) }()

	if *mode == "" {
		fmt.Fprintln(os.Stderr, "--mode is required (controller|storage|node)")
		os.Exit(exitCodeUsage)
	}

	if *namespace == "" {
		*namespace = "zfs-csi-system"
	}
	if *mode == ModeNVMet && *portalHost == "" {
		fmt.Fprintln(os.Stderr, "--portal-host is required for nvmet mode")
		os.Exit(exitCodeUsage)
	}

	switch *mode {
	case ModeController:
		ob := openbaoConfig{
			addr:    *openbaoAddr,
			token:   *openbaoToken,
			role:    *openbaoRole,
			jwtPath: *openbaoJWTPath,
			mount:   *openbaoMount,
		}
		if err := runController(ctx, setupLog, *csiAddr, *namespace, *portalHost, *metricsAddr, *healthAddr, *enableLeaderElection, *syncPeriod, ob); err != nil {
			setupLog.Error(err, "controller exited")
			os.Exit(1)
		}
	case ModeStorage:
		ob := openbaoConfig{
			addr:    *openbaoAddr,
			token:   *openbaoToken,
			role:    *openbaoRole,
			jwtPath: *openbaoJWTPath,
			mount:   *openbaoMount,
		}
		if err := runStorage(
			ctx, setupLog, *namespace, *portalHost, *nfsServer, *networkDomain, *reachableFrom, *expectedOwner, *keyTmpfsDir, *metricsAddr,
			*healthAddr, *maxConcurrentReconciles, *syncPeriod,
			*e2eEnableHealthRepairHold, *enableVolumeImports, *transportTLS, ob,
		); err != nil {
			setupLog.Error(err, "storage agent exited")
			os.Exit(1)
		}
	case ModeNode:
		if err := runNode(ctx, setupLog, *csiAddr, *portalHost, *nfsServer, *nvmetStageSocket, *nfsStageSocket, *maxVolumesPerNode, *networkDomain, *networkDomainSource, *networkDomainLabel); err != nil {
			setupLog.Error(err, "node exited")
			os.Exit(1)
		}
	case ModeNVMet:
		if err := runNVMet(ctx, setupLog, *namespace, *portalHost, *metricsAddr, *healthAddr, *syncPeriod); err != nil {
			setupLog.Error(err, "nvmet controller exited")
			os.Exit(1)
		}
	case ModeNVMetStage:
		if err := runStagePlugin(ctx, setupLog, *stageSocket, "nvmet-stage", *nvmeCtrlLossTMO, *nvmeReconnectDelay); err != nil {
			setupLog.Error(err, "nvmet stage plugin exited")
			os.Exit(1)
		}
	case ModeNFSStage:
		if err := runStagePlugin(ctx, setupLog, *stageSocket, "nfs-stage", *nvmeCtrlLossTMO, *nvmeReconnectDelay); err != nil {
			setupLog.Error(err, "nfs stage plugin exited")
			os.Exit(1)
		}
	case ModeTLSSigner:
		if err := runTLSSigner(ctx, setupLog, *namespace, *tlsSigningNamespace, *tlsServerLeaves, *metricsAddr, *healthAddr, *enableLeaderElection, *syncPeriod); err != nil {
			setupLog.Error(err, "TLS signer exited")
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q\n", *mode)
		os.Exit(exitCodeUsage)
	}
}

// scheme is the runtime.Scheme shared by all modes.
func newScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(zfscsiv1.AddToScheme(s))
	utilruntime.Must(nvmetv1.AddToScheme(s))

	return s
}

func newSignerScheme() *runtime.Scheme {
	s := newScheme()
	utilruntime.Must(certificatesv1beta1.AddToScheme(s))
	return s
}

// newManager builds a controller-runtime manager.
func newManager(
	setupLog logr.Logger,
	metricsAddr, healthAddr string,
	leaderElect bool,
	syncPeriod time.Duration,
) (ctrl.Manager, error) {
	opts := ctrl.Options{
		Scheme:                 newScheme(),
		Metrics:                server.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: healthAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "zfs-csi-controller.randomvariable.co.uk",
	}
	// SyncPeriod is the informer resync interval: controller-runtime re-delivers
	// every watched object every syncPeriod, driving a reconcile that re-applies
	// (idempotent) external state and thereby corrects drift — e.g. an nvmet
	// port->subsystem link lost out of band. This is the correct mechanism for
	// periodic drift correction, NOT a per-object RequeueAfter. Zero leaves the
	// controller-runtime default (10h).
	if syncPeriod > 0 {
		opts.Cache = cache.Options{SyncPeriod: &syncPeriod}
	}

	config := ctrl.GetConfigOrDie()
	mgr, err := ctrl.NewManager(config, opts)
	if err != nil {
		return nil, fmt.Errorf("create manager: %w", err)
	}

	// Health/readiness endpoints (F12): a wedged libzfs cgo call can silently
	// stall provisioning; a ping healthz at least lets the kubelet restart a
	// hard-hung manager process. Registered only when a probe address is set.
	if healthAddr != "" {
		if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
			return nil, fmt.Errorf("add healthz check: %w", err)
		}
		if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
			return nil, fmt.Errorf("add readyz check: %w", err)
		}
	}

	return mgr, nil
}

// openbaoConfig groups OpenBao auth + mount settings shared by controller and
// storage modes.
type openbaoConfig struct {
	addr    string
	token   string
	role    string
	jwtPath string
	mount   string
}

// runTLSSigner owns private authority initialization, public trust publication,
// server-leaf issuance, and PodCertificateRequest status signing.
func runTLSSigner(ctx context.Context, log logr.Logger, driverNamespace, signingNamespace, serverLeaves, metricsAddr, healthAddr string, leaderElect bool, syncPeriod time.Duration) error {
	if err := tlsca.ValidateSigningNamespace(driverNamespace, signingNamespace); err != nil {
		return fmt.Errorf("validate NFS TLS signing namespace: %w", err)
	}
	opts := ctrl.Options{
		Scheme:                 newSignerScheme(),
		Metrics:                server.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: healthAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "zfs-csi-tls-signer.randomvariable.co.uk",
	}
	if syncPeriod > 0 {
		opts.Cache = cache.Options{SyncPeriod: &syncPeriod}
	}
	config := ctrl.GetConfigOrDie()
	mgr, err := ctrl.NewManager(config, opts)
	if err != nil {
		return fmt.Errorf("create TLS signer manager: %w", err)
	}
	if healthAddr != "" {
		if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
			return fmt.Errorf("add TLS signer health check: %w", err)
		}
	}
	authorityClient, err := crclient.New(config, crclient.Options{Scheme: opts.Scheme})
	if err != nil {
		return fmt.Errorf("create uncached TLS authority client: %w", err)
	}
	reconciler := &podcertificatesigner.Reconciler{
		Client:           mgr.GetClient(),
		APIReader:        mgr.GetAPIReader(),
		AuthorityClient:  authorityClient,
		SigningNamespace: signingNamespace,
		DriverNamespace:  driverNamespace,
	}
	owners, err := parseTLSServerLeaves(serverLeaves)
	if err != nil {
		return err
	}
	authority := &podcertificatesigner.AuthorityRunnable{Reconciler: reconciler, Owners: owners, Ready: make(chan struct{})}
	if err := mgr.Add(authority); err != nil {
		return fmt.Errorf("add NFS TLS authority lifecycle: %w", err)
	}
	if healthAddr != "" {
		if err := mgr.AddReadyzCheck("authority", authority.ReadyCheck); err != nil {
			return fmt.Errorf("add TLS signer readiness check: %w", err)
		}
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup PodCertificateRequest signer: %w", err)
	}
	log.Info("starting NFS TLS signer", "signerName", podcertificatesigner.SignerName, "signingNamespace", signingNamespace)
	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("start TLS signer manager: %w", err)
	}
	return nil
}

func parseTLSServerLeaves(value string) (map[string]string, error) {
	leaves := map[string]string{}
	if strings.TrimSpace(value) == "" {
		return leaves, nil
	}
	for entry := range strings.SplitSeq(value, ",") {
		owner, endpoint, ok := strings.Cut(entry, "=")
		owner = strings.TrimSpace(owner)
		endpoint = strings.TrimSpace(endpoint)
		if !ok || endpoint == "" {
			return nil, fmt.Errorf("TLS signer server leaf %q must be owner=endpoint", entry)
		}
		if _, err := tlsca.ServerSecretName(owner); err != nil {
			return nil, fmt.Errorf("TLS signer server leaf owner: %w", err)
		}
		if _, exists := leaves[owner]; exists {
			return nil, fmt.Errorf("TLS signer server leaf owner %q is duplicated", owner)
		}
		leaves[owner] = endpoint
	}
	return leaves, nil
}

// runController starts the Identity+Controller gRPC servers + the manager.
func runController(
	ctx context.Context,
	log logr.Logger,
	csiAddr, namespace, portalHost, metricsAddr, healthAddr string,
	leaderElect bool,
	syncPeriod time.Duration,
	ob openbaoConfig,
) error {
	mgr, err := newManager(log, metricsAddr, healthAddr, leaderElect, syncPeriod)
	if err != nil {
		return fmt.Errorf("new manager: %w", err)
	}
	keys := newKeyProvider(ob.addr, ob.token, ob.role, ob.jwtPath, ob.mount)

	// CSI gRPC servers as manager Runnables.
	csiSrv := newCSIServer(ctx, log,
		driver.NewIdentityServer(nil),
		driver.NewControllerServer(driver.ControllerConfig{
			Log:           log.WithName("controller"),
			Client:        mgr.GetClient(),
			APIReader:     mgr.GetAPIReader(),
			Namespace:     namespace,
			Portal:        portalAddress(portalHost),
			EncryptPrefix: ob.mount + "/",
			Keys:          keys,
		}),
		nil,
	)
	if err := mgr.Add(csiSrv.asRunnable(csiAddr)); err != nil {
		return fmt.Errorf("add csi server runnable: %w", err)
	}
	log.Info("starting controller", "csiAddress", csiAddr, "namespace", namespace)

	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("start manager: %w", err)
	}

	return nil
}

// portalAddress preserves an explicitly port-qualified endpoint while adding
// the NVMe-TCP default port to a bare host. An empty host stays empty so the
// controller can reject only block CreateVolume requests in pure NFS setups.
func portalAddress(host string) string {
	if host == "" {
		return ""
	}
	if _, _, err := net.SplitHostPort(host); err == nil {
		return host
	}
	return net.JoinHostPort(host, "4420")
}

func portalAddressStrict(host string) (string, error) {
	portal := portalAddress(host)
	if portal == "" {
		return "", errors.New("portal host is required")
	}
	if _, _, err := reachability.ParsePortal(portal); err != nil {
		return "", err
	}
	return portal, nil
}

// runStorage starts the Volume+Snapshot reconcilers (no gRPC server).
func runStorage(
	ctx context.Context,
	log logr.Logger,
	namespace, portalHost, nfsServer, networkDomain, reachableFrom, expectedOwner, keyTmpfsDir, metricsAddr, healthAddr string,
	maxConcurrentReconciles int,
	syncPeriod time.Duration,
	e2eEnableHealthRepairHold bool,
	enableVolumeImports bool,
	transportTLS bool,
	ob openbaoConfig,
) error {
	mgr, err := newManager(log, metricsAddr, healthAddr, false, syncPeriod) // no leader election (DaemonSet)
	if err != nil {
		return fmt.Errorf("new manager: %w", err)
	}
	return runStorageWithManager(
		ctx,
		log,
		namespace,
		portalHost,
		nfsServer,
		networkDomain,
		reachableFrom,
		expectedOwner,
		keyTmpfsDir,
		maxConcurrentReconciles,
		e2eEnableHealthRepairHold,
		enableVolumeImports,
		transportTLS,
		ob,
		mgr,
	)
}

func runStorageWithManager(
	ctx context.Context,
	log logr.Logger,
	namespace, portalHost, nfsServer, networkDomain, reachableFrom, expectedOwner, keyTmpfsDir string,
	maxConcurrentReconciles int,
	e2eEnableHealthRepairHold bool,
	enableVolumeImports bool,
	transportTLS bool,
	ob openbaoConfig,
	mgr ctrl.Manager,
) error {
	nodeName := os.Getenv("NODE_NAME")
	if err := validateExpectedOwner(nodeName, expectedOwner); err != nil {
		return err
	}
	configuredReachability := []string{networkDomain}
	if reachableFrom != "" {
		configuredReachability = strings.Split(reachableFrom, ",")
	}
	canonicalReachability, err := reachability.CanonicalReachableFrom(networkDomain, configuredReachability)
	if err != nil {
		return fmt.Errorf("storage reachableFrom: %w", err)
	}

	// nfsd is host-global. Storage startup fails closed when its lifecycle or
	// responder channels cannot be acquired and configured.
	nfsd, err := startNFSDLifecycle(log)
	if err != nil {
		return fmt.Errorf("start nfsd lifecycle: %w", err)
	}
	defer func() { _ = nfsd.stop() }()

	nfsRuntime := newNFSRuntimeComponents(log)
	if err := nfsRuntime.add(mgr.Add, nfsd, nfsexport.CheckRuntimeStructure); err != nil {
		return err
	}

	// Privileged handles. ZFS backend: use the fake when cgo/libzfs unavailable
	// (dev/test); the real libzfs binding compiles only with the `libzfs` tag.
	zfsBackend := zfsBackendFn()
	// Publish per-node ZFS pool capacity. NODE_NAME comes from the downward API
	// (see the storage DaemonSet). Each node owns its own ConfigMap, so multiple
	// agents never contend; skip publication if the node name is missing rather
	// than colliding on a shared object.
	if nodeName != "" {
		capacityPublisher := &capacity.Publisher{
			Client: mgr.GetClient(), ZFS: zfsBackend, Namespace: namespace, Node: nodeName,
			Log: log.WithName("capacity-publisher"),
		}
		if err := mgr.Add(capacityPublisher); err != nil {
			return fmt.Errorf("add capacity publisher: %w", err)
		}
		portal, portalErr := portalAddressStrict(portalHost)
		if portalErr != nil {
			return fmt.Errorf("storage NVMe endpoint: %w", portalErr)
		}
		nvmeHost, nvmePort, _ := reachability.ParsePortal(portal)
		inventoryPublisher := &inventory.Publisher{
			Client: mgr.GetClient(), NodeReader: mgr.GetAPIReader(), ZFS: zfsBackend, NodeName: nodeName,
			Log:           log.WithName("storage-node-inventory-publisher"),
			ReachableFrom: canonicalReachability,
			Endpoints: []zfscsiv1.StorageNodeEndpoint{
				{Protocol: zfscsiv1.StorageProtocolNFS, Host: nfsServer, Port: reachability.DefaultNFSServicePort},
				{Protocol: zfscsiv1.StorageProtocolNVMeTCP, Host: nvmeHost, Port: nvmePort},
			},
		}
		if err := mgr.Add(inventoryPublisher); err != nil {
			return fmt.Errorf("add StorageNode inventory publisher: %w", err)
		}
	} else {
		log.Info("NODE_NAME unset; skipping ZFS capacity publication")
	}
	exportServer := transport.NewNVMET(transport.NewRealWriter()) // writes /sys/kernel/config/nvmet
	keys := newKeyProvider(ob.addr, ob.token, ob.role, ob.jwtPath, ob.mount)
	stager := crypto.NewFileStager(keyTmpfsDir)

	volRec := &agent.VolumeReconciler{
		Client:              mgr.GetClient(),
		Scheme:              mgr.GetScheme(),
		Log:                 log.WithName("volume-reconciler"),
		ZFS:                 zfsBackend,
		Export:              exportServer,
		Keys:                keys,
		Stager:              stager,
		Portal:              portalAddress(portalHost),
		NFSServer:           nfsServer,
		Namespace:           namespace,
		NodeName:            nodeName,
		NFSTLSEnabled:       transportTLS,
		APIReader:           mgr.GetAPIReader(),
		NVMeTLSPSK:          agent.NVMeTLSPSKKeyringProvisioner{},
		NVMeTLSSecretReader: mgr.GetAPIReader(),
		RootProbe:           agent.DefaultRootProbe,

		MaxConcurrentReconciles: maxConcurrentReconciles,
		EnableHealthRepairHold:  e2eEnableHealthRepairHold,
	}
	nfsRuntime.wireVolumeReconciler(volRec)
	if err := volRec.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup volume reconciler: %w", err)
	}
	if enableVolumeImports {
		if nodeName == "" {
			return fmt.Errorf("NODE_NAME is required when volume imports are enabled")
		}
		importRec := &agent.VolumeImportReconciler{
			Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Log: log.WithName("volume-import-reconciler"),
			ZFS: zfsBackend, NodeName: nodeName, PoolResolver: inventory.Resolver{Client: mgr.GetClient()}, MaxConcurrentReconciles: maxConcurrentReconciles,
		}
		if err := importRec.SetupWithManager(mgr); err != nil {
			return fmt.Errorf("setup volume import reconciler: %w", err)
		}
	}

	snapRec := &agent.SnapshotReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Log:      log.WithName("snapshot-reconciler"),
		ZFS:      zfsBackend,
		NodeName: nodeName,
	}
	if err := snapRec.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup snapshot reconciler: %w", err)
	}

	log.Info("starting storage agent", "namespace", namespace, "portalHost", portalHost)

	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("start manager: %w", err)
	}

	return nil
}

type nfsRuntimeComponents struct {
	exports        *nfsexport.MemTable
	responder      *nfsexport.Server
	writer         *nfsexport.ChannelWriter
	rootController *nfsexport.RootController

	workersDone   sync.WaitGroup
	runResponder  func(context.Context) error
	runController func(context.Context)
}

func newNFSRuntimeComponents(log logr.Logger) *nfsRuntimeComponents {
	exports := nfsexport.NewMemTable()
	responder := nfsexport.NewServer(exports, func(format string, args ...any) {
		log.Info(fmt.Sprintf(format, args...), "component", "nfs-export-responder")
	})
	writer := nfsexport.NewChannelWriter(func(format string, args ...any) {
		log.Info(fmt.Sprintf(format, args...), "component", "nfs-export-writer")
	})
	rootController := nfsexport.NewRootController(writer, func(format string, args ...any) {
		log.Info(fmt.Sprintf(format, args...), "component", "nfs-root-controller")
	})
	responder.SetRootController(rootController)
	return &nfsRuntimeComponents{
		exports:        exports,
		responder:      responder,
		writer:         writer,
		rootController: rootController,
		runResponder:   responder.Run,
		runController:  rootController.Run,
	}
}

func (c *nfsRuntimeComponents) wireVolumeReconciler(reconciler *agent.VolumeReconciler) {
	reconciler.NFSExports = c.exports
	reconciler.NFSFlusher = c.responder
	reconciler.NFSWriter = c.writer
	reconciler.NFSRootController = c.rootController
}

// add registers responder, root controller, and ordered nfsd shutdown as one lifecycle.
func (c *nfsRuntimeComponents) add(addRunnable func(manager.Runnable) error, nfsd *nfsdLifecycle, checkRuntime func() error) error {
	if err := checkRuntime(); err != nil {
		return fmt.Errorf("NFS cache channels not ready: %w", err)
	}

	c.workersDone.Add(2)
	if err := addRunnable(manager.RunnableFunc(func(runCtx context.Context) error {
		defer c.workersDone.Done()
		if err := c.runResponder(runCtx); err != nil {
			return fmt.Errorf("run NFS export responder: %w", err)
		}
		return nil
	})); err != nil {
		return fmt.Errorf("add NFS export responder: %w", err)
	}
	if err := addRunnable(manager.RunnableFunc(func(runCtx context.Context) error {
		defer c.workersDone.Done()
		c.runController(runCtx)
		return nil
	})); err != nil {
		return fmt.Errorf("add NFS root controller: %w", err)
	}
	if err := addRunnable(manager.RunnableFunc(func(runCtx context.Context) error {
		<-runCtx.Done()
		c.workersDone.Wait()
		return nfsd.stop()
	})); err != nil {
		return fmt.Errorf("add nfsd shutdown runnable: %w", err)
	}
	return nil
}

// runNVMet starts ONLY the NVMeExport reconciler (sole configfs writer). In the
// Phase 0 deployment topology this runs as a separate DaemonSet alongside the
// storage agent so the nvmet controller can be deployed/updated independently.
func runNVMet(
	ctx context.Context,
	log logr.Logger,
	namespace, portalHost, metricsAddr, healthAddr string,
	syncPeriod time.Duration,
) error {
	mgr, err := newManager(log, metricsAddr, healthAddr, false, syncPeriod)
	if err != nil {
		return fmt.Errorf("new manager: %w", err)
	}

	exportServer := transport.NewNVMET(transport.NewRealWriter())

	exportRec := &nvmet.ExportReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Log:    log.WithName("nvmeexport-reconciler"),
		Export: exportServer,
	}
	if err := exportRec.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup NVMeExport reconciler: %w", err)
	}

	log.Info("starting nvmet controller", "namespace", namespace, "portalHost", portalHost)

	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("start manager: %w", err)
	}

	return nil
}

// runStagePlugin starts a node-local StagePlugin gRPC server over a UNIX
// socket. The plugin wraps the existing transport.Client + mount.MountOps as
// its internals; the CSI node driver (router) dials it by transport kind.
func runStagePlugin(
	ctx context.Context,
	log logr.Logger,
	socket, kind string,
	nvmeCtrlLossTMO, nvmeReconnectDelay int,
) error {
	if err := os.MkdirAll(filepath.Dir(socket), 0o750); err != nil {
		return fmt.Errorf("mkdir stage socket dir: %w", err)
	}

	_ = os.Remove(socket) // clean stale socket

	lis, err := net.Listen("unix", socket)
	if err != nil {
		return fmt.Errorf("listen stage socket %s: %w", socket, err)
	}

	mnt := mountimpl.New("", mountutils.New("/proc/mounts"), utilexec.New())

	var srv stagepb.StagePluginServer
	switch kind {
	case stage.KindNVMe:
		nvmeStage := stage.NewNVMeServer(version, log.WithName("stage-plugin"),
			transport.NewNVMETClient(
				transport.WithCtrlLossTMO(nvmeCtrlLossTMO),
				transport.WithReconnectDelay(nvmeReconnectDelay),
			), mnt)
		cfg, err := rest.InClusterConfig()
		if err != nil {
			return fmt.Errorf("build in-cluster config for NVMe TLS Secret reader: %w", err)
		}
		clientset, err := kubernetes.NewForConfig(cfg)
		if err != nil {
			return fmt.Errorf("build NVMe TLS Secret reader: %w", err)
		}
		nvmeStage.NVMeTLSNamespace = os.Getenv("POD_NAMESPACE")
		if nvmeStage.NVMeTLSNamespace == "" {
			return errors.New("POD_NAMESPACE is required for NVMe TLS Secret reader")
		}
		nvmeStage.NVMeTLSSecrets = stage.ExactSecretReader{GetSecret: func(ctx context.Context, namespace, name string, opts metav1.GetOptions) (*corev1.Secret, error) {
			return clientset.CoreV1().Secrets(namespace).Get(ctx, name, opts)
		}}
		nvmeStage.NVMeTLSPSK = stage.LinuxNVMeTLSPSKProvisioner{}
		srv = nvmeStage
	case stage.KindNFS:
		srv = stage.NewNFSServer(version, log.WithName("stage-plugin"), mnt)
	default:
		return fmt.Errorf("unknown stage plugin kind %q", kind)
	}

	gs := grpc.NewServer(observability.GRPCServerOptions(log)...)
	stagepb.RegisterStagePluginServer(gs, srv)

	go func() {
		<-ctx.Done()

		stopped := make(chan struct{})
		go func() { gs.GracefulStop(); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(grpcDrainTimeout):
			gs.Stop()
		}
	}()

	log.Info("stage plugin gRPC listening", "kind", kind, "address", socket)

	if err := gs.Serve(lis); err != nil {
		return fmt.Errorf("stage grpc serve: %w", err)
	}

	return nil
}

// runNode starts the Identity+Node gRPC servers (no manager needed).
func runNode(
	ctx context.Context,
	log logr.Logger,
	csiAddr, portalHost, nfsServer, nvmetSock, nfsSock string,
	maxVolumesPerNode int64,
	networkDomain, networkDomainSource, networkDomainLabel string,
) error {
	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" && networkDomainSource != reachability.NodeDomainSourceNodeLabel {
		nodeName, _ = os.Hostname()
	}
	var getter reachability.NodeGetter
	if networkDomainSource == reachability.NodeDomainSourceNodeLabel {
		getter = inClusterNodeGetter{}
	}
	resolvedDomain, err := reachability.ResolveNodeDomain(ctx, reachability.NodeDomainConfig{
		Source:       networkDomainSource,
		StaticDomain: networkDomain,
		LabelKey:     networkDomainLabel,
		NodeName:     nodeName,
	}, getter)
	if err != nil {
		return fmt.Errorf("resolve node network domain: %w", err)
	}

	mounter := mountimpl.New("", mountutils.New("/proc/mounts"), utilexec.New())

	// Node staging always routes via the StagePlugin gRPC sidecars; the node
	// driver is now a router (provider-aware translator), not an executor.
	nvmeCli, err := stage.Dial(ctx, nvmetSock, log.WithName("stage-nvmet-client"))
	if err != nil {
		return fmt.Errorf("dial nvmet stage plugin %s: %w", nvmetSock, err)
	}
	defer func() { _ = nvmeCli.Close() }()

	nfsCli, err := stage.Dial(ctx, nfsSock, log.WithName("stage-nfs-client"))
	if err != nil {
		return fmt.Errorf("dial nfs stage plugin %s: %w", nfsSock, err)
	}
	defer func() { _ = nfsCli.Close() }()

	ns := driver.NewNodeServer(driver.NodeConfig{
		Log:        log.WithName("node"),
		NodeID:     nodeName,
		PortalHost: portalHost,
		NFSServer:  nfsServer,
		Mounter:    mounter,
		StagePlugins: map[zfs.VolumeKind]*stage.Client{
			zfs.KindBlock:      nvmeCli,
			zfs.KindFilesystem: nfsCli,
		},
		MaxVolumesPerNode: maxVolumesPerNode,
		NetworkDomain:     resolvedDomain,
	})
	log.Info("node plugin routing via stage plugins", "nvmetSocket", nvmetSock, "nfsSocket", nfsSock)

	ids := driver.NewIdentityServer(nil)

	// node plugin runs without a manager (kubelet-driven lifecycle).
	csiSrv := newCSIServer(ctx, log, ids, nil, ns)
	log.Info("starting node plugin", "csiAddress", csiAddr, "node", nodeName)

	if err := csiSrv.serve(ctx, csiAddr); err != nil {
		return fmt.Errorf("serve csi grpc: %w", err)
	}

	return nil
}

type inClusterNodeGetter struct{}

func (inClusterNodeGetter) GetNode(ctx context.Context, name string) (*corev1.Node, error) {
	return getInClusterNode(ctx, name, rest.InClusterConfig, kubernetes.NewForConfig)
}

func getInClusterNode(
	ctx context.Context,
	name string,
	configFn func() (*rest.Config, error),
	clientFn func(*rest.Config) (*kubernetes.Clientset, error),
) (*corev1.Node, error) {
	config, err := configFn()
	if err != nil {
		return nil, fmt.Errorf("build in-cluster Kubernetes config: %w", err)
	}
	clientset, err := clientFn(config)
	if err != nil {
		return nil, fmt.Errorf("build Kubernetes client: %w", err)
	}
	return clientset.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
}

func validateExpectedOwner(nodeName, expectedOwner string) error {
	if expectedOwner == "" {
		return nil
	}
	if nodeName == "" {
		return fmt.Errorf("NODE_NAME is required when expected owner is configured")
	}
	if nodeName != expectedOwner {
		return fmt.Errorf("NODE_NAME %q does not match expected owner %q", nodeName, expectedOwner)
	}
	return nil
}

// --- wiring helpers ---

// newKeyProvider returns an OpenBao provider if configured, else a no-op.
func newKeyProvider(addr, token, role, jwtPath, mount string) crypto.KeyProvider {
	if addr == "" {
		return nopKeyProvider{}
	}

	var opts []openbao.Option
	if token == "" {
		jwt, err := os.ReadFile(jwtPath)
		if err != nil {
			return nopKeyProvider{}
		}

		opts = append(opts, openbao.WithKubernetesAuth(role, strings.TrimSpace(string(jwt))))
	}

	p, err := openbao.New(addr, token, mount, nil, opts...)
	if err != nil {
		return nopKeyProvider{}
	}

	return p
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}

	return fallback
}

// nopKeyProvider is a no-op KeyProvider for unencrypted/dev runs.
type nopKeyProvider struct{}

var errEncryptionNotConfigured = errors.New("encryption not configured")

func (nopKeyProvider) Generate(context.Context, string) (string, error) {
	return "", fmt.Errorf("%w: no openbao addr", errEncryptionNotConfigured)
}

func (nopKeyProvider) Fetch(context.Context, string) ([]byte, error) {
	return nil, crypto.ErrKeyNotFound
}
func (nopKeyProvider) Delete(context.Context, string) error         { return nil }
func (nopKeyProvider) Stage(string, []byte) (string, string, error) { return "", "", nil }
func (nopKeyProvider) Shred(string) error                           { return nil }

// --- gRPC server glue ---

type csiServer struct {
	log        logr.Logger
	identity   *driver.IdentityServer
	controller *driver.ControllerServer
	node       *driver.NodeServer
}

func newCSIServer(
	ctx context.Context,
	log logr.Logger,
	id *driver.IdentityServer,
	c *driver.ControllerServer,
	n *driver.NodeServer,
) *csiServer {
	return &csiServer{log: log, identity: id, controller: c, node: n}
}

func (s *csiServer) register(srv *grpc.Server) {
	if s.identity != nil {
		csi.RegisterIdentityServer(srv, s.identity)
	}

	if s.controller != nil {
		csi.RegisterControllerServer(srv, s.controller)
	}

	if s.node != nil {
		csi.RegisterNodeServer(srv, s.node)
	}
}

func (s *csiServer) asRunnable(addr string) manager.Runnable {
	return &grpcRunnable{addr: addr, srv: s}
}

func (s *csiServer) serve(ctx context.Context, addr string) error {
	return runGRPC(ctx, addr, s)
}

// runGRPC starts the gRPC server (UNIX or TCP) and blocks until ctx is done.
func runGRPC(ctx context.Context, addr string, s *csiServer) error {
	var (
		lis net.Listener
		err error
	)

	if _, parseErr := url.Parse(addr); parseErr == nil && len(addr) > 0 && addr[0] != '/' {
		lis, err = net.Listen("tcp", addr)
	} else {
		_ = os.Remove(addr) // clean stale socket
		lis, err = net.Listen("unix", addr)
	}

	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	gs := grpc.NewServer(observability.GRPCServerOptions(s.log)...)
	s.register(gs)

	go func() {
		<-ctx.Done()

		stopped := make(chan struct{})

		go func() { gs.GracefulStop(); close(stopped) }()

		select {
		case <-stopped:
		case <-time.After(grpcDrainTimeout):
			gs.Stop()
		}
	}()

	s.log.Info("csi gRPC listening", "address", addr)

	if err := gs.Serve(lis); err != nil {
		return fmt.Errorf("grpc serve: %w", err)
	}

	return nil
}

// grpcRunnable adapts a csiServer to a controller-runtime Runnable.
type grpcRunnable struct {
	addr string
	srv  *csiServer
}

func (r *grpcRunnable) Start(ctx context.Context) error {
	return runGRPC(ctx, r.addr, r.srv)
}

// keep imports referenced for the unused mount pkg in node-less builds.
var _ mount.MountOps = (mount.MountOps)(nil)
