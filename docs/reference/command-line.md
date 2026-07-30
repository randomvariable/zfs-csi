# Command-Line Reference

The `zfs-csi` container image ships one binary. Select the process role with
`--mode`; the same executable runs the CSI controller, storage reconciler, CSI
node plugin, NVMe export controller, and node-local staging sidecars.

## Operating Modes

| Mode | Role |
| --- | --- |
| `controller` | Runs the CSI Identity and Controller gRPC services plus the controller-runtime manager. This mode reads storage inventory, performs serialized placement, creates Volume/Snapshot intent, and records publication intent. |
| `storage` | Runs the privileged storage agent for one logical owner. It publishes StorageNode status and owns backend materialization, protocol endpoint status, ZFS operations, NFS/NVMe export setup, and encryption key staging. |
| `node` | Runs the CSI Identity and Node gRPC services for kubelet. It routes staging and unstaging work to the node-local `nvmet-stage` and `nfs-stage` sidecars. |
| `nvmet` | Runs only the NVMeExport reconciler. It is the configfs writer for NVMe-TCP exports. |
| `nvmet-stage` | Runs a node-local StagePlugin gRPC server for NVMe-TCP staging and unstaging. |
| `nfs-stage` | Runs a node-local StagePlugin gRPC server for NFS staging and unstaging. |

`--mode` is required. Unknown modes exit with usage status.

## Flags

| Flag | Default | Modes | Description |
| --- | --- | --- | --- |
| `--mode` | none | all | Operating mode: `controller`, `storage`, `node`, `nvmet`, `nvmet-stage`, or `nfs-stage`. |
| `--csi-address` | `/csi/csi.sock` | `controller`, `node` | UNIX socket for the CSI gRPC server. |
| `--stage-socket` | `/stage/stage.sock` | `nvmet-stage`, `nfs-stage` | UNIX socket served by a StagePlugin sidecar. |
| `--nvmet-stage-socket` | `/stage/nvmet.sock` | `node` | UNIX socket the node plugin dials for NVMe-TCP staging. |
| `--nfs-stage-socket` | `/stage/nfs.sock` | `node` | UNIX socket the node plugin dials for NFS staging. |
| `--namespace` | `$NAMESPACE`, then `zfs-csi-system` | `controller`, `storage`, `nvmet` | Namespace used for driver custom resources. |
| `--portal-host` | — | `controller`, `storage`, `node`, `nvmet` | NVMe-TCP portal host or IP. Storage publishes port `4420` with owner inventory and volume status; bracket IPv6 only when combined with a port. |
| `--nfs-server` | — | `storage`, `node` | NFS server host. Storage publishes owner inventory and volume status; node mode retains compatibility configuration. |
| `--network-domain` | `$NETWORK_DOMAIN` | `storage`, `node` | Consumer reachability domain. Storage mode publishes inventory; node mode reports `topology.zfs.csi.randomvariable.co.uk/network-domain`. |
| `--metrics-bind-address` | `:8080` | `controller`, `storage`, `nvmet` | Bind address for the Prometheus metrics server. |
| `--leader-elect` | `true` | `controller` | Enables controller-runtime leader election. Storage and nvmet modes run without leader election. |
| `--openbao-addr` | `$OPENBAO_ADDR` | `controller`, `storage` | OpenBao API address. Empty disables encryption and uses a no-op key provider. |
| `--openbao-token` | `$OPENBAO_TOKEN` | `controller`, `storage` | OpenBao token. Empty makes the driver try Kubernetes auth instead. |
| `--openbao-role` | `$OPENBAO_ROLE`, then `zfs-csi-storage` | `controller`, `storage` | OpenBao Kubernetes auth role. |
| `--openbao-kubernetes-jwt-path` | `$OPENBAO_KUBERNETES_JWT_PATH`, then `/var/run/secrets/kubernetes.io/serviceaccount/token` | `controller`, `storage` | ServiceAccount JWT path used when `--openbao-token` is empty. |
| `--openbao-mount` | `transit` | `controller`, `storage` | OpenBao Transit mount path. |
| `--key-tmpfs-dir` | `/run/zfs-keys` | `storage` | tmpfs directory used to stage data encryption keys. |
| `--max-concurrent-reconciles` | `10` | `storage` | Volume reconciler parallelism. |
| `--enable-volume-imports` | `false` | `storage` | Enables retained `VolumeImport` validation and materialisation. Enable only after retain-aware agents are deployed on every storage node. |
| `--max-volumes-per-node` | `128` | `node` | Maximum attachable volumes reported by `NodeGetInfo`. Use `0` to report no limit. |
| `--nvme-ctrl-loss-tmo` | `-1` | `nvmet-stage` | NVMe-oF controller-loss timeout in seconds. `-1` retries forever and is required for storage-node reboot recovery. |
| `--nvme-reconnect-delay` | `10` | `nvmet-stage` | NVMe-oF reconnect delay in seconds. |
| `--sync-period` | `10m` | `controller`, `storage`, `nvmet` | Manager informer resync interval for periodic drift-correcting reconcile. |
| `--tracing-otlp-endpoint` | `$OTEL_EXPORTER_OTLP_ENDPOINT` | all | OTLP gRPC endpoint in `host:port` form. Empty disables tracing. |

## Environment Variables

| Variable | Used by | Description |
| --- | --- | --- |
| `NAMESPACE` | `--namespace` | Default namespace for driver custom resources. If the flag and variable are empty, the binary uses `zfs-csi-system`. |
| `NODE_NAME` | `node` | Node ID reported by the CSI node service. If empty, the binary falls back to the host name. |
| `NETWORK_DOMAIN` | `--network-domain` | Default stable consumer reachability domain for storage and node modes. The stock chart does not yet expose final per-domain multi-agent configuration. |
| `OPENBAO_ADDR` | `--openbao-addr` | Default OpenBao API address. Empty disables encryption support. |
| `OPENBAO_TOKEN` | `--openbao-token` | Default OpenBao token. Empty enables Kubernetes auth fallback. |
| `OPENBAO_ROLE` | `--openbao-role` | Default OpenBao Kubernetes auth role. |
| `OPENBAO_KUBERNETES_JWT_PATH` | `--openbao-kubernetes-jwt-path` | Default JWT file path for OpenBao Kubernetes auth. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `--tracing-otlp-endpoint` | Default OpenTelemetry OTLP endpoint. Empty disables tracing. |

## Examples

Run the controller with an explicit storage-node portal host:

```sh
zfs-csi --mode=controller --csi-address=/csi/csi.sock --portal-host=10.42.0.7
```

Run the storage agent with an explicit portal host and OpenBao encryption enabled:

```sh
zfs-csi --mode=storage --portal-host=10.42.0.7 --openbao-addr=https://openbao.example:8200
```

Run the node plugin and route staging to the sidecars:

```sh
zfs-csi --mode=node \
  --portal-host=10.42.0.7 \
  --nfs-server=10.42.0.7 \
  --nvmet-stage-socket=/stage/nvmet.sock \
  --nfs-stage-socket=/stage/nfs.sock
```

## Endpoints and Logging

- The NVMe-TCP portal port is fixed at `4420`.
- The Prometheus metrics endpoint defaults to `:8080`.
- Logs are structured JSON (zap encoder, RFC3339 timestamps) written to stderr.

## See Also

- [Components and Workloads](components.md) (reference)
- [Helm Values Reference](helm-values.md) (reference)
- [Architecture](../explanation/architecture.md) (explanation)
- [VolumeImport Reference](volume-import.md) (reference)
- [Multi-Storage-Agent Topology and Placement](../explanation/multi-storage-agent-topology.md) (explanation)

---

**Last Updated:** July 2026
