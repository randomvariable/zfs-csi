# Components and Workloads

zfs-csi is a single binary that runs in one of six modes. This reference maps each mode to
the Kubernetes workload that runs it, the node it runs on, and the CSI sidecars that
accompany it.

## Modes and Workloads

| Mode | Workload | Runs On | Responsibility |
| --- | --- | --- | --- |
| `controller` | Deployment `zfs-csi-controller` | Storage node | CSI Identity and Controller gRPC plus the reconciler manager. Leader-elected. Orchestrates provisioning. The chart pins it to the storage node via `storageNode.selector`. |
| `storage` | DaemonSet `zfs-csi-storage` | Storage node only | Volume and Snapshot reconcilers that materialise ZFS datasets and zvols, load encryption keys, and manage NFS exports. Optionally validates administrator-created `VolumeImport` resources. Privileged. |
| `nvmet` | DaemonSet `nvmet-controller` | Storage node only | The `NVMeExport` reconciler — the sole writer of the kernel `nvmet` configfs target state. Privileged. |
| `node` | DaemonSet `zfs-csi-node` | Every node | CSI Identity and Node gRPC. Routes volume staging to the stage-plugin sidecars and mounts volumes. Kubelet-driven. Privileged. |
| `nvmet-stage` | Sidecar in `zfs-csi-node` | Every node | Node-local `StagePlugin` gRPC server for NVMe-TCP attach, format, and mount. Privileged. |
| `nfs-stage` | Sidecar in `zfs-csi-node` | Every node | Node-local `StagePlugin` gRPC server for NFS mount. Privileged. |
| `tlshd` | Sidecar in `zfs-csi-node` and `zfs-csi-storage` | Every node / storage node | Userspace TLS handshake agent the kernel calls up to for NVMe-TCP and NFS transport security. Privileged, `hostNetwork`. Present only when `network.tls.enabled` is set. |
| TLS signer | Deployment in the signing namespace | Any node | Signs node client certificates for NFS mutual TLS via PodCertificateRequest. Runs outside the driver namespace so the CA signing key is not reachable from runtime identities. Present only when `network.tls.enabled` is set. |

**Note:** The `zfs-csi-storage` and `nvmet-controller` DaemonSets schedule only onto nodes
carrying the `zfs.csi.randomvariable.co.uk/storage` label and tolerate the matching taint. The
`zfs-csi-node` DaemonSet tolerates all taints (the standard CSI node-plugin pattern), so it runs
on every node — including the tainted storage node. A pod can stage and mount a volume only when
its node's reported network domain matches the domain selected for that volume.

## CSI Sidecars

The upstream Kubernetes CSI sidecars run alongside the driver containers. The
node-driver-registrar runs in the `zfs-csi-node` DaemonSet; the controller-side sidecars run
in the `zfs-csi-controller` Deployment.

| Sidecar | Location | Responsibility |
| --- | --- | --- |
| `node-driver-registrar` | `zfs-csi-node` DaemonSet | Registers the node plugin's socket with the kubelet. |
| `external-provisioner` | `zfs-csi-controller` Deployment | Watches PersistentVolumeClaims and calls `CreateVolume`/`DeleteVolume`. |
| `external-attacher` | `zfs-csi-controller` Deployment | Calls `ControllerPublishVolume`/`ControllerUnpublishVolume` for block attach. |
| `external-resizer` | `zfs-csi-controller` Deployment | Calls `ControllerExpandVolume` when a claim is enlarged. |
| `external-snapshotter` (`csi-snapshotter`) | `zfs-csi-controller` Deployment | Calls `CreateSnapshot`/`DeleteSnapshot` for VolumeSnapshots. |

**Note:** The cluster-scoped snapshot machinery — the `VolumeSnapshot` custom resource
definitions and the snapshot controller — is a separate, cluster-wide component that the
zfs-csi chart does not install. See [Snapshot and Restore a Volume](../how-to/snapshot-and-restore.md).

## The Node Plugin Data Path

The `node` container is a router, not an executor. When the kubelet calls `NodeStageVolume`,
the node container dials the appropriate stage-plugin sidecar over a UNIX socket in a shared
volume:

- **Block volumes** are routed to the `nvmet-stage` sidecar, which attaches the NVMe-TCP
  target, formats the device on first use, and mounts it.
- **Filesystem volumes** are routed to the `nfs-stage` sidecar, which mounts the NFS export.

The sidecars expose readiness probes on their sockets; the node container fails fast if a
sidecar socket is not ready.

## Driver Identity

| Property | Value |
| --- | --- |
| CSI driver name | `zfs.csi.randomvariable.co.uk` |
| API group | `zfs.csi.randomvariable.co.uk` |
| API version | `v1alpha1` |
| Volume lifecycle modes | `Persistent` only (no CSI ephemeral inline volumes) |

## See Also

- [Command-Line Reference](command-line.md) (reference)
- [Architecture](../explanation/architecture.md) (explanation)
- [Storage Model](../explanation/storage-model.md) (explanation)
- [VolumeImport Reference](volume-import.md) (reference)

---

**Last Updated:** July 2026
**API Version:** zfs.csi.randomvariable.co.uk/v1alpha1
