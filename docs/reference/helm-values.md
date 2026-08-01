# Helm Values Reference

This reference describes the configurable values for the `zfs-csi` Helm chart, which lives
in the repository under `charts/zfs-csi`. The chart is version `0.1.0` with an `appVersion`
of `dev`.

The chart declares `kubeVersion: ">=1.36.0-0"` and depends on the pinned
[bitnami/common](https://github.com/bitnami/charts/tree/main/bitnami/common) library chart
(`oci://registry-1.docker.io/bitnamicharts`, version `2.41.0`, recorded in `Chart.lock`).
Run `helm dependency build charts/zfs-csi` before `helm template`, `helm lint`, or
`helm install` from a source checkout; packaged releases already vendor the dependency.
The library supplies the standard `app.kubernetes.io/*` and `helm.sh/chart` object labels
and the image-reference helper. Workload `spec.selector.matchLabels` and pod template
labels are deliberately left unchanged, because those are immutable on existing objects
and would otherwise roll every workload on an unrelated chart version bump.

## Top-Level Values

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `namespace` | string | `""` | Namespace the driver components run in. Defaults to the Helm release namespace. |

## Image

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `image.repository` | string | `docker.io/randomvariable/zfs-csi` | Container image repository. Override with your own registry. |
| `image.tag` | string | `""` | Image tag. Defaults to the chart `appVersion` when empty. |
| `image.pullPolicy` | string | `IfNotPresent` | Image pull policy. |

## Storage Node

!!! note "Storage owner configuration"

    `storageNode` configures one legacy logical owner. For multiple owners, use the
    `storageOwners` list and configure each owner's placement, pool GUIDs, network domains, and
    NFS/NVMe endpoints explicitly.

    `authoritativePoolGUIDs` remains plural for NVMe and future topology. In this release, an
    enabled owner with more than one GUID cannot be combined with an enabled chart NFS
    StorageClass. No owner-scoped NFS switch exists: disable `tankNFS`, `tankNFSTLS`, and
    `flashNFS`, or split NFS pools across owners with distinct endpoints and separate host nfsd
    instances.

These values currently pin the controller Deployment, storage agent, and `nvmet` controller to
one ZFS host.

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `storageNode.name` | string | `""` | Required when `controller.enabled`, `storage.enabled`, or `nvmet.enabled` is true. Kubernetes node name hosting the ZFS pool. |
| `storageNode.authoritativePoolGUIDs` | string list | `[]` | Immutable administrator-declared decimal pool GUID set for the logical owner. Required when rendering `StorageNode`. |
| `storageNode.networkDomain` | string | `workers` | Immutable reachability domain published by the owner. Kubernetes label value, not an endpoint. |
| `storageNode.enabled` | boolean | `true` | Allows fresh owner inventory to participate in new placement. |
| `storageNode.selector` | map | `{kubernetes.io/arch: amd64, zfs.csi.randomvariable.co.uk/storage: "true"}` | Node selector for legacy storage-owning components and `nvmet`. The storage node must carry these labels. Explicit `storageOwners[].nodeSelector` maps also receive the amd64 selector. |
| `storageNode.tolerations` | list | Tolerates key `zfs.csi.randomvariable.co.uk/storage`, value `"true"`, effect `NoSchedule` | Tolerations so the components schedule onto the tainted storage node. |
| `storageNode.poolMountRoot` | string | `/tank` | Host directory where the ZFS pool mounts its datasets (the pool's mountpoint; a pool named `tank` mounts at `/tank`). Bind-mounted with `Bidirectional` propagation so datasets the storage agent mounts inside its container appear in the host mount namespace, which the node's in-kernel NFS server requires to export a populated path. |

## Network

When `network.tls.enabled=true`, Kubernetes 1.36 clusters must enable the
`PodCertificateRequest` feature gate on kube-apiserver, kube-controller-manager,
and kubelet. The gate is
beta and defaults off in 1.36. Node `tlshd` reads the kubelet-rotated
`certificateChainPath` and `keyPath` files directly; no credential relay or
process reload signal is used.

The chart creates the deterministic TLS signing Namespace and annotates it with
`helm.sh/resource-policy: keep`. Helm uninstall therefore retains the Namespace
and its private CA authority. Operators must remove that Namespace only after
intentionally retiring the trust authority and every certificate that chains to
it; uninstalling the driver is not authority retirement.

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `network.portalHost` | string | `""` | Required only when an enabled workload uses an NVMe StorageClass, encryption, or `nvmet`. Host or IP the NVMe-TCP initiators connect to. A valid change refreshes owned volume status for future publish/stage operations; active attachments do not migrate. |
| `network.portalPort` | integer | `4420` | NVMe-TCP portal port. |
| `network.nfsServer` | string | `""` | Required only when `node.enabled` and an NFS StorageClass are enabled. Host or IP the NFS clients mount from. A valid change refreshes owned volume status for future publish/stage operations; active mounts do not migrate. |
| `network.tls.enabled` | boolean | `true` | Enables kernel RPC-with-TLS support. TLS requires both `node.enabled=true` and `storage.enabled=true`; the chart rejects TLS transport classes unless it is enabled. The node and storage workloads add privileged, host-network `tlshd` sidecars and use `hostUsers: true`. |
| `network.tls.signer.nodeSelector` | map | `{kubernetes.io/arch: amd64}` | Node selector for the TLS signer StatefulSet. Additional keys merge with the amd64 default. |
| `network.tls.tlshd.loglevel` | integer | `0` | `tlshd` configuration log level. Increase only for TLS transport troubleshooting. |
| `network.tls.tlshd.extraArgs` | string list | `[]` | Additional arguments passed to each `tlshd` sidecar. Use only arguments supported by the host `tlshd` implementation. |
| `node.networkDomain` | string | `workers` | Domain reported by every node-plugin pod through `NodeGetInfo`. Each node-plugin pod currently advertises one configured domain, which must be reachable from the selected owner. |

## Storage Classes

Each of `tankNVMe`, `tankNFS`, `flashNVMe`, and `flashNFS` exposes the common
sub-keys below. `fsType`, `blocksize`, and `mountOptions` apply only to NVMe
block classes; `nfsExportCIDRs` applies to the filesystem classes. Because the
driver requires an explicit export CIDR, plaintext NFS classes are disabled by
default. TLS tank classes are enabled by default; enable only the transports
required by workloads in this release.

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `storageClasses.defaultClass` | string | `""` | Optionally selects exactly ONE chart StorageClass to carry `storageclass.kubernetes.io/is-default-class: "true"`. Empty (the default) keeps the historical behaviour: no chart StorageClass becomes the cluster default and unrelated PVCs are unaffected. Rendering fails when the named key is unknown, disabled, or not rendered by this release. |
| `storageClasses.defaultClassVariant` | string | `plain` | Which variant of `defaultClass` becomes the cluster default when `encryption.enabled=true` renders both: `plain` or `encrypted`. Ignored when `defaultClass` is empty. `encrypted` requires `encryption.enabled=true`. |
| `storageClasses.<class>.enabled` | boolean | `false` | Whether the chart creates this StorageClass. `tankNFSTLS` and `tankNVMeTLS` default to `true`; plaintext and `flash*` classes default to `false`. |
| `storageClasses.<class>.name` | string | `zfs-tank-nvme`, `zfs-tank-nfs`, `zfs-flash-nvme`, `zfs-flash-nfs`, `zfs-tank-nfs-tls`, `zfs-tank-nvme-tls` | StorageClass metadata name. |
| `storageClasses.<class>.pool` | string | `tank` for the tank classes, `flash` for the flash classes | ZFS pool the class provisions into. |
| `storageClasses.<class>.reclaimPolicy` | string | `Delete` | Reclaim policy of the StorageClass. |
| `storageClasses.<class>.fsType` | string | `xfs` | Block filesystem passed through Kubernetes' reserved `csi.storage.k8s.io/fstype` parameter. Set `ext4` for metadata-heavy small-file workloads. |
| `storageClasses.<class>.blocksize` | string | `16k` | Immutable ZFS `volblocksize` for new zvols. Use `16k` for database-style I/O or `128k` for sequential/VM workloads. |
| `storageClasses.<class>.mountOptions` | list | `[]` | Additional Kubernetes StorageClass mount options for NVMe block filesystems. Do not use `nobarrier`; XFS must retain flush ordering. |
| `storageClasses.<class>.nfsExportCIDRs` | string list | `[]` | Required IPv4/IPv6 CIDRs permitted to mount an enabled NFS StorageClass. The chart joins the list into the StorageClass string parameter. |
| `storageClasses.tankNFSTLS` | map | `{enabled: true, name: zfs-tank-nfs-tls, pool: tank, nfsExportCIDRs: [127.0.0.1/32], reclaimPolicy: Delete}` | TLS-protected NFS class for the `tank` pool. Replace the loopback-only default export CIDR with consumer-node CIDRs before use. Requires `network.tls.enabled=true`. |
| `storageClasses.tankNVMeTLS` | map | `{enabled: true, name: zfs-tank-nvme-tls, pool: tank, fsType: xfs, blocksize: 16k, mountOptions: [], reclaimPolicy: Delete}` | TLS-protected NVMe-TCP class for the `tank` pool. Requires `network.tls.enabled=true`. |

## Throughput And Control Plane

| Key | Default | Description |
| --- | --- | --- |
| `controller.nodeSelector` | `{kubernetes.io/arch: amd64}` | Node selector for multi-owner controller pods. Additional keys merge with the amd64 default. |
| `controller.metricsBindAddress` / `controller.healthProbeBindAddress` | `:8080` / `:8082` | Controller metrics and health listener addresses. |
| `controller.manager.syncPeriod` | `10m` | Informer full-resync interval. Increasing it reduces periodic reconcile load but lengthens drift repair. |
| `controller.sidecars.*` | upstream defaults | Worker, Kubernetes API QPS/burst, and RPC timeout settings for provisioner, attacher, resizer, and snapshotter. Tune after API/server measurements. |
| `storage.metricsBindAddress` / `storage.healthProbeBindAddress` | `:8080` / `:8082` | Storage agent metrics and health listener addresses. |
| `storage.manager.maxConcurrentReconciles` / `syncPeriod` | `10` / `10m` | Bounds storage-agent parallel work and controls periodic drift reconciliation. |
| `storage.enableVolumeImports` | `false` | Enables storage-administrator `VolumeImport` validation and materialisation. Enable only after retain-aware storage agents run on every storage node. |
| `node.maxVolumesPerNode` | `128` | Scheduler-facing maximum attachable volumes; set `0` to omit a limit. |
| `node.nodeSelector` | `{kubernetes.io/arch: amd64}` | Node selector for node-plugin pods. Additional keys merge with the amd64 default. |
| `node.nvme.ctrlLossTMO` / `reconnectDelay` | `-1` / `10` seconds | NVMe-oF reconnect policy. Keep `ctrlLossTMO=-1`; finite values break reboot recovery. |

## CSI Service Account Tokens

These values opt the `CSIDriver` into KEP-5538 service-account-token delivery in
`NodePublishVolumeRequest.secrets`. Core zfs-csi does not consume these tokens or
use them for authorization. Enable this only when a downstream node-publish
policy integration consumes the token; otherwise it creates sensitive tokens
and token-refresh churn without any benefit. This opt-in requires Kubernetes
1.35 or newer. Kubernetes 1.35 requires the `CSIServiceAccountTokenSecrets`
feature gate on the kube-apiserver and kubelet; the feature is locked on in
Kubernetes 1.36.

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `serviceAccountToken.enabled` | boolean | `false` | Render `spec.tokenRequests` and `spec.serviceAccountTokenInSecrets: true`. Disabled by default, so the chart requests no workload tokens. |
| `serviceAccountToken.audience` | string | `""` | Token audience. Must be non-empty when enabled and must identify the downstream policy integration that validates the token. |
| `serviceAccountToken.expirationSeconds` | integer | `3600` | Requested token lifetime. Kubernetes requires at least 600 seconds. |
| `serviceAccountToken.requiresRepublish` | boolean | `false` | Set `CSIDriver.spec.requiresRepublish`. Enable only if the integration needs refreshed tokens through repeated `NodePublishVolume` calls; this adds kubelet and driver churn. |

## Encryption

OpenBao Transit-backed ZFS native encryption. When enabled, the controller and storage
agent authenticate to OpenBao, generate a per-volume data-encryption key via Transit, and
create datasets with `encryption=on`. Enabling encryption also causes the chart to render
the `zfs-tank-nvme-encrypted` StorageClass.

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `encryption.enabled` | boolean | `false` | Enable per-volume ZFS native encryption. |
| `encryption.openbao.addr` | string | `http://openbao.openbao.svc:8200` | OpenBao API address. |
| `encryption.openbao.transitMount` | string | `transit` | OpenBao Transit mount path. |
| `encryption.openbao.role` | string | `zfs-csi-storage` | OpenBao Kubernetes auth role the driver authenticates as. |
| `encryption.openbao.token` | string | `""` | Static OpenBao token for dev mode. Leave empty to use Kubernetes auth (the recommended path). |

## Minimal Values Example

A minimal values file sets the image repository, the storage node name and address, and
enables the required `tank` StorageClasses explicitly. No StorageClass becomes the
Kubernetes default.

```yaml
image:
  repository: registry.example.com/zfs-csi/zfs-csi
  tag: "v0.1.0"
storageNode:
  name: storage-node-1
  # Replace with `zpool get -H -o value guid tank` output.
  authoritativePoolGUIDs:
    - "12345678901234567890"
  networkDomain: workers
  poolMountRoot: /tank
network:
  # Example RFC1918 address; replace with the storage node's reachable address.
  portalHost: 10.42.0.7
  nfsServer: 10.42.0.7
storageClasses:
  tankNVMe:
    enabled: true
  tankNFS:
    enabled: true
    nfsExportCIDRs:
      - 10.42.0.0/16
      - 2001:db8:42::/64
```

## See Also

- [Install zfs-csi with Helm](../how-to/install-with-helm.md) (how-to)
- [StorageClass Reference](storage-classes.md) (reference)
- [Enable Per-Volume Encryption](../how-to/enable-encryption.md) (how-to)
- [Import an Existing ZFS Volume](../how-to/import-existing-zfs-volume.md) (how-to)
- [VolumeImport Reference](volume-import.md) (reference)
- [Multi-Storage-Agent Topology and Placement](../explanation/multi-storage-agent-topology.md) (explanation)

---

**Last Updated:** July 2026
**Chart Version:** 0.1.0
