# StorageClass Reference

The zfs-csi driver provisions volumes for the `zfs.csi.randomvariable.co.uk` provisioner.
This reference describes the StorageClass `parameters` the driver supports and the
StorageClasses the Helm chart creates.

## Parameters

StorageClass parameters are passed to the driver through the `parameters` field.

| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `pool` | string | Yes | — | ZFS pool the volume is provisioned into. Chart-created classes use `tank` or `flash`. |
| `type` | string | No | `block` | Volume type. `block` is a ZFS zvol exported over NVMe-TCP (`ReadWriteOnce`). `filesystem` is a ZFS dataset exported over NFS (`ReadWriteMany`). |
| `transport` | string | No | `nvme-tcp` | Block transport protocol. Only `nvme-tcp` is supported. Ignored for `filesystem` type. |
| `fsType` | string | No | `xfs` | Filesystem formatted on a block volume before mount. One of `xfs` or `ext4`. XFS is the default for VM and sequential-I/O workloads; choose ext4 for metadata-heavy small-file loads. Ignored for `filesystem` type. |
| `blocksize` | string | No | ZFS default | ZFS `volblocksize` for block volumes (for example `16k`). Digits with an optional `k`/`K`, `m`/`M`, or `g`/`G` suffix, base 1024; must be positive. It is immutable after creation. Use `16k` for database-style I/O or `128k` for sequential/VM workloads. Block volume capacity is rounded up to a multiple of this value — see [Block Capacity Alignment](#block-capacity-alignment). |
| `encrypted` | string | No | unset | When `"true"`, the volume is created with ZFS native encryption using a per-volume key generated via OpenBao Transit. Requires encryption to be enabled at install time. |
| `nfsExportCIDRs` | string list | Yes for `filesystem` | — | IPv4/IPv6 CIDRs permitted to mount the NFS export. Must cover the consumer nodes' network or NFS mounts fail `access denied by server`. |
| `nfsExportAccessMode` | string | No | driver default | Export access mode applied to the NFS export. Ignored for `block` type. |
| `nfsTLS` | string | No | unset | When `"true"`, the NFS export requires mutual TLS (`xprtsec=mtls`). Requires `type=filesystem` and transport TLS enabled at install time. See [Transport Security](../explanation/transport-security.md). |
| `nvmeTLS` | string | No | unset | When `"true"`, the NVMe-TCP target requires TLS with a per-volume pre-shared key. Requires `type=block` with `transport=nvme-tcp` and transport TLS enabled at install time. |
| `compression` | string | No | ZFS default | ZFS compression algorithm. One of `on`, `off`, `lz4`, `gzip`, `zstd`, or a `zstd-<1-9>` / `zstd-<1-9>-fast` variant. Mutable via VolumeAttributesClass. |

Parameter keys are matched case-insensitively, and unknown keys are ignored.

## Block Capacity Alignment

ZFS requires a zvol's `volsize` to be a whole number of `volblocksize` units. A
PVC request such as `1Gi + 1` byte against `blocksize: "16k"` is not a legal
`volsize`, so the driver aligns capacity instead of passing the raw request to
ZFS.

For `type=block` volumes the driver:

- Rounds `required_bytes` **up** to the next multiple of the effective
  `volblocksize`. The effective block size is the `blocksize` parameter, or
  16 KiB when the parameter is unset. The driver persists that default
  explicitly on `Volume.spec.volBlockSize` rather than leaving ZFS to pick one,
  so create-time and expand-time alignment always agree.
- Rejects the request with `InvalidArgument` when `blocksize` is not a value
  OpenZFS accepts for a zvol (a power of two between 512 bytes and 128 KiB),
  before any `Volume` resource is created.
- Rejects the request with `InvalidArgument` when `limit_bytes` is set and the
  smallest aligned capacity would exceed it, rather than over-provisioning past
  the limit.
- Rejects the request with `InvalidArgument` when `required_bytes` exceeds
  `limit_bytes`.

The rounded capacity is what the driver reserves during placement, persists in
`Volume.spec.capacity`, and returns in the CSI `CreateVolume` /
`ControllerExpandVolume` response, so the reported capacity is always the
capacity ZFS actually provisions. A PVC may therefore bind to a PV slightly
larger than requested — at most `volblocksize - 1` bytes.

Volume expansion follows the same rule and uses the volume's own persisted
`volBlockSize`, because expansion carries no StorageClass parameters and
`volblocksize` is immutable after creation. A request that rounds to the volume's
current capacity or below is a no-op and returns the current capacity.

Clones and snapshot restores align to the **source** volume's `volblocksize`:
`zfs clone` inherits `volblocksize` from the origin and cannot change it, so the
`blocksize` parameter on the target StorageClass does not apply. A snapshot
records its source block size on the `Snapshot` resource at creation time, so a
restore stays correctly aligned even if the parent volume was retained and its
`Volume` resource removed.

If a `block` source records no `volblocksize` at all — a legacy `Volume` or
`Snapshot` written before the driver persisted the value explicitly — the clone,
restore, or `CreateSnapshot` is rejected with `FailedPrecondition`. The
controller never reads ZFS properties (only the owning storage-agent can), so it
has no authoritative block size for such a source, and assuming today's 16 KiB
default could mis-align a zvol created under a different one (OpenZFS used 8 KiB
before 2.2, and pools may override it). Filesystem sources are unaffected.

`type=filesystem` volumes are byte-exact. Capacity is enforced through `refquota`
on a dataset, which has no alignment constraint, so `recordsize` never changes
the requested size.

## Filesystem Size And Selection

The default block filesystem is XFS. Although `mkfs.xfs` can format smaller
devices in some configurations, treat **300 MiB** as the practical minimum
volume size for XFS workloads. For smaller test volumes or metadata-heavy
small-file workloads, override the StorageClass with
`csi.storage.k8s.io/fstype: ext4`. This is workload guidance, not a driver-side
minimum-size validation.

Block StorageClasses set `volumeBindingMode: WaitForFirstConsumer` and
`allowVolumeExpansion: true`. NFS StorageClasses set `volumeBindingMode: Immediate` and
`allowVolumeExpansion: true`.

## StorageClasses Created by the Chart

| Name | Pool | Type | Transport | Access Modes | volumeBindingMode | Reclaim | Enabled by Default |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `zfs-tank-nvme` | `tank` | `block` | `nvme-tcp` | `ReadWriteOnce` | `WaitForFirstConsumer` | `Delete` | Yes |
| `zfs-tank-nvme-encrypted` | `tank` | `block` | `nvme-tcp` | `ReadWriteOnce` | `WaitForFirstConsumer` | `Delete` | Only when `encryption.enabled=true` |
| `zfs-tank-nfs` | `tank` | `filesystem` | NFS | `ReadWriteMany` | `Immediate` | `Delete` | Only when configured with `nfsExportCIDRs` |
| `zfs-tank-nfs-encrypted` | `tank` | `filesystem` | NFS | `ReadWriteMany` | `Immediate` | `Delete` | `encryption.enabled=true` and `nfsExportCIDRs` |
| `zfs-tank-nfs-tls` | `tank` | `filesystem` | NFS+TLS | `ReadWriteMany` | `Immediate` | `Delete` | TLS and `nfsExportCIDRs` configured |
| `zfs-tank-nfs-tls-encrypted` | `tank` | `filesystem` | NFS+TLS | `ReadWriteMany` | `Immediate` | `Delete` | TLS, encryption, and `nfsExportCIDRs` |
| `zfs-tank-nvme-tls` | `tank` | `block` | `nvme-tcp+TLS` | `ReadWriteOnce` | `WaitForFirstConsumer` | `Delete` | TLS enabled |
| `zfs-tank-nvme-tls-encrypted` | `tank` | `block` | `nvme-tcp+TLS` | `ReadWriteOnce` | `WaitForFirstConsumer` | `Delete` | TLS and encryption enabled |
| `zfs-flash-nvme` | `flash` | `block` | `nvme-tcp` | `ReadWriteOnce` | `WaitForFirstConsumer` | `Delete` | No |
| `zfs-flash-nvme-encrypted` | `flash` | `block` | `nvme-tcp` | `ReadWriteOnce` | `WaitForFirstConsumer` | `Delete` | `encryption.enabled=true` |
| `zfs-flash-nfs` | `flash` | `filesystem` | NFS | `ReadWriteMany` | `Immediate` | `Delete` | No |
| `zfs-flash-nfs-encrypted` | `flash` | `filesystem` | NFS | `ReadWriteMany` | `Immediate` | `Delete` | `encryption.enabled=true` and `nfsExportCIDRs` |

**Note:** The `flash` classes are disabled by default. Enable them by setting
`storageClasses.flashNVMe.enabled=true` or `storageClasses.flashNFS.enabled=true` in the
Helm values. Encrypted variants render only when `encryption.enabled=true` and their base
class is enabled. Encrypted NFS variants require `nfsExportCIDRs`; TLS variants also require
`network.tls.enabled=true` and the controller, node, and storage components enabled.

### Cluster Default StorageClass

By default the chart marks **no** StorageClass as the cluster default, so PVCs that
omit `storageClassName` are unaffected by installing zfs-csi.

To make exactly one chart-generated class the cluster default, set
`storageClasses.defaultClass` to its values key (`tankNVMe`, `tankNFS`, `tankNFSTLS`,
`tankNVMeTLS`, `flashNVMe`, or `flashNFS`). Only that one class receives
`storageclass.kubernetes.io/is-default-class: "true"`.

When `encryption.enabled=true` a selected class renders both a plaintext and an
`-encrypted` variant. `storageClasses.defaultClassVariant` picks which one carries the
annotation: `plain` (the default) or `encrypted`. The annotation never lands on both.

```yaml
storageClasses:
  defaultClass: tankNVMeTLS
  defaultClassVariant: encrypted   # -> zfs-tank-nvme-tls-encrypted is the cluster default
```

The chart fails to render, rather than silently producing no default, when the selected
class is unknown, disabled, or not rendered by the current release (for example a TLS
class selected while `network.tls.enabled=false`, or an `encrypted` variant selected
while `encryption.enabled=false`).

Kubernetes permits only one default StorageClass per cluster. Clear any pre-existing
default before setting `storageClasses.defaultClass`, or PVC defaulting stays ambiguous.

## Examples

A block StorageClass provisions a ZFS zvol exported over NVMe-TCP:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: zfs-tank-nvme
provisioner: zfs.csi.randomvariable.co.uk
parameters:
  pool: tank
  type: block
  transport: nvme-tcp
  # XFS is the chart default; use ext4 for metadata-heavy small-file workloads.
  csi.storage.k8s.io/fstype: xfs
  blocksize: "16k"
reclaimPolicy: Delete
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
```

A filesystem StorageClass provisions a ZFS dataset exported over NFS. Set `nfsExportCIDRs`
to cover the consumer nodes' network:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: zfs-tank-nfs
provisioner: zfs.csi.randomvariable.co.uk
parameters:
  pool: tank
  type: filesystem
  nfsExportCIDRs: "10.0.0.0/16,2001:db8:42::/64"
reclaimPolicy: Delete
volumeBindingMode: Immediate
allowVolumeExpansion: true
```

## See Also

- [Helm Values Reference](helm-values.md) (reference)
- [Provision Block Storage](../how-to/provision-block-storage.md) (how-to)
- [Provision a Shared Filesystem](../how-to/provision-shared-filesystem.md) (how-to)
- [Storage Model](../explanation/storage-model.md) (explanation)

---

**Last Updated:** July 2026
**API Version:** zfs.csi.randomvariable.co.uk/v1alpha1
