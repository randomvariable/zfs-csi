# Volume (Internal API)

!!! warning "Internal API"
    `Volume` is an internal custom resource. The driver's controller creates and manages
    `Volume` objects in response to `PersistentVolumeClaim` provisioning; the storage agent
    reconciles them. **Do not create, edit, or delete dynamically provisioned `Volume`
    objects directly.** Consume dynamic storage through `PersistentVolumeClaim`. Storage
    administrators delete a materialised imported `Volume` only during the documented
    retained de-adoption procedure. This reference otherwise exists for debugging and
    development.

`Volume` is the desired and observed state of a single CSI-provisioned ZFS volume. It is the
contract between the CSI controller (which writes `spec`) and the storage agent (which
reconciles it and writes `status`).

| Property | Value |
| --- | --- |
| API group | `zfs.csi.randomvariable.co.uk` |
| API version | `v1alpha1` |
| Kind | `Volume` |
| Short name | `zv` |
| Scope | Namespaced |
| Finalizer | `zfs.csi.randomvariable.co.uk/volume-protect` |

## Spec

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `provenance` | string | No | `Dynamic` | `Dynamic` for CSI-provisioned storage or `Imported` for a validated retained backend. |
| `backendPath` | string | Yes for `Imported` | unset | Immutable canonical ZFS object name. Empty for dynamic volumes. |
| `deletionPolicy` | string | No | `Delete` | `Retain` for imports and `Delete` for dynamic volumes. |
| `pool` | string | Yes | — | ZFS pool the volume is provisioned into (1–63 characters). |
| `capacity` | integer | Yes | — | Provisioned size in bytes: the capacity the storage-agent applies to ZFS (`volsize` for `block`, `refquota` for `filesystem`), not the raw CSI request. For `block` this is the CSI `required_bytes` rounded up to a multiple of the effective `volBlockSize`; for `filesystem` it is byte-exact. Minimum 1. |
| `type` | string | No | `block` | `block` provisions a zvol; `filesystem` provisions a dataset. |
| `fsType` | string | No | `ext4` | Filesystem to format on a block volume before mount. `ext4` or `xfs`. Ignored for `filesystem`. |
| `volBlockSize` | string | No | `16k` for `block`, ZFS default for `filesystem` | ZFS `volblocksize`/`recordsize` (for example `16k`); digits with an optional `k`/`K`, `m`/`M`, or `g`/`G` suffix, base 1024. Immutable after creation, enforced by a CEL validation rule on the CRD. For `block` volumes this is the alignment unit for `capacity`, and the controller always writes an explicit canonical value (a power of two between 512 bytes and 128 KiB, defaulting to `16k`) so create-time and expand-time alignment cannot diverge from an unset ZFS default. For clones and snapshot restores the controller copies the source volume's value, because `zfs clone` inherits `volblocksize` from the origin; a `block` source that records no value is rejected with `FailedPrecondition` rather than assumed to be 16 KiB, because the controller cannot read the actual ZFS property. An empty value is normal and safe on `filesystem` volumes, where `recordsize` constrains no capacity. |
| `compression` | string | No | inherit | ZFS compression property. One of `on`, `off`, `lz4`, `gzip`, `zstd`, `zstd-<1-9>`, or `zstd-<1-9>-fast`. |
| `encryptionKeyRef` | string | No | unset (no encryption) | OpenBao key reference for the per-volume DEK, in the form `transit/<keyName>` or `kv/<path>`. |
| `transport` | string | No | `nvme-tcp` | Block transport. Ignored for `filesystem`. |
| `ownerNode` | string | Yes | — | Storage node that materialises this volume. |
| `volName` | string | Yes | — | Human-readable CSI volume name; derives the ZFS dataset leaf name. |
| `volumeID` | string | Yes | — | CSI volume handle returned to the provisioner. |
| `sourceSnapshotID` | string | No | unset | CSI snapshot handle to restore into this volume. |
| `sourceVolumeID` | string | No | unset | CSI volume handle to clone into this volume. |
| `nfsExportCIDRs` | string list | Yes for `filesystem` | — | IPv4/IPv6 CIDRs allowed to mount the volume over NFS. |
| `nfsExportAccessMode` | string | No | `rw` | Whether the NFS export is `rw` or `ro`. The reconciler always adds `root_squash`. |

## Status

| Field | Type | Description |
| --- | --- | --- |
| `conditions` | array | Standard condition list. Condition types: `Ready`, `Encrypted`. |
| `state` | string | Lifecycle state: `Pending`, `Ready`, `ReadyToPublish`, `Deleting`, `Destroyed`, or `Error`. |
| `observedGeneration` | integer | The `spec` generation last reconciled. |
| `targetNQN` | string | NVMe-TCP subsystem NQN (block). |
| `portal` | string | `host:port` the consumer connects to (block). |
| `deviceGUID` | string | Stable per-volume GUID embedded in the NVMe namespace identifier. |
| `exportPath` | string | NFS export path (`server:<dataset>`) for filesystem volumes. |
| `mappedInitiators` | array | Consumers allowed to attach (block): `nodeName`, `initiatorID`, `attachedAt`. |
| `publishedInitiators` | array | Initiator IDs the storage agent has confirmed live in the transport. |
| `keyStatus` | string | Encryption key availability: `Available` or `Unavailable`. |
| `zvolPath` | string | Host `/dev/zvol/...` path (block). |
| `datasetPath` | string | Full ZFS dataset name (for example `tank/csi/block/<id>`). |
| `actualCapacity` | integer | Size ZFS actually provisioned, in bytes. For `block` volumes this matches the aligned `spec.capacity`; the controller returns it as the CSI volume capacity. |

## Printer Columns

`kubectl get zv` shows: `Pool`, `Type`, `Capacity`, `State`, `Age`.

## See Also

- [Kubernetes API Surface](../kubernetes-api.md) (reference)
- [Snapshot (Internal API)](snapshot.md) (reference)
- [Architecture](../../explanation/architecture.md) (explanation)
- [VolumeImport Reference](../volume-import.md) (reference)

---

**Last Updated:** July 2026
**API Version:** zfs.csi.randomvariable.co.uk/v1alpha1
