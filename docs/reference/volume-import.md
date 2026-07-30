# VolumeImport Reference

`VolumeImport` validates an existing ZFS dataset or zvol and materialises retained internal
`Volume` intent for a storage administrator to bind through a static Kubernetes PV.

| Property | Value |
| --- | --- |
| API group | `zfs.csi.randomvariable.co.uk` |
| API version | `v1alpha1` |
| Kind | `VolumeImport` |
| Short name | `zvi` |
| Scope | Namespaced |
| Feature gate | `storage.enableVolumeImports` / `--enable-volume-imports` |
| Default state | Disabled |

**Important:** `VolumeImport` is an administrator-created trust boundary. Runtime zfs-csi
RBAC can reconcile imports but cannot create them. Create imports only in the namespace used
by the driver controller and storage agent.

## Spec

The complete `spec` is immutable after creation.

| Field | Type | Required | Default | Constraints and Behaviour |
| --- | --- | --- | --- | --- |
| `pool` | string | Yes | none | Imported ZFS pool name. The pool must be imported on `ownerNode`. |
| `backendPath` | string | Yes | none | Canonical existing dataset or zvol name. Must begin with `<pool>/`, contain no snapshot suffix, and remain outside `<pool>/csi` and `<pool>/csi/**`. |
| `type` | string | Yes | none | Closed enum: `block` validates a zvol; `filesystem` validates a filesystem dataset. |
| `capacity` | integer | Yes | none | Minimum acceptable capacity in bytes. Must be at least 1. The observed ZFS capacity must be equal or greater. |
| `ownerNode` | string | Yes | none | Kubernetes storage-node name whose storage agent may inspect and materialise the import. Other agents ignore it. |
| `transport` | string | No | `nvme-tcp` | Closed enum containing only `nvme-tcp`. Applies to block imports. |
| `fsType` | string | No | empty | Existing zvol signature. Closed enum: empty for raw block, `ext4`, or `xfs`. The `blkid` probe must match exactly. |
| `nfsExportCIDRs` | string list | Yes for `filesystem` | none | IPv4/IPv6 CIDRs permitted to mount the NFS export. Ignored for block imports. |
| `nfsExportAccessMode` | string | No | `rw` | Closed enum: `rw` or `ro`. zfs-csi adds `root_squash`; raw `sharenfs` strings are not accepted. |
| `deletionPolicy` | string | No | `Retain` | Closed enum containing only `Retain`. Dataset, zvol, key, snapshots, clones, and data remain after deletion. |

## Backend Validation

The storage agent performs all checks before materialising an internal `Volume`:

| Check | Failed Reason | Result |
| --- | --- | --- |
| Path is outside `<pool>/csi/**` and belongs to `pool` | `InvalidBackendPath` | `Failed` |
| `deletionPolicy` is `Retain` | `InvalidDeletionPolicy` | `Failed` |
| Block transport is `nvme-tcp` | `UnsupportedTransport` | `Failed` |
| Filesystem import supplies `nfsExportCIDRs` | `NFSExportCIDRsRequired` | `Failed` |
| Pool is imported on `ownerNode` | `PoolNotImported` | `Pending`, retried |
| Backend exists and reports its canonical identity | `BackendNotFound` or `BackendIdentityMismatch` | `Failed` |
| ZFS kind matches `type` | `WrongKind` | `Failed` |
| Backend is unencrypted | `EncryptedUnsupported` | `Failed` |
| Filesystem has non-zero `refquota` | `RefquotaRequired` | `Failed` |
| Observed capacity is at least `capacity` | `InsufficientCapacity` | `Failed` |
| Filesystem has an observable authoritative mountpoint | `ExportPathUnavailable` | `Failed` |
| Existing zvol signature matches `fsType` | `FormatMismatch` or `FormatProbeFailed` | `Failed` |
| No other import or incompatible deterministic `Volume` claims the backend | `BackendConflict` or `VolumeConflict` | `Failed` |

An import that already materialised a `Volume` never silently re-adopts it after that
`Volume` disappears or begins deletion. It reports `MaterializedVolumeMissing` or
`MaterializedVolumeDeleting` instead.

## Status

| Field | Type | Presence and Meaning |
| --- | --- | --- |
| `conditions` | array | Standard condition list. `Ready=True` means validation completed and the materialised `Volume` is ready. |
| `state` | string | Closed enum: `Pending`, `Ready`, or `Failed`. |
| `observedGeneration` | integer | Most recent generation observed by the controller. When equal to `metadata.generation`, status reflects the immutable spec. |
| `volumeHandle` | string | Deterministic CSI handle to copy exactly into a static PV. Present after backend validation. |
| `volumeRef` | string | Name of the materialised internal `Volume` in the same namespace. |
| `actualCapacity` | integer | Observed ZFS `volsize` or `refquota`, in bytes. |
| `exportPath` | string | Authoritative ZFS mountpoint for filesystem imports. Empty for block imports. |

## Lifecycle and CSI Capabilities

| Operation | Imported Volume Behaviour |
| --- | --- |
| Publish and stage | Supported through a static PV using `status.volumeHandle`. Block uses NVMe-TCP; filesystem uses NFS and authoritative `exportPath`. |
| Delete retained static PV or PVC | Does not invoke CSI backend deletion and does not remove the decoupled internal `Volume`. |
| Delete `VolumeImport` | Removes only the import request. It does not cascade to the decoupled internal `Volume`. |
| Delete materialised internal `Volume` | After all publications are removed, removes zfs-csi transport and reconciliation state; retains backend, key, snapshots, clones, and data. |
| Snapshot or restore | Not supported in Phase 1. |
| PVC clone | Not supported in Phase 1. |
| Expansion | Not supported in Phase 1. Change backend capacity outside CSI, then create a new import contract if required. |
| `VolumeAttributesClass` modification | Not supported in Phase 1. |
| Encryption import | Not supported in Phase 1. Only unencrypted backends pass validation. |

Deletion of an imported NFS filesystem sets its driver-managed share to `off` and unmounts
only when zfs-csi had enabled sharing. Import and de-adoption preserve root mode, UID, and GID.

## Minimal Examples

An existing raw zvol import requires an empty `fsType`:

```yaml
apiVersion: zfs.csi.randomvariable.co.uk/v1alpha1
kind: VolumeImport
metadata:
  name: raw-disk-import
spec:
  pool: archive
  backendPath: archive/migration/raw-disk
  type: block
  capacity: 10737418240
  ownerNode: storage-node-a
  transport: nvme-tcp
  fsType: ""
  deletionPolicy: Retain
```

An existing filesystem import requires an export CIDR and finite refquota:

```yaml
apiVersion: zfs.csi.randomvariable.co.uk/v1alpha1
kind: VolumeImport
metadata:
  name: shared-data-import
spec:
  pool: archive
  backendPath: archive/migration/shared-data
  type: filesystem
  capacity: 107374182400
  ownerNode: storage-node-a
  nfsExportCIDRs:
    - 192.0.2.0/24
    - 2001:db8:42::/64
  nfsExportAccessMode: rw
  deletionPolicy: Retain
```

## See Also

- [Import an Existing ZFS Volume](../how-to/import-existing-zfs-volume.md) (how-to)
- [Imported Volume Safety Model](../explanation/imported-volume-safety.md) (explanation)
- [Volume (Internal API)](internal/volume.md) (reference)
- [Helm Values Reference](helm-values.md) (reference)

---

**Last Updated:** July 2026
**API Version:** zfs.csi.randomvariable.co.uk/v1alpha1
