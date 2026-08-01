# Snapshot (Internal API)

!!! warning "Internal API"
    `Snapshot` is an internal custom resource. The driver's controller creates and manages
    `Snapshot` objects in response to `VolumeSnapshot` creation; the storage agent reconciles
    them. **Do not create, edit, or delete `Snapshot` objects directly.** Take snapshots
    through the `VolumeSnapshot` API. This reference exists for debugging and development.

`Snapshot` is the desired and observed state of a single CSI-provisioned ZFS snapshot,
materialised by the storage agent.

| Property | Value |
| --- | --- |
| API group | `zfs.csi.randomvariable.co.uk` |
| API version | `v1alpha1` |
| Kind | `Snapshot` |
| Short name | `zsnap` |
| Scope | Namespaced |
| Finalizer | `zfs.csi.randomvariable.co.uk/snapshot-protect` |

## Spec

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `volumeRef` | string | Yes | Name of the parent `Volume` custom resource (same namespace). |
| `sourceVolumeID` | string | Yes | CSI source volume handle. |
| `sourceVolBlockSize` | string | No | Source volume's ZFS `volblocksize`/`recordsize` at snapshot time (for example `16k`). Immutable. A restore clones the snapshot and inherits this block size, so the controller aligns the restored volume's capacity to it — authoritatively, even when the parent `Volume` has been removed. Empty on filesystem sources, where `recordsize` constrains no capacity, and on snapshots taken by older drivers. `CreateSnapshot` now refuses a `block` source that itself records no block size rather than persisting an empty value here, and a restore from such a legacy snapshot is rejected with `FailedPrecondition`. |
| `snapName` | string | Yes | Human-readable CSI snapshot name; derives the ZFS snapshot leaf. |
| `snapshotID` | string | Yes | CSI snapshot handle returned to the snapshotter sidecar. |

## Status

| Field | Type | Description |
| --- | --- | --- |
| `conditions` | array | Standard condition list. Condition type: `Ready`. |
| `state` | string | Lifecycle state: `Pending`, `Ready`, `Error`, or `Deleting`. |
| `observedGeneration` | integer | The `spec` generation last reconciled. |
| `readyToUse` | boolean | Mirrors the CSI snapshot `readyToUse` field. |
| `size` | integer | Snapshot size in bytes. |
| `createdAt` | integer | ZFS snapshot creation time (Unix seconds). |
| `datasetPath` | string | Full ZFS snapshot name (for example `tank/csi/block/<id>@<snap>`). |

## Printer Columns

`kubectl get zsnap` shows: `Source`, `Ready`, `State`, `Age`.

## See Also

- [Kubernetes API Surface](../kubernetes-api.md) (reference)
- [Volume (Internal API)](volume.md) (reference)
- [Snapshot and Restore a Volume](../../how-to/snapshot-and-restore.md) (how-to)

---

**Last Updated:** July 2026
**API Version:** zfs.csi.randomvariable.co.uk/v1alpha1
