# Kubernetes API Surface

This reference describes the standard Kubernetes objects you use to consume zfs-csi storage:
`PersistentVolume`, `PersistentVolumeClaim`, `VolumeSnapshot`, and `VolumeSnapshotClass`.
Application operators normally create claims and snapshots. Storage administrators also
create static PVs when adopting retained storage through `VolumeImport`. The driver's other
custom resources (`Volume`, `Snapshot`, `NVMeExport`) are internal and are documented
separately under [Internal API](internal/volume.md).

## PersistentVolumeClaim

You request storage with a `PersistentVolumeClaim` that references a zfs-csi StorageClass.

### Access Modes

The access mode a claim may request depends on the StorageClass volume type:

| Volume Type | StorageClass | Access Modes | volumeMode |
| --- | --- | --- | --- |
| Block (NVMe-TCP) | `zfs-tank-nvme`, `zfs-flash-nvme` | `ReadWriteOnce` | `Filesystem` (default) or `Block` |
| Filesystem (NFS) | `zfs-tank-nfs`, `zfs-flash-nfs` | `ReadWriteMany` | `Filesystem` |

**Note:** Block volumes are single-writer (`ReadWriteOnce`) — one node attaches the NVMe-TCP
target at a time. Filesystem volumes are shared (`ReadWriteMany`) — any number of nodes mount
the NFS export concurrently, provided those nodes are in the volume's selected network domain.

### volumeMode

A block-backed claim defaults to `volumeMode: Filesystem`, where the driver formats the
device (per the StorageClass `fsType`) and mounts it. Set `volumeMode: Block` to expose the
raw block device to the pod instead.

### Example

A block claim of 10Gi:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: app-data
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: zfs-tank-nvme
  resources:
    requests:
      storage: 10Gi
```

A shared filesystem claim:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: shared-data
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: zfs-tank-nfs
  resources:
    requests:
      storage: 100Gi
```

## Volume Expansion

All zfs-csi StorageClasses set `allowVolumeExpansion: true`. Enlarge a volume by editing the
claim's `spec.resources.requests.storage` to a larger value. Shrinking is not supported.

Imported static volumes do not support CSI expansion in Phase 1.

## Static PersistentVolume

A storage administrator binds an imported backend with a static `PersistentVolume` whose
`spec.csi.volumeHandle` exactly matches `VolumeImport.status.volumeHandle`. Filesystem imports
also require `volumeAttributes.provenance: Imported` and the authoritative
`volumeAttributes.exportPath` copied from `VolumeImport.status.exportPath`.

See [Import an Existing ZFS Volume](../how-to/import-existing-zfs-volume.md) for validated
block and NFS examples.

## VolumeSnapshotClass

A `VolumeSnapshotClass` selects the zfs-csi driver for snapshots. Create one before taking
snapshots:

```yaml
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata:
  name: zfs-tank-snapclass
driver: zfs.csi.randomvariable.co.uk
deletionPolicy: Delete
```

**Note:** The `VolumeSnapshot` custom resource definitions and the snapshot controller are a
cluster-wide component that the zfs-csi chart does not install. See
[Snapshot and Restore a Volume](../how-to/snapshot-and-restore.md) for the full setup.

## VolumeSnapshot

A `VolumeSnapshot` captures a point-in-time snapshot of a claim:

```yaml
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: app-data-snapshot
spec:
  volumeSnapshotClassName: zfs-tank-snapclass
  source:
    persistentVolumeClaimName: app-data
```

## Restoring and Cloning

Create a new claim from a snapshot with a `dataSource`, or clone an existing claim with a
`dataSource` that references another PVC:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: app-data-restored
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: zfs-tank-nvme
  dataSource:
    name: app-data-snapshot
    kind: VolumeSnapshot
    apiGroup: snapshot.storage.k8s.io
  resources:
    requests:
      storage: 10Gi
```

**Important:** A restore or clone must target the same ZFS pool as its source. The driver
enforces this at creation time and rejects a source in a different pool. See
[Storage Model](../explanation/storage-model.md) for why.

## Ephemeral Volumes

zfs-csi does not support CSI ephemeral inline volumes (`volumeLifecycleModes: Ephemeral`).
Use [generic ephemeral volumes](https://kubernetes.io/docs/concepts/storage/ephemeral-volumes/#generic-ephemeral-volumes)
instead, which provision through the normal claim path.

## StorageNode Inventory

`StorageNode` is a cluster-scoped, administrator-owned placement intent. One object represents
one logical storage owner; current runtime matches its name to storage-agent `NODE_NAME` and the
backing Kubernetes Node name. `spec.authoritativePoolGUIDs` and `spec.networkDomain` are immutable;
`spec.enabled` controls eligibility for new placement and defaults to `true`.

The owning storage agent writes `status`: readiness and freshness, `reachableFrom`, NFS and
NVMe-TCP endpoints, and pool observations. It may refresh valid endpoint status when owner
configuration changes; future publish and stage operations read current volume status, while
active mounts and attachments do not migrate automatically. Controllers have read-only
inventory access. Current multi-owner chart rollout remains unsupported; this API reference does
not constitute an enablement recipe.

Inspect the exact schema with:

```bash
kubectl explain storagenode.spec
kubectl explain storagenode.status
kubectl get storagenodes
```

See [Multi-Storage-Agent Topology and Placement](../explanation/multi-storage-agent-topology.md)
for field semantics and rollout status.

## See Also

- [StorageClass Reference](storage-classes.md) (reference)
- [Provision Your First Volume](../tutorials/getting-started.md) (tutorial)
- [Snapshot and Restore a Volume](../how-to/snapshot-and-restore.md) (how-to)
- [Storage Model](../explanation/storage-model.md) (explanation)
- [VolumeImport Reference](volume-import.md) (reference)

---

**Last Updated:** July 2026
