# Snapshot and Restore a Volume

This guide shows you how to snapshot a zfs-csi volume and restore it into a new volume.
Snapshots use ZFS snapshots under the hood and are near-instant and space-efficient.

## Prerequisites

Before you begin, verify that you have the following:

- zfs-csi installed and healthy. See [Install zfs-csi with Helm](install-with-helm.md).
- The cluster-wide snapshot machinery installed (see the next section).
- A bound `PersistentVolumeClaim` to snapshot.

## Install the Snapshot Machinery

The zfs-csi driver ships the CSI snapshotter sidecar, but the `VolumeSnapshot` custom resource
definitions and the snapshot controller are a **cluster-wide** component that the chart does
not install. Install them once per cluster before creating snapshots.

### Step 1: Install the Snapshot CRDs and Controller

Apply the external-snapshotter CRDs and controller. This guide is validated against
external-snapshotter v8.2.0.

```bash
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/v8.2.0/client/config/crd/snapshot.storage.k8s.io_volumesnapshotclasses.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/v8.2.0/client/config/crd/snapshot.storage.k8s.io_volumesnapshotcontents.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/v8.2.0/client/config/crd/snapshot.storage.k8s.io_volumesnapshots.yaml
```

Then install the snapshot controller into `kube-system` following the external-snapshotter
deployment for v8.2.0. Confirm it is running:

```bash
kubectl -n kube-system rollout status deployment/snapshot-controller
```

### Step 2: Create a VolumeSnapshotClass

A `VolumeSnapshotClass` binds snapshots to the zfs-csi driver:

```yaml
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata:
  name: zfs-tank-snapclass
driver: zfs.csi.randomvariable.co.uk
deletionPolicy: Delete
```

```bash
kubectl apply -f zfs-tank-snapclass.yaml
```

## Take a Snapshot

### Step 1: Create a VolumeSnapshot

Snapshot an existing claim by referencing it as the source:

```yaml
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: database-snapshot
spec:
  volumeSnapshotClassName: zfs-tank-snapclass
  source:
    persistentVolumeClaimName: database-data
```

```bash
kubectl apply -f database-snapshot.yaml
```

### Step 2: Wait for the Snapshot to Be Ready

```bash
kubectl get volumesnapshot database-snapshot --watch
```

Wait until `READYTOUSE` is `true`. The snapshot is now available as a restore source.

## Restore into a New Volume

Create a new claim with a `dataSource` that references the snapshot. The new volume is a ZFS
clone of the snapshot:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: database-restored
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: zfs-tank-nvme
  dataSource:
    name: database-snapshot
    kind: VolumeSnapshot
    apiGroup: snapshot.storage.k8s.io
  resources:
    requests:
      storage: 20Gi
```

```bash
kubectl apply -f database-restored.yaml
```

**Important:** The restore must target the same ZFS pool as the snapshot's source. Restoring a
`tank` snapshot into a `flash` StorageClass is rejected, because a ZFS clone must share its
origin's pool. See [Storage Model](../explanation/storage-model.md).

## Cloning a Volume Directly

**If you want a copy of a live volume** without an intermediate snapshot, set the `dataSource`
to the source PVC instead:

```yaml
  dataSource:
    name: database-data
    kind: PersistentVolumeClaim
```

The same-pool rule applies to clones as well.

## Related Practices

- **Provisioning**: [Provision Block Storage](provision-block-storage.md) (how-to)
- **Kubernetes surface**: [Kubernetes API Surface](../reference/kubernetes-api.md) (reference)
- **Why same-pool**: [Storage Model](../explanation/storage-model.md) (explanation)
