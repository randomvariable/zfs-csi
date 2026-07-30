# Uninstall zfs-csi

This guide shows you how to remove zfs-csi from a cluster cleanly. The driver protects its
volumes with finalizers, so the order of removal matters: remove the workloads and volumes
first, then the driver.

## Prerequisites

Before you begin, verify that you have the following:

- `kubectl` and `helm` configured against the cluster.
- Knowledge of which workloads use zfs-csi volumes.

**Caution:** Deleting a `PersistentVolumeClaim` bound to a `Delete`-reclaim StorageClass
destroys the underlying ZFS volume and its data. Back up anything you need first.

## Step 1: Remove Workloads Using zfs-csi Volumes

Delete the pods, Deployments, and StatefulSets that mount zfs-csi volumes. Until a volume is
no longer in use, it cannot be detached or deleted.

```bash
kubectl delete deployment,statefulset,pod -l <your-workload-selector>
```

## Step 2: Delete Snapshots and Claims

Delete `VolumeSnapshot` objects first, then the `PersistentVolumeClaim` objects. The driver's
finalizers hold each backing `Snapshot` and `Volume` custom resource until the storage agent
has destroyed the ZFS resource, so deletion may take a moment to complete.

```bash
kubectl delete volumesnapshot --all --all-namespaces
kubectl delete pvc <your-claims>
```

Confirm the driver's custom resources have drained. When empty, the finalizers have released:

```bash
kubectl get zv,zsnap,nvex
```

**If a `Volume` or `NVMeExport` is stuck deleting,** an initiator may still hold a live
NVMe-TCP connection — the `export-protect` finalizer blocks teardown while a connection is
live. Confirm no pod still uses the volume, then retry. See [Troubleshooting](troubleshooting.md).

## Step 3: Uninstall the Helm Release

With the volumes gone, uninstall the driver:

```bash
helm uninstall zfs-csi --namespace zfs-csi-system
```

## Step 4: Remove Remaining Cluster-Scoped Objects

Helm removes the chart's objects, but confirm the CSIDriver and StorageClasses are gone (they
are cluster-scoped):

```bash
kubectl get csidriver zfs.csi.randomvariable.co.uk
kubectl get storageclass | grep zfs-
```

**If any remain,** delete them explicitly:

```bash
kubectl delete csidriver zfs.csi.randomvariable.co.uk
kubectl delete storageclass zfs-tank-nvme zfs-tank-nfs
```

## Step 5: Clean the Storage Node (Optional)

The driver does not destroy the ZFS pool. **If you are decommissioning the storage node,**
the pool and its datasets remain until you remove them manually with the standard OpenZFS
tools. The label and taint you applied during preparation can also be removed:

```bash
kubectl label node <storage-node> zfs.csi.randomvariable.co.uk/storage-
kubectl taint node <storage-node> zfs.csi.randomvariable.co.uk/storage-
```

## Related Practices

- **Troubleshooting stuck deletes**: [Troubleshooting](troubleshooting.md) (how-to)
- **Reinstall**: [Install zfs-csi with Helm](install-with-helm.md) (how-to)
