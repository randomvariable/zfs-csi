# Expand a Volume

This guide shows you how to grow an existing zfs-csi volume. All zfs-csi StorageClasses allow
expansion. Volumes can only grow — shrinking is not supported.

## Prerequisites

Before you begin, verify that you have the following:

- An existing bound `PersistentVolumeClaim` provisioned by zfs-csi.
- The StorageClass has `allowVolumeExpansion: true`. All chart-created classes do.

## Expand the Volume

### Step 1: Edit the Claim's Requested Size

Increase the claim's `spec.resources.requests.storage` to the new size. Patch it in place:

```bash
kubectl patch pvc database-data --type merge -p '{"spec":{"resources":{"requests":{"storage":"40Gi"}}}}'
```

**Caution:** Specify a size larger than the current request. A smaller value is rejected;
volumes cannot shrink.

### Step 2: Watch the Expansion

The controller expands the ZFS volume, and for block volumes the node grows the filesystem on
the attached device. Watch the claim's capacity:

```bash
kubectl get pvc database-data --watch
```

The `CAPACITY` column updates to the new size once expansion completes.

### Step 3: Confirm Inside the Pod

Confirm the workload sees the new capacity:

```bash
kubectl exec database -- df -h /var/lib/postgresql/data
```

The mounted filesystem should report the larger size.

## How Expansion Differs by Volume Type

- **Block volumes** expand in two stages: the controller enlarges the zvol, then the node
  grows the filesystem (`ext4` or `xfs`) on the device. Both stages are online — no pod
  restart is required.
- **Filesystem volumes** expand by growing the ZFS dataset's quota. The larger size is visible
  to all mounting pods without remount.

## Related Practices

- **Provisioning**: [Provision Block Storage](provision-block-storage.md) (how-to)
- **Kubernetes surface**: [Kubernetes API Surface](../reference/kubernetes-api.md) (reference)
