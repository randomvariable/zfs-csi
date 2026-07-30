# Provision Block Storage

This guide shows you how to provision a block volume backed by a ZFS zvol and exported over
NVMe-TCP. Use block storage for single-writer workloads such as databases, where one pod owns
the volume.

## Prerequisites

Before you begin, verify that you have the following:

- zfs-csi installed and healthy. See [Install zfs-csi with Helm](install-with-helm.md).
- A block StorageClass. The chart creates `zfs-tank-nvme` by default.

## Provision a Block Volume

### Step 1: Create the Claim

Block volumes are `ReadWriteOnce`. Create a claim against the block StorageClass:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: database-data
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: zfs-tank-nvme
  resources:
    requests:
      storage: 20Gi
```

```bash
kubectl apply -f database-data.yaml
```

**Note:** The `zfs-tank-nvme` StorageClass uses `WaitForFirstConsumer`, so the claim stays
`Pending` until a pod consumes it. This is expected.

### Step 2: Consume the Claim

Reference the claim from a pod. The volume is created and attached when the pod is scheduled:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: database
spec:
  containers:
    - name: db
      image: postgres:16
      env:
        - name: POSTGRES_PASSWORD
          value: example
      volumeMounts:
        - name: data
          mountPath: /var/lib/postgresql/data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: database-data
```

```bash
kubectl apply -f database.yaml
```

### Step 3: Verify

Confirm the claim is bound and the pod is running:

```bash
kubectl get pvc database-data
kubectl get pod database
```

The claim should report `Bound` and the pod `Running`.

## Using a Raw Block Device

**If your workload wants a raw block device** rather than a formatted filesystem, set
`volumeMode: Block` on the claim and use `volumeDevices` instead of `volumeMounts` in the pod.

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: raw-block
spec:
  accessModes:
    - ReadWriteOnce
  volumeMode: Block
  storageClassName: zfs-tank-nvme
  resources:
    requests:
      storage: 20Gi
```

Reference it with `volumeDevices`:

```yaml
      volumeDevices:
        - name: data
          devicePath: /dev/xvda
```

## Choosing a Storage Tier

The chart defines two tiers, selected by StorageClass:

- `zfs-tank-nvme` — the `tank` pool.
- `zfs-flash-nvme` — the `flash` pool (disabled by default; enable it in the Helm values).

To place a volume on the `flash` tier, set `storageClassName: zfs-flash-nvme` on the claim.

## Related Practices

- **Filesystem storage**: [Provision a Shared Filesystem](provision-shared-filesystem.md) (how-to)
- **Expansion**: [Expand a Volume](expand-a-volume.md) (how-to)
- **Snapshots**: [Snapshot and Restore a Volume](snapshot-and-restore.md) (how-to)
- **Parameters**: [StorageClass Reference](../reference/storage-classes.md) (reference)
