# Provision Your First Volume

In this tutorial, you create your first zfs-csi volume, attach it to a pod, write data to
it, and confirm the data survives a pod restart. By the end you will have seen a
`PersistentVolumeClaim` bind to a ZFS-backed volume and a pod use it.

This is a learning exercise on a working cluster. You do not need to understand how the
driver works internally to complete it — every step tells you exactly what to type and what
to expect.

## Before You Start

This tutorial assumes zfs-csi is already installed and healthy. If it is not, follow
[Prepare Nodes for zfs-csi](../how-to/prepare-nodes.md) and
[Install zfs-csi with Helm](../how-to/install-with-helm.md) first, then return here.

Confirm the `zfs-tank-nvme` StorageClass exists:

```bash
kubectl get storageclass zfs-tank-nvme
```

You should see:

```
NAME            PROVISIONER                    RECLAIMPOLICY   VOLUMEBINDINGMODE      AGE
zfs-tank-nvme   zfs.csi.randomvariable.co.uk   Delete          WaitForFirstConsumer   5m
```

If the StorageClass is missing, the driver is not installed correctly — return to the
install guide before continuing.

## Step 1: Create a Persistent Volume Claim

A `PersistentVolumeClaim` (PVC) is how you ask Kubernetes for storage. You do not create
ZFS datasets or NVMe targets yourself; you create a PVC and the driver does the rest.

Create a file named `my-first-volume.yaml` with exactly these contents:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-first-volume
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: zfs-tank-nvme
  resources:
    requests:
      storage: 1Gi
```

Apply it to the cluster:

```bash
kubectl apply -f my-first-volume.yaml
```

You should see:

```
persistentvolumeclaim/my-first-volume created
```

Now look at the claim:

```bash
kubectl get pvc my-first-volume
```

You should see the claim in the `Pending` state:

```
NAME              STATUS    VOLUME   CAPACITY   ACCESS MODES   STORAGECLASS    AGE
my-first-volume   Pending                                     zfs-tank-nvme   5s
```

**Note:** `Pending` is expected here, not an error. The `zfs-tank-nvme` StorageClass uses
`WaitForFirstConsumer`, so the driver waits until a pod actually uses the claim before it
creates the volume. You create that pod next.

## Step 2: Create a Pod That Uses the Volume

Create a file named `my-first-pod.yaml` with exactly these contents:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: my-first-pod
spec:
  containers:
    - name: app
      image: busybox:1.36
      command: ["sh", "-c", "sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: my-first-volume
```

Apply it:

```bash
kubectl apply -f my-first-pod.yaml
```

You should see:

```
pod/my-first-pod created
```

## Step 3: Watch the Volume Bind

Now that a pod consumes the claim, the driver creates a ZFS zvol, exports it over NVMe-TCP,
attaches it to the pod's node, and formats it. Watch the claim change to `Bound`:

```bash
kubectl get pvc my-first-volume --watch
```

Within a minute or two you should see the status change to `Bound`:

```
NAME              STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS
my-first-volume   Bound    pvc-3f8c1e2a-...                           1Gi        RWO            zfs-tank-nvme
```

Press `Ctrl+C` to stop watching. Confirm the pod is running:

```bash
kubectl get pod my-first-pod
```

You should see:

```
NAME           READY   STATUS    RESTARTS   AGE
my-first-pod   1/1     Running   0          90s
```

**Note:** The pod may take up to two minutes to reach `Running` while the volume is created
and attached for the first time.

## Step 4: Write Data to the Volume

Write a file into the mounted volume at `/data`:

```bash
kubectl exec my-first-pod -- sh -c 'echo "hello from zfs-csi" > /data/hello.txt'
```

Read it back to confirm it is there:

```bash
kubectl exec my-first-pod -- cat /data/hello.txt
```

You should see:

```
hello from zfs-csi
```

## Step 5: Confirm the Data Survives a Restart

The value of persistent storage is that data outlives the pod. Delete the pod:

```bash
kubectl delete pod my-first-pod
```

You should see:

```
pod "my-first-pod" deleted
```

Recreate it from the same file:

```bash
kubectl apply -f my-first-pod.yaml
```

Wait for it to run again, then read the file you wrote before:

```bash
kubectl wait --for=condition=Ready pod/my-first-pod --timeout=120s
kubectl exec my-first-pod -- cat /data/hello.txt
```

You should see the same content:

```
hello from zfs-csi
```

The data survived because it lives on the ZFS volume, not inside the pod. Notice that the
new pod re-attached the same volume: the driver detached the NVMe-TCP target from the old
pod and re-attached it to the new one automatically.

## Step 6: Clean Up

Remove the resources you created in this tutorial. Deleting the claim destroys the ZFS
volume, because the `zfs-tank-nvme` StorageClass uses the `Delete` reclaim policy.

```bash
kubectl delete pod my-first-pod
kubectl delete pvc my-first-volume
```

You should see both resources deleted:

```
pod "my-first-pod" deleted
persistentvolumeclaim "my-first-volume" deleted
```

## What You Learned

You created a `PersistentVolumeClaim`, watched the driver provision a ZFS zvol and attach it
over NVMe-TCP when a pod consumed it, wrote data, and confirmed the data survived a pod
restart. You did all of this through the standard Kubernetes storage API — the ZFS,
NVMe-TCP, and encryption machinery stayed out of your way.

## Next Steps

- Provision a shared, multi-node filesystem: [Provision a Shared Filesystem](../how-to/provision-shared-filesystem.md) (how-to)
- Take a snapshot and restore it: [Snapshot and Restore a Volume](../how-to/snapshot-and-restore.md) (how-to)
- Understand what happened behind the scenes: [Architecture](../explanation/architecture.md) (explanation)
