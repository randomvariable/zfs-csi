# Provision a Shared Filesystem

This guide shows you how to provision a shared filesystem volume backed by a ZFS dataset and
exported over NFS. Use a shared filesystem for `ReadWriteMany` workloads where several pods,
possibly on different nodes, read and write the same data.

## Prerequisites

Before you begin, verify that you have the following:

- zfs-csi installed and healthy. See [Install zfs-csi with Helm](install-with-helm.md).
- A filesystem StorageClass configured with an explicit export CIDR.
- The NFS export CIDR configured to cover your consumer nodes' network (see below).

## Configure the Export CIDR

An NFS export is only mountable from IP addresses within its permitted CIDR. Set the
CIDR to match your consumer-node network before provisioning; otherwise the driver rejects
the filesystem StorageClass and an enabled NFS chart class fails to render.

Set it once on the StorageClass through the Helm value `storageClasses.tankNFS.nfsExportCIDRs`,
for example `10.0.0.0/16` for an AWS VPC. Also set `storageClasses.tankNFS.enabled=true`. See the
[Helm Values Reference](../reference/helm-values.md).

## Provision a Shared Filesystem Volume

### Step 1: Create the Claim

Filesystem volumes are `ReadWriteMany`. Create a claim against the filesystem StorageClass:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: shared-content
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: zfs-tank-nfs
  resources:
    requests:
      storage: 100Gi
```

```bash
kubectl apply -f shared-content.yaml
```

**Note:** The `zfs-tank-nfs` StorageClass uses `Immediate` binding, so the volume is
provisioned as soon as the claim is created, before any pod consumes it.

### Step 2: Mount the Volume from Multiple Pods

Reference the same claim from a Deployment with several replicas. Every replica mounts the
same NFS export:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
spec:
  replicas: 3
  selector:
    matchLabels:
      app: web
  template:
    metadata:
      labels:
        app: web
    spec:
      containers:
        - name: web
          image: nginx:1.27
          volumeMounts:
            - name: content
              mountPath: /usr/share/nginx/html
      volumes:
        - name: content
          persistentVolumeClaim:
            claimName: shared-content
```

```bash
kubectl apply -f web.yaml
```

### Step 3: Verify Shared Access

Confirm the claim is bound and all replicas are running:

```bash
kubectl get pvc shared-content
kubectl get pods -l app=web
```

Write a file from one pod and read it from another to confirm the volume is shared:

```bash
POD_A=$(kubectl get pod -l app=web -o jsonpath='{.items[0].metadata.name}')
POD_B=$(kubectl get pod -l app=web -o jsonpath='{.items[1].metadata.name}')
kubectl exec "$POD_A" -- sh -c 'echo shared > /usr/share/nginx/html/index.html'
kubectl exec "$POD_B" -- cat /usr/share/nginx/html/index.html
```

You should see `shared` — the file written by the first pod is visible to the second.

## Related Practices

- **Block storage**: [Provision Block Storage](provision-block-storage.md) (how-to)
- **Troubleshooting NFS**: [Troubleshooting](troubleshooting.md) (how-to)
- **Parameters**: [StorageClass Reference](../reference/storage-classes.md) (reference)
- **Why NFS**: [Transport](../explanation/transport.md) (explanation)
