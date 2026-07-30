# Enable Per-Volume Encryption

This guide shows you how to enable ZFS native per-volume encryption, backed by OpenBao. Once
enabled, volumes provisioned from an encrypted StorageClass are created with their own
encryption key, and deleting such a volume crypto-shreds its data.

Before you rely on this, understand what it protects: encryption is **at rest on the storage
node only**. Read [Encryption](../explanation/encryption.md) for the security boundary,
including the fact that NVMe-TCP and NFS traffic is not encrypted in flight unless you separately
enable [transport security](enable-transport-security.md).

## Prerequisites

Before you begin, verify that you have the following:

- A reachable [OpenBao](https://openbao.org/) instance with the Transit secrets engine
  enabled.
- OpenBao configured for Kubernetes authentication, with a role the driver's ServiceAccount
  can assume.
- Permission to install or upgrade the zfs-csi Helm release.

## Step 1: Prepare OpenBao

Enable the Transit engine and create a role bound to the driver's ServiceAccount. The driver
authenticates as the `zfs-csi-storage` ServiceAccount in the release namespace by default.

The OpenBao role must permit the driver to create and use per-volume keys under the Transit
mount. Configure the Kubernetes auth role to map the `zfs-csi-storage` and `zfs-csi-controller`
ServiceAccounts to a policy granting Transit key generation and encryption or decryption.

**Note:** Use Kubernetes authentication rather than a static token wherever possible. A static
token is supported for development only.

## Step 2: Enable Encryption in the Helm Values

Set the encryption values to point at your OpenBao instance. Save this as
`encryption-values.yaml`:

```yaml
encryption:
  enabled: true
  openbao:
    addr: https://openbao.example.com:8200
    transitMount: transit
    role: zfs-csi-storage
```

Apply the change. `--reuse-values` preserves your existing install values while the file
layers the encryption settings on top:

```bash
helm upgrade zfs-csi ./charts/zfs-csi \
  --namespace zfs-csi-system \
  --reuse-values \
  --values encryption-values.yaml
```

Enabling encryption does two things: it configures the controller and storage agent to
authenticate to OpenBao, and it causes the chart to render an additional StorageClass named
`zfs-tank-nvme-encrypted`.

**Note:** `zfs-tank-nvme-encrypted` is the only encrypted StorageClass the chart renders.
There is no encrypted variant of the NFS or `flash` classes.

## Step 3: Provision an Encrypted Volume

Create a claim against the encrypted StorageClass:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: secret-data
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: zfs-tank-nvme-encrypted
  resources:
    requests:
      storage: 10Gi
```

```bash
kubectl apply -f secret-data.yaml
```

The driver generates a per-volume key through OpenBao Transit and creates the dataset with
`encryption=on`.

## Step 4: Verify Encryption

Confirm the volume's `Volume` custom resource reports the key as available. The
`status.keyStatus` field should read `Available` and the `Encrypted` condition should be true:

```bash
kubectl get zv -o custom-columns=NAME:.metadata.name,KEY:.status.keyStatus
```

## How Deletion Crypto-Shreds Data

When you delete an encrypted volume, the driver destroys the ZFS dataset and removes the
volume's key from OpenBao. Without the key, the underlying blocks are unrecoverable
ciphertext. This per-volume crypto-shred is the reason to build encryption into the driver
rather than relying on whole-pool encryption.

## Related Practices

- **Security boundary**: [Encryption](../explanation/encryption.md) (explanation)
- **Values**: [Helm Values Reference](../reference/helm-values.md) (reference)
- **Install**: [Install zfs-csi with Helm](install-with-helm.md) (how-to)
