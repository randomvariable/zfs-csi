# Enable Transport Security

This guide shows you how to encrypt volume traffic in flight: NVMe-TCP block volumes with
per-volume pre-shared keys, and NFS filesystem volumes with mutual TLS. Once enabled, volumes
provisioned from a TLS-enabled StorageClass fail closed rather than falling back to plaintext.

Before you rely on this, understand what it proves. Mutual TLS gives you machine identity and
confidentiality, **not** per-node authorization — the kernel validates client certificates only as
CA membership. Read [Transport Security](../explanation/transport-security.md), particularly the
security boundary section, before designing around it.

Transport security is independent of [encryption at rest](enable-encryption.md). You can enable
either, both, or neither.

## Prerequisites

Before you begin, verify that you have the following:

- Kubernetes v1.36 or later with the `PodCertificateRequest` feature gate enabled on the **API
  server, controller manager and kubelet**. Node client certificates are delivered exclusively
  through PodCertificate projection; without the gate, no node can obtain a credential and TLS
  volumes will never become ready. See [Version Compatibility](../reference/compatibility.md).
- Consumer and storage nodes running a kernel with `xprtsec` support for NFS and TLS support in
  the `nvme-tcp` initiator.
- The kernel modules loaded on the relevant nodes. The chart's init containers handle this:
  consumer nodes load `nvme-fabrics`, `nvme-tcp` and `nvme-keyring`; storage nodes additionally
  load the target-side modules. Consumer nodes must never load `nvmet`.
- Permission to install or upgrade the zfs-csi Helm release, and to create the signing namespace.

## Step 1: Enable TLS in the Chart

Transport TLS is controlled by the `network.tls` block. It is enabled by default:

```yaml
network:
  tls:
    enabled: true
    signingNamespace: ""
    signer:
      enabled: true
      replicas: 1
```

Enabling it deploys three things: the certificate signer, the certificate authority material, and
privileged `hostNetwork` `tlshd` sidecars in the node and storage DaemonSets that answer the
kernel's handshake upcalls.

The `tlshd` sidecars start only when both the node and storage workloads are enabled. If you are
running a partial installation, TLS volumes will not become ready.

## Step 2: Choose the Signing Namespace

The signing CA holds a private key and lives **outside** the driver namespace. This is
deliberate: runtime node and storage identities need broad Secret read permissions in the driver
namespace for dynamic NVMe pre-shared keys, and the CA signing key must not be reachable from
that blast radius.

Leave `signingNamespace` empty to use the default — the driver namespace suffixed with
`-signing` — or set it explicitly:

```yaml
network:
  tls:
    signingNamespace: zfs-csi-signing
```

The chart creates this namespace when it differs from the driver namespace. Storage agents
consume only the CA *public* material and their own server leaf certificates; they never see the
signing key.

## Step 3: Verify the Signer Is Running

Install or upgrade the release, then confirm the signer is healthy:

```bash
kubectl -n <signing-namespace> get deploy
kubectl -n <signing-namespace> logs deploy/zfs-csi-tls-signer
```

Then confirm nodes are actually receiving certificates. Each node plugin pod should have a
projected certificate; if the `PodCertificateRequest` feature gate is missing, the pod will not
start cleanly and the events will say why:

```bash
kubectl -n <driver-namespace> get pods -l app.kubernetes.io/component=node
kubectl -n <driver-namespace> describe pod <node-plugin-pod>
```

## Step 4: Enable TLS StorageClasses

Transport security is requested per volume through StorageClass parameters — `nfsTLS` for
filesystem volumes, `nvmeTLS` for block volumes. The chart ships TLS variants:

```yaml
storageClasses:
  tankNFSTLS:
    enabled: true
    name: zfs-tank-nfs-tls
    pool: tank
    nfsExportCIDRs: ["10.0.0.0/24"]
    reclaimPolicy: Delete
  tankNVMeTLS:
    enabled: true
    name: zfs-tank-nvme-tls
    pool: tank
    fsType: xfs
    blocksize: 16k
    reclaimPolicy: Delete
```

**Set `nfsExportCIDRs` to your consumer-node network.** This is not optional hardening — because
certificate identity does not authorize individual clients, the export CIDR list is doing real
access-control work. The chart default is a placeholder and will not admit your nodes.

`nvmeTLS` requires `type=block` with `transport=nvme-tcp`; `nfsTLS` requires `type=filesystem`.
The driver rejects mismatched combinations at provisioning time.

These StorageClasses are never marked as the cluster default. The chart deliberately avoids
affecting PVCs belonging to other drivers.

## Step 5: Verify a Volume

Provision a PVC from a TLS StorageClass and confirm it binds and mounts:

```bash
kubectl get pvc <name>
kubectl describe pvc <name>
```

For a filesystem volume, confirm the mount actually negotiated TLS on the consumer node — the
mount options should include `xprtsec=mtls`:

```bash
kubectl -n <driver-namespace> exec <node-plugin-pod> -c <container> -- \
  grep <volume-id> /proc/mounts
```

If the volume stays Pending or the pod cannot mount, that is the fail-closed behaviour working.
Check the `tlshd` sidecar logs on both the consumer and storage side.

## Troubleshooting

**Volume never becomes ready.** A volume requesting TLS will not go ready until the runtime is
genuinely capable of serving it — the storage side needs a valid endpoint-bound server
certificate. Check the storage agent and signer logs.

**Handshake fails with no obvious error.** Confirm `tlshd` is running as a `hostNetwork`
privileged sidecar on both ends. The kernel's handshake upcall is delivered over netlink in the
host network namespace; a `tlshd` in a different namespace will never see it.

**NVMe attach fails on a node that previously worked.** Confirm the `nvme-keyring` module is
loaded. Pre-shared keys live in the kernel `.nvme` keyring, and keys are tagged with the network
namespace of the process that inserted them — inserter and consumer must share the host namespace.

**Mount is rejected immediately.** Check the export CIDRs on the StorageClass actually cover the
consumer node's address. Fail-closed rejection is the correct response to an unauthorized client.

## Further Reading

- [Transport Security](../explanation/transport-security.md) (explanation)
- [Transport](../explanation/transport.md) (explanation)
- [Enable Per-Volume Encryption](enable-encryption.md) (how-to)
- [Version Compatibility](../reference/compatibility.md) (reference)
- [Helm Values Reference](../reference/helm-values.md) (reference)

---

**Last Updated:** July 2026
