# Encryption

This document explains zfs-csi's per-volume encryption: what it protects, how keys flow from
OpenBao to ZFS, and — importantly — what it does *not* protect. Read the security boundary
section carefully before relying on encryption.

## Per-Volume Keys

zfs-csi uses ZFS native encryption with one key per volume. Each encrypted volume is a ZFS
dataset created with `encryption=on` and its own data-encryption key (DEK). This is finer
grained than encrypting a whole pool with a single key: because each volume has a distinct
key, a volume's data can be rendered permanently unrecoverable by destroying just its key,
without touching any other volume.

## How Keys Flow

The key material comes from [OpenBao](https://openbao.org/) Transit, and it is handled so that
the plaintext key touches disk as briefly as possible and never travels over the CSI wire.

1. When the controller provisions an encrypted volume, it obtains a per-volume key reference
   from OpenBao and records only that *reference* on the `Volume` resource. The key material
   itself never crosses the CSI API.
2. On the storage node, the storage agent fetches the key from OpenBao using its own
   Kubernetes-authenticated OpenBao identity, writes it to a `tmpfs` (memory-backed) path,
   creates or unlocks the dataset with that key, and then shreds the `tmpfs` copy. The key is
   never written to persistent storage.
3. Before a volume is exported or attached, the agent confirms the key is loaded and the
   dataset is unlocked.

The storage agent authenticates to OpenBao with its Kubernetes ServiceAccount rather than a
static token, so there is no long-lived secret to distribute or rotate for the driver itself.

## Crypto-Shred on Delete

Because each volume's key is independent, deleting a volume can make its data unrecoverable by
destroying the key, not just the dataset. When an encrypted volume is deleted, the driver
destroys the dataset and removes the OpenBao key. Even if the underlying blocks were somehow
recovered, they remain ciphertext with no key to decrypt them. This per-volume crypto-shred is
the capability that motivated building encryption into the driver.

### Orphaned keys on controller crash (accepted risk)

The controller mints a volume's key in OpenBao *before* it creates the `Volume` resource. If
the controller process crashes in the narrow window between those two steps, the freshly minted
key is left in OpenBao with no `Volume` referencing it. This is a benign leak, not a data or
security exposure: the orphaned key protects no data (its dataset was never created), and the
provisioning request is retried by the external provisioner, which mints a fresh key and
succeeds. The driver does **not** run a background garbage collector for such orphans — Transit
keys are cheap, the window is vanishingly small, and a GC would need to enumerate all Transit
keys and cross-check every namespace, adding standing OpenBao permissions and load for no
data-integrity benefit. Operators who want to reclaim orphaned keys can list Transit keys under
the driver's key prefix and delete any with no matching `Volume`.

## The Security Boundary — Read This

zfs-csi encryption protects data **at rest on the storage node**. It does not encrypt data in
flight.

A volume created from an encrypted dataset is decrypted on the storage node before its blocks
are sent over the network. Whether that traffic is protected on the wire is a separate,
independently configured concern.

zfs-csi can secure both transports in flight — NVMe-TCP with per-volume pre-shared keys, and NFS
with mutual TLS — but this is **opt-in per volume** and off unless you request it. A volume
provisioned from a StorageClass without `nvmeTLS` or `nfsTLS` travels the network in plaintext,
even when its data is encrypted at rest. See
[Transport Security](transport-security.md) for the model and its boundaries, and
[Enable Transport Security](../how-to/enable-transport-security.md) for setup.

If you do not enable transport security, and your threat model includes an attacker on the
storage network, you need in-flight encryption from another layer — for example WireGuard
node-to-node encryption at the cluster network layer. The driver's at-rest encryption and
in-flight protection are complementary: one protects the blocks on disk, the other protects them
on the wire.

## When Encryption Applies

Encryption is opt-in. It is enabled at install time (the driver and agent must be configured
with an OpenBao address and role) and requested per volume through a StorageClass. A volume
provisioned from a StorageClass without the encryption parameter is stored unencrypted. See
[Enable Per-Volume Encryption](../how-to/enable-encryption.md) for the setup.

## Further Reading

- [Enable Per-Volume Encryption](../how-to/enable-encryption.md) (how-to)
- [Transport Security](transport-security.md) (explanation)
- [Enable Transport Security](../how-to/enable-transport-security.md) (how-to)
- [Architecture](architecture.md) (explanation)
- [Helm Values Reference](../reference/helm-values.md) (reference)

---

**Last Updated:** July 2026
