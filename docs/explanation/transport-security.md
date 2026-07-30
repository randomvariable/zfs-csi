# Transport Security

This document explains how zfs-csi protects volume traffic in flight: NVMe-TCP with per-volume
pre-shared keys, and NFS with mutual TLS. It covers what each mechanism proves, how credentials
reach the nodes that need them, and — importantly — what the model does *not* give you. Read the
security boundary section before relying on it.

Transport security is distinct from [encryption at rest](encryption.md). Encryption protects
blocks on disk; transport security protects them on the wire. They are complementary and
independently configured.

## Why the Kernel Does the Work

Both transports terminate in the Linux kernel — the `nvme-tcp` initiator and the NFS client are
kernel subsystems, and the data never passes through a userland proxy. The kernel therefore has to
perform the TLS handshake, and it does so by calling up to a userspace handshake agent, `tlshd`,
over a netlink interface. zfs-csi ships `tlshd` in its image and runs it as a privileged,
`hostNetwork` sidecar in both the node and storage DaemonSets, so it shares the network namespace
that owns the kernel-created transport sockets.

This shapes everything else. The driver does not implement TLS itself; it makes sure the right
credential is in the right place, in the right form, at the moment the kernel asks for it.

## NVMe-TCP: Per-Volume Pre-Shared Keys

Block volumes are secured with TLS pre-shared keys rather than certificates. Each volume gets its
own key, so compromising one volume's credential does not expose another.

The key lives in the kernel's `.nvme` keyring. On the storage node, the target installs a retained
PSK for each authorized initiator; on the consumer node, the initiator installs its own copy
before attaching. When the kernel performs the handshake it looks the key up from the keyring
itself — the key material never travels over the NVMe connection.

Two properties of that keyring shape the deployment. Keys are tagged with the network namespace of
the process that inserted them, so the component that installs a key and the `tlshd` that consumes
it must share a network namespace — which is why both run with `hostNetwork`. And the target-side
lookup is keyed on the *initiator's* NQN presented in the handshake, not the target's own, so each
retained key is installed under the identity of the initiator authorized to use it.

That identity binding is what limits the damage from a leaked key. A PSK is bound to a specific
(interchange, host NQN, subsystem NQN) tuple, so a key extracted from one node is inert unless the
holder is already an authorized initiator for that volume on the target.

Raw key material is deliberately kept out of the CSI API. The publish context carries only a TLS
mode flag and a *reference* to an immutable Kubernetes Secret; the privileged host-network staging
component reads the Secret and installs the key. Nothing in the kubelet-facing path ever handles
the key bytes. If the credential is missing or malformed, staging fails closed rather than
attaching in plaintext.

## NFS: Mutual TLS

Filesystem volumes are secured with RPC-with-TLS in mutual mode (`xprtsec=mtls`). Each consumer
node presents a client certificate that identifies the machine, each storage owner presents its
own server certificate, and a shared certificate authority establishes trust between them.

Because the `sharenfs` property cannot express `xprtsec`, TLS exports are only possible at all
because zfs-csi serves NFS export decisions from its own in-process responder rather than through
OpenZFS libshare. See [Transport](transport.md) for how that works.

Storage owners get per-owner server certificates rather than one shared leaf, because the
certificate has to match the endpoint the client connects to and owners have distinct endpoints.
The shared CA public material is distributed to consumers as read-only trust.

One consequence worth knowing before you plan a rollout: handshake policy applies per storage
endpoint, not per export. Enabling mutual TLS on an owner changes the handshake for every export
served by that owner, so mixing server-authenticated and mutually-authenticated exports requires
separate endpoints.

## Delivering Node Identity

Giving each consumer node its own client certificate is the genuinely difficult part, and it is
worth explaining why the obvious approaches do not work.

The node plugin runs from a DaemonSet, so every node's pod shares one ServiceAccount. That
identity cannot prove *which* node a given pod is running on: an authorization check answers
"is this ServiceAccount allowed to do this", not "is this caller the node it claims to be". Any
scheme where a shared identity fetches a per-node credential is therefore only as strong as the
weakest node holding that identity.

zfs-csi resolves this with Kubernetes
[PodCertificate](https://kubernetes.io/docs/concepts/security/) projection. The kubelet requests a
certificate on the pod's behalf, and the API server attests the pod, node and ServiceAccount
identity that the request is bound to. A custom signer shipped in this repository issues the
certificate against a single domain-prefixed signer name, trusting only the identity the API
server attested rather than anything the requesting client asserted. The result is a client
certificate whose binding to a specific node is established by the kubelet and API server, not by
the driver.

The certificate is projected into the pod as separate chain and key files and rotates on a short
lifetime. `tlshd` reopens its credential files on each handshake, so rotation needs no restart and
no signal. One wrinkle surfaces in the chart: the projection pins every file to a single mode,
while `tlshd` insists on `0600` for private keys and `0644` for certificates, so the material is
mirrored into a writable volume with the modes it demands.

PodCertificate is an alpha/beta Kubernetes feature and requires the `PodCertificateRequest`
feature gate enabled on the API server, controller manager and kubelet. This is a deliberate
dependency — see [Version Compatibility](../reference/compatibility.md).

## The Security Boundary — Read This

**Mutual TLS gives you machine identity and confidentiality. It does not give you
authorization.**

The kernel validates a client certificate only as far as CA membership: it checks that the
certificate chains to a trusted authority. It cannot map an individual certificate to a per-node
or per-user access decision, so possession of *any* valid client certificate issued by the CA
satisfies the NFS transport check.

Authorization therefore comes from the layers above it, and all of them are load-bearing:

- **Export CIDR ACLs** restrict which client addresses may mount a given volume at all.
- **AUTH_SYS** carries the client-supplied user identity for file permission checks.
- **root_squash** prevents a remote root from acting as root on the export.

Do not read certificate identity as tenant isolation. A workload that can reach a storage
endpoint from an allowed CIDR, on a node holding a valid client certificate, is authorized to the
extent those export ACLs and file permissions allow — the certificate is not an additional
per-volume gate.

For NVMe-TCP the situation differs: the PSK *is* per-volume, so a node without that volume's key
cannot complete a handshake for it. Authorization there comes from the target's allow-list of
initiator NQNs combined with per-volume key possession.

Both mechanisms fail closed. There is no plaintext fallback and no server-only downgrade path; a
volume that requires transport security and cannot obtain valid credentials fails to stage rather
than proceeding unprotected.

## When Transport Security Applies

Transport security is opt-in per volume, requested through a StorageClass parameter — `nfsTLS`
for filesystem volumes and `nvmeTLS` for block volumes. A volume provisioned from a StorageClass
without the relevant parameter uses the plaintext transport.

It also requires deployment-level enablement: the chart must be installed with transport TLS
enabled so that the signer, CA material and `tlshd` sidecars exist. A volume requesting TLS will
not become ready until the runtime is genuinely capable of serving it. See
[Enable Transport Security](../how-to/enable-transport-security.md) for setup.

## Further Reading

- [Enable Transport Security](../how-to/enable-transport-security.md) (how-to)
- [Transport](transport.md) (explanation)
- [Encryption](encryption.md) (explanation)
- [Version Compatibility](../reference/compatibility.md) (reference)
- [Helm Values Reference](../reference/helm-values.md) (reference)

---

**Last Updated:** July 2026
