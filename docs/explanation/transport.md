# Transport

This document explains how zfs-csi moves data between the storage node and consumer nodes:
NVMe-TCP for block volumes and NFS for shared filesystems. It covers why these transports were
chosen and how the block target is managed.

## Two Transports for Two Volume Types

zfs-csi serves two kinds of volume, and each has a transport suited to its access pattern.

- **Block volumes use NVMe-TCP.** A ZFS zvol is exported as an NVMe-oF namespace over TCP. A
  single consumer node attaches it as an initiator and sees a block device, which the node
  formats and mounts. Block volumes are single-writer (`ReadWriteOnce`).
- **Filesystem volumes use NFS.** A ZFS dataset is exported over NFS version 4, and any number
  of consumer nodes mount it concurrently. Filesystem volumes are shared (`ReadWriteMany`).

## Why NVMe-TCP for Block

NVMe-TCP was chosen over alternatives such as iSCSI for several reasons:

- **It is mainline and architecture-independent.** The `nvme-tcp` transport has been in the
  Linux kernel since the 5.x series and works identically on `amd64` and `arm64`. There is no
  out-of-tree module to build.
- **The target is simple.** The kernel NVMe-oF target is configured through configfs — a
  filesystem-like tree of directories and files under `/sys/kernel/config/nvmet`. There is no
  daemon and no separate configuration tool. Compared with the iSCSI target's login,
  authentication, and portal-group ceremony, the NVMe-oF target is lean.
- **It performs well.** NVMe-oF has lower protocol overhead than iSCSI, which suits a
  latency-sensitive block path.

The driver writes the target configuration into configfs directly, and the consumer connects
by writing a connect string to the kernel's `/dev/nvme-fabrics` interface. Neither side shells
out to a command-line tool, so consumer nodes do not need the `nvme-cli` package installed —
only the `nvme-tcp` and `nvme-fabrics` kernel modules.

## Managing the Block Target

The kernel `nvmet` configfs tree is volatile: it does not survive a reboot of the storage
node. Rather than persist and replay target configuration, zfs-csi treats the desired target
state as data in Kubernetes. Each `NVMeExport` custom resource declares one subsystem,
namespace, and allow-host set; the `nvmet` controller reconciles the live configfs tree to
match. When the storage node restarts, the controller rebuilds every target from the
`NVMeExport` resources.

A single component — the `nvmet` controller — is the only writer of the configfs tree. This
avoids two processes racing to configure the same kernel subsystem and makes the target state
a straightforward function of the declared exports.

Access control is exact: the `NVMeExport` `allowedInitiators` set is the desired allow-host
list, and the controller reconciles the kernel's admitted hosts to precisely that set. A
consumer is admitted only when its initiator NQN is present, and safe teardown is gated on
there being no live connection.

## Why NFS for Shared Filesystems

Block volumes cannot be shared safely between writers, so a `ReadWriteMany` volume needs a
different transport. NFS is the natural fit: it is a mature shared-filesystem protocol with a
client in every mainstream Linux distribution, and the export path is served by the node's own
in-kernel NFS server rather than a userland process inside a container.

## How Exports Are Served

The obvious way to export a ZFS dataset is to set the `sharenfs` property and let OpenZFS drive
`exportfs` for you. zfs-csi deliberately does not do that. Datasets are mounted with
`sharenfs=off`, so OpenZFS exports nothing, and the storage agent serves the export itself.

The Linux kernel does not hold a static export table. When a client touches an export, the NFS
server asks userspace for a decision by writing an upcall to a set of sunrpc cache channels under
`/proc/net/rpc` — `nfsd.export` for export options, `nfsd.fh` for filehandle-to-path mapping, and
`auth.unix.ip` for client authorisation. Conventionally `rpc.mountd` answers those upcalls. In
zfs-csi the storage agent answers them from an in-process responder.

This buys two things. The export table becomes desired state held in Kubernetes rather than
state persisted in `/etc/exports`, so a storage node reboot cannot leave stale exports behind and
recovery is just the reconciler re-registering what the `Volume` resources say should exist. More
importantly it makes per-volume export policy expressible at all: the `sharenfs` property cannot
represent `xprtsec=mtls`, so serving the upcalls directly is what allows one volume to require
mutual TLS while another does not.

Entries are established reactively, in response to kernel upcalls, rather than pushed
speculatively. A volume with no matching export gets a negative answer, which is the correct
fail-closed signal — the kernel rejects the mount instead of hanging the client indefinitely.

Because the agent answers the host's own kernel server, it runs with `hostNetwork` and elevated
privilege, and it shares that server with the host. The host's own `nfs-server`/`mountd` ownership
must therefore be disabled; the agent fails closed at startup on collision rather than fighting
another writer for the same kernel state.

Exports also need a mountpoint to exist before they mean anything, which is why the agent mounts
the dataset on every reconcile pass rather than only at creation — a reboot that wiped the kernel
mount and export cache is healed by the same idempotent path that created it.

Because the export is served from the host's mount namespace, the datasets the storage agent
mounts must be visible to the host — that is why the storage agent mounts the pool with shared
mount propagation. Without that, an NFS client would mount the export and find it empty.

## Securing Both Transports

Both transports can be encrypted in flight — NVMe-TCP with per-volume pre-shared keys, NFS with
mutual TLS. Both are opt-in per volume through a StorageClass parameter, and both fail closed.
See [Transport Security](transport-security.md) for the model and its boundaries.

## Further Reading

- [Architecture](architecture.md) (explanation)
- [Storage Model](storage-model.md) (explanation)
- [Transport Security](transport-security.md) (explanation)
- [Encryption](encryption.md) (explanation)
- [NVMeExport (Internal API)](../reference/internal/nvmeexport.md) (reference)

---

**Last Updated:** July 2026
