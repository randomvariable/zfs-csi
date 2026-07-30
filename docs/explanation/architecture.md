# Architecture

This document explains how zfs-csi is structured and why. It covers the split between the
control plane and the storage node, the role each component plays, how a volume comes to
life, and the reconciliation model that keeps the system correct.

## The Problem zfs-csi Solves

A ZFS pool lives on a specific machine. The commands that create datasets and zvols, that
export them over NVMe-TCP, and that load encryption keys all have to run *on that machine*,
against its kernel and its ZFS pool. Kubernetes, meanwhile, wants to schedule the storage
control plane independently and to attach volumes from consumer nodes in the selected
network domain.

zfs-csi resolves this tension by splitting responsibilities across process roles that run in
different places, and by connecting them through Kubernetes custom resources rather than
through a remote-shell channel. This is a deliberate departure from the common pattern of a
controller that SSHes into a storage box to run `zfs` commands.

## Components

zfs-csi is a single binary that runs in one of several modes. Three roles carry the weight:

- The **controller** is the CSI control plane. It answers the CSI Controller RPCs
  (`CreateVolume`, `DeleteVolume`, `ControllerPublishVolume`, and so on) that the CSI
  sidecars call. It does not touch ZFS. Instead, it translates each request into a `Volume`
  or `Snapshot` custom resource and waits for that resource's status to report success.
- The **storage agent** runs on the storage node. It watches `Volume` and `Snapshot`
  resources and does the privileged work: creating datasets and zvols through the OpenZFS
  library, loading encryption keys, and setting up NFS exports. It is the local executor that
  a remote-shell controller would otherwise be.
- When explicitly enabled, the storage agent also watches administrator-created
  `VolumeImport` resources. It validates existing retained backends and materialises internal
  `Volume` intent without invoking CSI `CreateVolume` or taking dataset deletion ownership.
- The **node plugin** runs on every node. It answers the CSI Node RPCs that the kubelet
  calls, attaching the transport and mounting the volume for a pod. It is a router: it
  forwards the actual attach-and-mount work to two sidecars, one for NVMe-TCP and one for
  NFS.

A fourth role, the **`nvmet` controller**, owns the kernel NVMe-oF target. It is the sole
writer of the `nvmet` configfs tree, reconciling `NVMeExport` resources into live target
state. Keeping a single writer for configfs avoids two components racing to configure the
same kernel subsystem.

For the exact mode-to-workload mapping, see [Components and Workloads](../reference/components.md).

## Why Custom Resources Instead of a Direct Call

The controller could, in principle, call the storage agent over gRPC or SSH. zfs-csi instead
routes every controller-to-agent request through a `Volume` or `Snapshot` custom resource,
for three reasons:

- **The controller can move.** It is leader-elected and can be rescheduled. The privileged
  work is pinned to the storage node. Custom resources decouple the two: the controller
  records intent, and whichever storage agent is running picks it up.
- **No remote shell.** Replacing an SSH channel with declarative Kubernetes objects removes a
  class of security and reliability problems. There is no long-lived shell session to
  maintain, no command-string escaping, and no credential to rotate.
- **Reconciliation for free.** A custom resource has a spec and a status. The agent
  continuously drives the observed state toward the desired state, which makes the system
  self-correcting (see below).

## The Life of a Volume

When a pod needs a block volume, the sequence is:

1. A user creates a `PersistentVolumeClaim`. The external-provisioner sidecar calls the
   controller's `CreateVolume`.
2. The controller creates a `Volume` custom resource with the desired pool, size, type, and
   encryption reference, and waits.
3. The storage agent, watching `Volume` resources, creates the ZFS zvol (generating and
   loading an encryption key first if the volume is encrypted) and records the dataset path
   and capacity in the resource's status.
4. When the pod is scheduled, the external-attacher calls `ControllerPublishVolume`. The
   controller records the target node's initiator in the `Volume` status. The storage agent
   creates an `NVMeExport`, and the `nvmet` controller admits the initiator into the kernel
   target.
5. The kubelet calls the node plugin's `NodeStageVolume`. The node plugin routes the request
   to the `nvmet-stage` sidecar, which connects to the NVMe-TCP target, formats the device on
   first use, and mounts it.
6. The kubelet calls `NodePublishVolume`, and the sidecar bind-mounts the staged volume into
   the pod.

A filesystem volume follows a similar path, but the transport is NFS: there is no
per-initiator publish step, and the node plugin routes staging to the `nfs-stage` sidecar,
which mounts the export.

## The Reconciliation Model

zfs-csi is level-triggered, not edge-triggered. Each reconciler looks at the current desired
state (the resource spec) and the current observed state (the live ZFS pool, the kernel
target, the NFS exports) and makes the observed state match, whatever it currently is. It
does not assume it saw every intermediate event.

This matters most across restarts. The kernel `nvmet` configfs is volatile — it does not
survive a reboot of the storage node. Rather than persisting and replaying target
configuration, the storage agent and the `nvmet` controller rebuild the target state from the
`Volume` and `NVMeExport` resources when they start. The desired state is durable in
Kubernetes; the live kernel state is re-derived from it.

The manager also resyncs every watched object on a periodic interval (the `--sync-period`
flag). Each resync re-runs the reconcilers, which idempotently re-apply external state. If an
export is lost out of band, the next resync restores it.

## Idempotency

Because the CSI sidecars and the kubelet retry aggressively, every operation is keyed by the
CSI-supplied name or ID and is safe to repeat. Creating a volume twice with the same name
yields one zvol, not two. Publishing an already-published volume is a no-op. This is what
makes the level-triggered model safe under retry.

## Further Reading

- [Storage Model](storage-model.md) (explanation)
- [Transport](transport.md) (explanation)
- [Encryption](encryption.md) (explanation)
- [Components and Workloads](../reference/components.md) (reference)
- [Imported Volume Safety Model](imported-volume-safety.md) (explanation)

---

**Last Updated:** July 2026
