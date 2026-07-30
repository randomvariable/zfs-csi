# Use Cases and Deployment Fit

zfs-csi turns a Linux host with an OpenZFS pool into disaggregated storage for a Kubernetes
cluster. It is most useful when you want Kubernetes-native lifecycle management while keeping
ZFS as the storage backend and standard network protocols on the data path.

## A Dedicated ZFS Storage Node for Kubernetes

The primary deployment is a Kubernetes node that owns one or more ZFS pools and serves
volumes to the rest of the cluster:

- zvol-backed block volumes travel over NVMe-TCP for databases, virtual machines, and other
  single-writer workloads;
- dataset-backed filesystems travel over NFS for shared content, build artifacts, and other
  multi-writer workloads; and
- Kubernetes custom resources hold desired state while the storage agent reconciles the
  local ZFS and export state.

Consumer nodes do not need ZFS. They need only the mainline NVMe-TCP initiator modules for
block volumes and an NFS version 4 client for shared filesystems. This keeps pool ownership on
the storage node while allowing workloads to run anywhere that can reach its storage network.

## Replace a TrueNAS Appliance

zfs-csi can replace the Kubernetes storage role of a TrueNAS appliance when the replacement
storage host can run Linux, join the Kubernetes cluster, and own the imported or newly created
ZFS pool. The resulting architecture removes the appliance API and SSH control path:

| Concern | TrueNAS-backed deployment | zfs-csi deployment |
| --- | --- | --- |
| Storage host | TrueNAS appliance | Linux Kubernetes storage node |
| Control path | Appliance API or remote commands | Kubernetes `Volume`, `Snapshot`, and `NVMeExport` resources |
| Block transport | Commonly iSCSI | NVMe-TCP |
| Shared filesystem | NFS or SMB | NFS version 4 |
| Reconciliation | External CSI driver against appliance state | Storage-node controllers against local ZFS and kernel state |
| Consumer-node ZFS | Not required | Not required |

This fit is strongest when TrueNAS exists primarily to provide persistent storage to one
Kubernetes cluster. It is a weaker fit when the appliance must continue serving SMB, sharing
datasets with non-Kubernetes clients, replicating through TrueNAS-specific workflows, or
providing its web administration experience. zfs-csi is a CSI driver, not a general NAS
management interface.

Replacing the appliance software does not require discarding the disks or pool. OpenZFS pool
import can provide an efficient host transition. Phase 1 can adopt eligible existing
unencrypted datasets or zvols as retained static volumes; encrypted or otherwise ineligible
objects remain migration sources for an operator-managed copy.

## Replace democratic-csi

zfs-csi can replace democratic-csi in deployments that use a ZFS storage host but want the
storage control plane to be Kubernetes-native and local to that host. The key change is not
only the driver name: ownership and transport assumptions change too.

| Concern | Typical democratic-csi deployment | zfs-csi deployment |
| --- | --- | --- |
| Backend management | Remote API or SSH to a storage system | Local `libzfs` calls on the storage node |
| Block transport | Often iSCSI | NVMe-TCP only |
| Filesystem transport | NFS | NFS version 4 |
| Existing CSI volume identity | democratic-csi volume handle | New zfs-csi volume handle |
| Static adoption | Backend- and driver-dependent | Retained import of validated unencrypted datasets and zvols |

Kubernetes cannot change the CSI driver or volume handle of a bound volume in place. Treat
the replacement as a data migration: install zfs-csi alongside the existing driver, provision
destination claims, quiesce each workload, copy or restore its data, and then switch the
workload to the new claim.

## Workload Patterns

### Databases and Stateful Services

Use an NVMe-TCP block StorageClass with `ReadWriteOnce` for a database or stateful service
that has one active writer. Choose XFS or ext4 according to the workload, and set an
appropriate zvol block size before provisioning because `volblocksize` is immutable.

### Kubernetes Virtual Machines

Use a block volume when a virtual machine needs a virtual disk backed by a ZFS zvol. Raw
`volumeMode: Block` claims are available when the workload needs a block device rather than a
filesystem mounted by kubelet. A single-writer claim supports ordinary VM ownership and
handoff; it does not provide a clustered filesystem or coordinate simultaneous writers.

### Shared Application Content

Use an NFS filesystem StorageClass with `ReadWriteMany` for web content, shared workspaces,
CI artifacts, or applications whose replicas need concurrent access. Set `nfsExportCIDRs` to
the consumer-node network and account for NFS semantics in the application.

### Per-Volume Encryption

Use an encrypted block StorageClass when each volume needs an independent ZFS native
encryption key. zfs-csi asks the configured OpenBao key provider to generate the key and
stores only its reference in Kubernetes. This protects data at rest without requiring ZFS on
consumer nodes.

## Cases That Need a Different Design

zfs-csi is not a direct fit for:

- highly available storage that must survive loss of the sole pool-owning storage node;
- local-PV deployments where pods should run on the same node as their ZFS data;
- iSCSI-only networks or clients, because the block transport is NVMe-TCP;
- SMB shares or general-purpose NAS administration;
- reuse of another CSI driver's `PersistentVolume` or volume handle; or
- cross-pool clones and restores, which require a real data copy rather than a ZFS clone.

The ZFS pool can provide disk redundancy, but that does not make the storage node itself
highly available. Plan host recovery, backups, and any ZFS send/receive replication outside
the CSI volume lifecycle.

## Further Reading

- [Migration Guide](../how-to/migrate-from-truenas-or-democratic-csi.md) (how-to)
- [Architecture](architecture.md) (explanation)
- [Storage Model](storage-model.md) (explanation)
- [Transport](transport.md) (explanation)
- [Version Compatibility](../reference/compatibility.md) (reference)
- [Imported Volume Safety Model](imported-volume-safety.md) (explanation)

---

**Last Updated:** July 2026
