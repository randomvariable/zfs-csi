# Storage Model

This document explains the physical storage model zfs-csi assumes and the invariants behind
volume placement, cloning, restore, and import behavior.

## Disaggregated Storage

ZFS pools live on dedicated storage hosts while Kubernetes workloads consume volumes over the
network. Block volumes use NVMe-TCP; filesystem volumes use NFS. Consumer nodes are clients, not
pool owners.

The Helm chart supports multiple logical storage owners through `storageOwners`. Each owner has
its own pool GUID set, network domain, reachable consumer domains, and NFS/NVMe endpoints; an
empty `storageOwners` list retains the legacy single-owner `storageNode` configuration.

## Why Topology Describes Network Reachability

Kubernetes CSI topology answers where a consumer can reach storage. zfs-csi uses:

```text
topology.zfs.csi.randomvariable.co.uk/network-domain
```

It does not report storage-owner identity as consumer topology. A pod need not run on the host
that owns its pool; it needs a network path to that owner's persisted NFS or NVMe-TCP endpoint.

Placement selects fresh inventory whose `reachableFrom` contains an allowed domain, then
persists that domain with logical owner and pool GUID. `CreateVolumeResponse` returns the
persisted domain as `AccessibleTopology`, and `NodeGetInfo` publishes the same topology key.
Each consumer node currently advertises one network domain through the node plugin. That domain
must be included in `reachableFrom` for any storage owner selected for its volumes. Controller
replicas can be increased for owner-independent active-active operation, while each storage owner
continues to use one storage-agent Deployment.

## Pool and Owner Identity

Pool names are mutable administrative labels. Decimal ZFS pool GUIDs are immutable backend
identity. Each cluster-scoped `StorageNode` defines one logical owner and an immutable exact set
of `spec.authoritativePoolGUIDs` that its storage agent may mutate.

This decouples storage safety from Kubernetes Node UID. A host may be reinstalled while the same
owner and pools return. A same-named replacement pool has a new GUID and is different storage.

## Same-Pool Clone Invariant

ZFS clones and snapshot restores are same-pool operations. A new volume created from another
volume or snapshot is pinned to the source logical owner and pool GUID and must remain reachable
under the request's topology constraints.

Cross-owner or cross-pool movement is a real data migration, such as `zfs send`/`zfs receive`.
It is not a clone and is not performed by CSI placement.

## Imported Storage Has Separate Ownership

Phase 1 imports attach Kubernetes identity and transport to an existing unencrypted ZFS object
outside dynamically managed `<pool>/csi/**`. The canonical backend path remains authoritative.

Imported volumes are retain-only and reject snapshots, clones, expansion, and
`VolumeAttributesClass` changes. De-adoption removes NVMe or NFS transport while preserving the
dataset or zvol, key, snapshots, clones, and data. Filesystem imports persist their authoritative
NFS export path.

See [Imported Volume Safety Model](imported-volume-safety.md).

## Capacity Is Observed, Then Reserved

Storage agents publish per-pool free bytes and an observation marker. Placement subtracts
unaccounted reservations for non-terminal volumes; deleting volumes reserve until `Destroyed`.
The publisher stamps only volumes known materialized before its sample, allowing placement to
recognize capacity already included in free bytes without comparing clocks across hosts.

## Ephemeral Inline Volumes Are Not Supported

zfs-csi supports persistent volumes. CSI ephemeral inline volumes would bypass the controller,
but block provisioning requires controller intent, storage-agent export, and node-plugin attach.
Generic ephemeral PVCs provide pod-lifetime behavior through the normal reconciled path.

## Multiple Pools and Logical Owners

Placement filters by requested pool, source affinity, fresh effective capacity, and network
reachability. It then persists immutable `ownerNode`, `poolGUID`, and `networkDomain`.

The API intentionally permits up to 32 `authoritativePoolGUIDs` per owner. Multi-pool ownership
remains supported for NVMe block provisioning and future topology work; the API is not
single-pool.

This release limits NFS filesystem provisioning to one NFS-exportable pool root per enabled
owner. One owner-local storage-agent Deployment mounts one `poolMountRoot`, and one host nfsd
instance serves one NFSv4 `fsid=0` pseudoroot. Combining multiple authoritative pools with any
enabled chart NFS StorageClass is therefore rejected at Helm render time. The chart has no
owner-scoped NFS switch, so a multi-pool owner requires all chart NFS StorageClasses to be
disabled. Split NFS pools across owners with distinct endpoints instead.
Each such owner must be backed by a separate host nfsd instance; assigning multiple logical
owners to one host does not create isolated NFSv4 namespaces.

Future NFS multi-pool support should use one configured, host-global, non-overlay ZFS-backed
pseudoroot containing mounts for every exportable pool. It should not create separate per-pool
nfsd namespaces. Valid owner endpoint changes may refresh `Volume.status` for future publish and
stage operations, but active mounts and attachments do not migrate automatically.
See [Multi-Storage-Agent Topology and Placement](multi-storage-agent-topology.md) for the
current/future boundary.

## Further Reading

- [Architecture](architecture.md) (explanation)
- [Transport](transport.md) (explanation)
- [Topology and Scheduling](topology.md) (explanation)
- [Multi-Storage-Agent Topology and Placement](multi-storage-agent-topology.md) (explanation)
- [StorageClass Reference](../reference/storage-classes.md) (reference)
- [Imported Volume Safety Model](imported-volume-safety.md) (explanation)

---

**Last Updated:** July 2026
