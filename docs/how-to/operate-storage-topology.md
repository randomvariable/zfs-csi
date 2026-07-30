# Operate Storage Topology Safely

!!! warning "Inspection and recovery only"

    The current chart renders one logical `StorageNode` owner and one endpoint set. Use this
    guide to inspect current state and plan lifecycle work. Do not hand-build a production
    multi-agent deployment until the final chart rollout and two-storage-node IPv4/IPv6 E2E
    gate are complete.

This guide helps storage administrators inspect ownership, diagnose placement failures, and
perform safe logical-owner lifecycle operations without redirecting or destroying data.

## Prerequisites

- cluster-admin or storage-admin access to cluster-scoped zfs-csi resources;
- host access to inspect ZFS pool GUIDs and network endpoints;
- a maintenance window for pool, endpoint, or logical-owner lifecycle changes;
- application-level backup and rollback plan;
- no assumption that editing inventory migrates existing volumes.

## Inspect Logical Owners

List owner intent and inventory:

```bash
kubectl get storagenodes.zfs.csi.randomvariable.co.uk \
  -o custom-columns='OWNER:.metadata.name,ENABLED:.spec.enabled,DOMAIN:.spec.networkDomain,GUIDS:.spec.authoritativePoolGUIDs,READY:.status.conditions[?(@.type=="Ready")].status,OBSERVED:.status.lastObservedTime'

kubectl get storagenode <owner-name> -o yaml
kubectl get node <storage-host> \
  -o custom-columns='NAME:.metadata.name,READY:.status.conditions[?(@.type=="Ready")].status'
```

Confirm each declared pool GUID on the intended storage host. Pool name alone is insufficient:

```bash
zpool list -Hp -o name,guid,health,size,free
```

For current placement eligibility, verify all of these together:

1. `spec.enabled` is `true`.
2. The backing Kubernetes `Node` is `Ready`.
3. `status.observedGeneration` equals `metadata.generation`.
4. `Ready=True`, `lastObservedTime`, and each pool `capacityObservedAt` are fresh.
5. `status.pools` contains exactly the authoritative `ONLINE` GUID set.
6. `status.reachableFrom` contains expected consumer domains.
7. Both `status.endpoints.nfs` and `status.endpoints.nvme-tcp` have valid hosts and ports.

Do not "repair" a mismatch by changing authoritative GUIDs. The field is immutable because a
different GUID means different storage.

## Inspect Consumer Reachability

List consumer topology labels:

```bash
kubectl get nodes -L topology.zfs.csi.randomvariable.co.uk/network-domain
```

Label a consumer node only after proving both NFS and NVMe-TCP connectivity from that domain:

```bash
kubectl label node worker-a \
  topology.zfs.csi.randomvariable.co.uk/network-domain=storage-east
```

The current node plugin receives its reported segment through `--network-domain`; the
`NETWORK_DOMAIN` environment variable is the command default. The stock chart sets the flag
from one global `node.networkDomain` value, so do not patch the DaemonSet as a production
multi-agent enablement shortcut.

Test endpoint syntax independently:

```bash
# NFS mount source examples
mount -t nfs 192.0.2.44:/tank/csi/filesystem/example /mnt/check
mount -t nfs '[2001:db8:44::10]:/tank/csi/filesystem/example' /mnt/check

# NVMe-TCP portal examples
nvme discover -t tcp -a 192.0.2.45 -s 4420
nvme discover -t tcp -a 2001:db8:45::10 -s 4420
```

Unmount the test mount after validation. NFS mount-source IPv6 brackets belong around the
server in `server:/path`; OpenZFS `sharenfs` export-policy hosts use their own host/CIDR syntax.
NVMe tools commonly accept address and service separately, while persisted portal strings use
`host:port` or `[IPv6]:port` formatting.

## Trace Placement and Persisted Identity

Inspect cluster-scoped volume placement and protocol status:

```bash
kubectl get volumes.zfs.csi.randomvariable.co.uk \
  -o custom-columns='NAME:.metadata.name,OWNER:.spec.ownerNode,POOL-GUID:.spec.poolGUID,DOMAIN:.spec.networkDomain,STATE:.status.state'

kubectl get volume <volume-name> -o yaml
```

Check these fields before investigating transport:

- `spec.ownerNode`, `spec.poolGUID`, `spec.pool`, and `spec.networkDomain`;
- `spec.provenance` and `spec.deletionPolicy` for imported storage;
- `status.state`, `status.conditions`, `status.observedGeneration`, and
  `status.capacityAccountedAt`;
- filesystem `status.nfsServer` and `status.exportPath`;
- block `status.portalHost`, `status.portalPort`, `status.targetNQN`,
  `status.deviceGUID`, `status.mappedInitiators`, and `status.publishedInitiators`.

The placement fields in `spec` are immutable. The owning storage agent may refresh valid
endpoint and transport status when its configured endpoint changes; future publish and stage
operations read the current status. Do not edit status manually or treat an endpoint refresh as
owner or domain migration.

## Inspect Placement Serialization and Capacity

The placement lease is namespaced with the controller:

```bash
kubectl -n zfs-csi-system get lease zfs-csi-placement -o yaml
```

If provisioning pauses after a controller crash, inspect `holderIdentity`, `renewTime`, and
`leaseDurationSeconds`. Wait for normal lease expiry and controller recovery; do not delete a
live holder's lease. The maximum crash stall is bounded by lease expiry.

Pool capacity comes from `StorageNode.status.pools[*].freeBytes` and
`capacityObservedAt`. Inspect pending and deleting reservations alongside that sample:

```bash
kubectl get storagenode <owner-name> \
  -o jsonpath='{range .status.pools[*]}{.name}{"\t"}{.poolGUID}{"\t"}{.freeBytes}{"\t"}{.capacityObservedAt}{"\n"}{end}'

kubectl get volumes.zfs.csi.randomvariable.co.uk \
  -o custom-columns='NAME:.metadata.name,OWNER:.spec.ownerNode,POOL-GUID:.spec.poolGUID,CAPACITY:.spec.capacity,STATE:.status.state,ACCOUNTED:.status.capacityAccountedAt'
```

A volume reserves capacity while non-terminal. A deleting volume reserves until `Destroyed`.
Matching `capacityAccountedAt` and pool `capacityObservedAt` markers mean the pool sample already
includes that materialized volume. Do not compare timestamps from different hosts to infer this.

The legacy namespace ConfigMap with label `app.kubernetes.io/name=zfs-csi-capacity` remains a
fallback for unconstrained `GetCapacity`; topology-aware placement uses `StorageNode` inventory.

## Diagnose Common Failures

### `ResourceExhausted` During CreateVolume

`ResourceExhausted` means no eligible reachable pool remained after topology, requested pool or
source affinity, freshness, and effective-capacity filtering. Check:

```bash
kubectl get storagenodes -o yaml
kubectl get nodes -L topology.zfs.csi.randomvariable.co.uk/network-domain
kubectl -n zfs-csi-system get lease zfs-csi-placement -o yaml
kubectl -n zfs-csi-system logs deployment/zfs-csi-controller -c driver --since=15m
```

Look for stale inventory, disabled owners, `Ready=False`, malformed endpoints, absent domains,
requested pool mismatch, source affinity, or reservations consuming effective free space.
`preferred` topology cannot make an ineligible candidate eligible; `requisite` is mandatory.

### Inventory Is Missing, Stale, or Malformed

New placement excludes the owner. Existing volumes retain immutable owner, pool GUID, and
network domain. Their last published endpoint status remains available while the owner recovers.
Check storage-agent logs and Kubernetes events:

```bash
kubectl -n zfs-csi-system get pods -l app.kubernetes.io/component=storage
kubectl -n zfs-csi-system logs daemonset/zfs-csi-storage -c driver --since=15m
kubectl get events --all-namespaces --sort-by=.lastTimestamp
```

Restore the same logical owner, exact pool GUIDs, and valid endpoint publication. Do not create a
replacement `StorageNode` with a new name for the same data as a quick fix.

### Pool GUID Mismatch or Pool Absent

The storage agent fails closed before mutation and retains finalizers. Verify on the intended
host:

```bash
zpool status -g
zpool list -Hp -o name,guid,health
```

Recover or import the expected pool through explicit operator procedures. Never force-remove
finalizers merely to clear Kubernetes objects: doing so can orphan transport or backend state.
A same-named replacement pool has a different GUID and is not the original owner.

### Existing Volume Cannot Mount or Attach

Inspect the persisted `Volume.status` endpoint first, then consumer-side node plugin logs:

```bash
kubectl get volume <volume-name> -o yaml
kubectl -n zfs-csi-system get pods -l app.kubernetes.io/component=node -o wide
kubectl -n zfs-csi-system logs daemonset/zfs-csi-node -c driver --since=15m
kubectl get volumeattachments.storage.k8s.io -o wide
```

For block volumes, compare NQN, DeviceGUID, portal, mapped initiators, and published initiators.
For filesystems, compare NFS server and authoritative export path. The owning agent refreshes
valid endpoint status from its configuration. Future publish and stage operations use that
status, but an existing NFS mount or NVMe attachment stays on its current session until normal
unpublish, unstage, and restage.

## Restart and Maintenance Procedures

### Restart a Controller

Controller restart does not change persisted volume placement. A replacement waits for normal
leader election and placement lease availability. Existing node-stage operations continue to
use persisted status. Check the placement lease if new provisioning pauses.

### Restart a Storage Agent or Reinstall Its Host

1. Quiesce maintenance-sensitive workloads.
2. Preserve `StorageNode.metadata.name` and the exact pool GUID set.
3. Reinstall or restart the Kubernetes node and storage agent.
4. Import the same pool explicitly if required.
5. Wait for the Kubernetes `Node` and `StorageNode` to report `Ready` with fresh observations.
6. Verify each volume reports the intended valid owner endpoint before resuming workloads.

Kubernetes Node UID continuity is not required. The replacement Kubernetes node must retain the
same node name used by `StorageNode.metadata.name`/`NODE_NAME`, and the exact pool GUID set must
return.

### Change an Owner Endpoint

Changing a valid `network.portalHost` or `network.nfsServer` value keeps the same logical owner,
pool GUID, and network domain. Use a maintenance window:

1. Quiesce workloads that use the owner.
2. Change the endpoint configuration and roll the owning storage agent.
3. Wait for `StorageNode.status.endpoints` and each owned `Volume.status` endpoint to report the
   new value.
4. Unpublish, unstage, and restage workloads that must use the new endpoint.
5. Verify NFS mounts or NVMe attachments use the new endpoint before ending maintenance.

Active NFS mounts and NVMe attachments do not migrate automatically. A future publish or stage
reads current status. NVMe unstage can still use the transport identity persisted in node-local
staging state after owner status changes.

### Change a Network Domain

`StorageNode.spec.networkDomain` and `Volume.spec.networkDomain` are immutable. No transparent
owner/domain migration or failover exists. Use a migration plan: create storage under the new
identity, copy or restore data, quiesce applications, cut workloads over, validate, and retain
the old source for rollback.

### Replace a Pool

A replacement pool has a new GUID and is a new storage identity even when its name is unchanged.
Do not add its GUID to an existing immutable owner. Provision a new logical owner through the
future supported multi-agent chart path, migrate data explicitly, and cut workloads over.

### Rename a Logical Owner

Renaming is not supported in place. NQNs, DeviceGUIDs, persisted `ownerNode`, and finalizer safety
depend on owner identity. Treat a rename as explicit volume migration.

## Decommissioning Checklist

1. Stop new placement by setting `StorageNode.spec.enabled=false` through an administrator-
   controlled manifest update.
2. Inventory every `Volume`, `Snapshot`, and `VolumeImport` whose owner fields reference the
   logical owner.
3. Migrate dynamic volumes and snapshots explicitly; no automatic cross-owner migration exists.
4. For imported retained volumes, remove workloads and `VolumeAttachment`s, de-adopt transport
   through the documented retained-import procedure, and prove backend data remains.
5. Wait for dynamic volume deletion to reach `Destroyed`; do not assume a deletion timestamp
   freed capacity.
6. Confirm no persisted `ownerNode`, pool GUID, current NFS endpoint, NQN, or DeviceGUID references remain.
7. Export or retire pools through explicit ZFS operator procedures.
8. Remove the storage agent and authoritative object only after data and transport verification.

Do not delete and recreate authoritative identity objects casually. The Kubernetes object is
not the data, but it is the safety contract that decides which agent may mutate the data.

## Current Chart Boundary

The present chart has these single-owner mechanics:

- `storageNode.name` selects one logical owner;
- `storageNode.authoritativePoolGUIDs`, `storageNode.networkDomain`, and
  `storageNode.enabled` render one `StorageNode`;
- `network.portalHost` and `network.nfsServer` provide one endpoint set;
- every selected storage-agent pod receives the same `--node-name`, `--portal-host`,
  `--nfs-server`, and `--network-domain` values;
- the controller Deployment has one replica and is pinned by `storageNode.selector`;
- the node plugin supports `--network-domain`, but the chart supplies one global
  `node.networkDomain` value rather than final per-consumer-domain configuration.

There is no truthful multi-owner values example yet. A future supported rollout needs, at
minimum:

- independently configured logical owners and immutable pool GUID sets;
- independently selected storage-agent pods and endpoint/domain values;
- controller placement independent of any one owner and support for multiple replicas;
- per-consumer-node topology publication;
- documented owner/node-name continuity across restart, reinstall, replacement, and decommission;
- multi-owner RBAC/status-writer isolation;
- successful real two-storage-node IPv4/IPv6 provisioning and fault tests.

## Further Reading

- [Multi-Storage-Agent Topology and Placement](../explanation/multi-storage-agent-topology.md) (explanation)
- [Helm Values](../reference/helm-values.md) (reference)
- [Kubernetes API Surface](../reference/kubernetes-api.md) (reference)
- [Import an Existing ZFS Volume](import-existing-zfs-volume.md) (how-to)

---

**Last Updated:** July 2026
