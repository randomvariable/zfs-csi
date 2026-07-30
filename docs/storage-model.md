# zfs-csi physical storage model

This note documents the physical storage model zfs-csi assumes, the invariants that
follow from it, and why the driver reports the topology it does. It is the reference
for scheduling, clone/restore placement, and future multi-pool work.

## Disaggregated model

zfs-csi is **disaggregated**: ZFS pools live on one or more **dedicated storage
nodes**, and volumes are consumed over the network:

- **block** volumes via **NVMe-TCP** (the storage node exports an nvmet target;
  consumer nodes in the selected network domain connect as initiators),
- **filesystem** volumes via **NFS** (the storage node exports the dataset;
  consumer nodes in the selected network domain mount it).

Consequently a provisioned volume is reachable from consumer nodes in its persisted
network domain. There is no node-local storage path and no hyperconverged "pool is on
the consumer" assumption.

## Topology describes network reachability

The controller persists the selected reachability domain in
`Volume.spec.networkDomain` and returns it from `CreateVolumeResponse` as
`AccessibleTopology`. `NodeGetInfo` publishes the same
`topology.zfs.csi.randomvariable.co.uk/network-domain` key for each consumer node.

Reporting storage-node affinity here would be wrong: it would constrain the scheduler
to co-locate pods with the storage node. Scheduler-facing topology instead constrains
pods to consumer nodes in the volume's selected network domain.

`CSIStorageCapacity` is likewise left off (`storageCapacity: false`): global capacity
is near-useless for network storage and per-node capacity only pays off under a
node-affinity/topology model, which does not apply here.

## Same-pool clone invariant

`zfs clone` (and therefore snapshot-restore and PVC-clone) is **same-pool-only**: a
clone always lives in the same pool as its origin. A volume created from a snapshot or
PVC source is therefore **pinned to the source's pool** — and thus to the storage node
hosting that pool.

Today there is a single pool (`tank`) on a single storage node, so this is trivially
satisfied. It becomes load-bearing with multiple pools / multiple storage nodes.

### Enforcement

The constraint is enforced at **CreateVolume**, not via topology. `validateVolumeCon
tentSource` (`internal/driver/controller.go`) rejects a content source whose pool (or
kind) differs from the requested StorageClass with `InvalidArgument`:

- snapshot source in a different pool → rejected,
- volume (clone) source in a different pool → rejected.

Covered by `TestCreateVolume_RejectsMismatchedSnapshotSource` and
`TestCreateVolume_RejectsMismatchedVolumeSource`.

This keeps backend placement correct without leaking storage-owner affinity into
consumer scheduling. The *source* owner and pool selection are validated up front,
while the persisted network domain describes consumer reachability. A cross-pool copy
(if ever wanted) would require a real data copy (send/recv), not `zfs clone`, and is out
of scope.

## Future: multiple pools / storage nodes

When more than one pool or storage node exists:

- StorageClass `pool` parameter selects the target pool (already supported).
- Clone/restore sources must remain in the target pool (already enforced).
- Placement persists immutable logical owner, pool GUID, and network domain identity.
- Valid owner endpoint changes may refresh volume status used by future publish and
  stage operations. Existing mounts and attachments do not migrate automatically, and
  no transparent owner/domain migration or failover exists.
- Production multi-owner deployment remains unsupported until the chart independently
  configures owners and per-consumer domains, supports controller rollout beyond one
  owner-pinned replica, documents owner/node-name continuity, and passes real
  two-storage-node IPv4/IPv6 fault E2E.

## CSI ephemeral inline volumes: not supported (by design)

The CSIDriver object declares `volumeLifecycleModes: [Persistent]` only — CSI
*ephemeral inline* volumes (`volumeLifecycleModes: Ephemeral`, a volume defined
inline in a Pod spec and provisioned during `NodePublishVolume`) are
deliberately unsupported.

Rationale:

- **Generic ephemeral volumes already cover the use case.** A PVC-backed
  ephemeral volume (`Pod.spec.volumes[].ephemeral.volumeClaimTemplate`) gives the
  same "volume lives and dies with the Pod" lifecycle, but goes through the
  normal controller provisioning path (CreateVolume → Volume CR → agent
  materialises the dataset + export). This is proven working (the conformance
  generic-ephemeral suites pass).
- **Inline is architecturally wrong for this driver.** CSI inline volumes are
  provisioned entirely inside `NodePublishVolume` on the node plugin, bypassing
  the controller. This driver's block path requires the *controller* to create
  the zvol, the *storage agent* to export it over NVMe-TCP, and only then the
  node to attach — a three-party dance the node plugin cannot orchestrate alone
  during NodePublish. Forcing zvol-create + nvmet-export + teardown into the node
  plugin would duplicate the controller/agent logic and lose the level-triggered
  reconcile guarantees.

Users who want Pod-scoped volumes should use generic ephemeral volumes.
