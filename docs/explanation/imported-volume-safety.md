# Imported Volume Safety Model

Phase 1 imports let zfs-csi serve an existing unencrypted ZFS dataset or zvol without
assuming ownership of its data lifecycle. This boundary is intentionally narrower than
dynamic provisioning: adoption configures transport and Kubernetes identity, while storage
administrators retain authority over dataset creation, migration, encryption, capacity, and
destruction.

## Why Import Is an Administrative Trust Boundary

A `PersistentVolumeClaim` asks for storage with ordinary workload-level intent. A
`VolumeImport` names an exact host-local ZFS object and authorizes zfs-csi to expose it over
the network. That object may contain data unrelated to Kubernetes, so creating an import has
more authority than creating a claim.

The chart therefore grants runtime identities permission to reconcile `VolumeImport`
resources but not to create them. Cluster and storage administrators must review backend
identity, data classification, network export policy, and rollback before admission.

## Adoption Versus Provisioning

Dynamic provisioning derives a backend path below `<pool>/csi/**`, creates the dataset or
zvol, applies properties, and destroys it when deletion policy permits. Import does none of
those ownership-establishing steps.

Instead, import validates an existing path, creates an internal retained `Volume`, and
reconciles only transport state:

- block volumes receive a deterministic CSI handle and an NVMe-TCP export;
- filesystem volumes use the dataset's observed ZFS mountpoint as authoritative NFS
  `exportPath`; and
- static PVs bind that handle into Kubernetes without calling CSI `CreateVolume`.

Rejecting `<pool>/csi/**` imports prevents two ownership models from claiming the same object.
A dynamically managed path cannot be converted into retained storage by adding an import.

## Why Phase 1 Is Retain-Only

Imported storage predates zfs-csi. The driver cannot prove who created it, who else references
it, whether snapshots or clones depend on it, or whether an external backup policy requires
it. `Retain` makes deletion monotonic and reversible: zfs-csi can withdraw access without
destroying data it did not create.

After all publications are removed, deleting the materialised imported `Volume` removes NVMe
transport state for block or driver-managed NFS sharing for filesystem. Filesystem de-adoption
sets `sharenfs=off` but does not unmount the imported dataset. An in-use guard blocks cleanup
while the volume remains published. Deleting a retained PV, PVC, or the decoupled
`VolumeImport` alone does not trigger that cleanup. Neither transport cleanup path destroys or
unmounts the backend, keys, snapshots, clones, or data. NFS adoption also avoids the `chmod
0777` initialization used for newly provisioned shared filesystems, preserving root mode, UID,
and GID.

## Why Validation Is Strict

Import accepts only states the driver can identify without mutation:

- encrypted datasets are rejected because Phase 1 has no safe key-ownership handoff;
- filesystem datasets require finite, non-zero `refquota`, because CSI capacity must map to
  an enforceable ZFS limit;
- zvol format must match `fsType`, preventing kubelet from formatting a populated device as
  though it were new;
- filesystem mountpoint must be observable, because a handle-derived path is unsafe for an
  arbitrary imported dataset; and
- duplicate claims fail deterministically, preventing two imports from serving one backend
  under different Kubernetes identities.

These checks reduce accidental data exposure. They do not establish application consistency
or prove source data correctness; operators still need backup, quiesce, checksum, and
application validation.

## Why Migration Remains Outside CSI

CSI lifecycle RPCs create, publish, expand, snapshot, and delete volumes. They are not a data
movement protocol. ZFS replication, CephFS file copying, database restore, and validation have
different failure modes and rollback requirements, so Phase 1 leaves them under explicit
operator control.

An operator may use `zfs send` and `zfs receive`, `rsync -aHAX`, or application-native
replication to build the destination. Import begins only after the destination is stable and
validated. This sequencing keeps failed data transfer separate from transport adoption and
makes the rollback boundary visible.

## Capability Boundaries

Imported volumes support publish, stage, mount, unstage, and retained de-adoption. They do not
gain dynamic-volume mutation features merely because they use a CSI handle. Phase 1 rejects
snapshots, clones, expansion, and `VolumeAttributesClass` changes by provenance.

This asymmetry is deliberate. Those operations mutate backend lineage, capacity, or
properties and would imply ownership that import explicitly withholds. Future phases need
separate contracts for each mutation rather than inheriting dynamic behavior by accident.

## Further Reading

- [Import an Existing ZFS Volume](../how-to/import-existing-zfs-volume.md) (how-to)
- [Migrate Data into zfs-csi](../how-to/migrate-data-into-zfs-csi.md) (how-to)
- [VolumeImport Reference](../reference/volume-import.md) (reference)
- [Storage Model](storage-model.md) (explanation)

---

**Last Updated:** July 2026
**Version Compatibility:** Phase 1 imports
