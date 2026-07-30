# Migrate from TrueNAS or democratic-csi

This guide describes practical migration paths from a TrueNAS-backed CSI deployment or from
democratic-csi to zfs-csi. It separates the storage-host transition from the Kubernetes
volume transition so that each step has a clear rollback point.

zfs-csi Phase 1 can adopt an existing unencrypted ZFS dataset or zvol through an
administrator-created `VolumeImport`. It cannot reuse another driver's `PersistentVolume` or
CSI handle. Choose between retained static import and copying into a new destination according
to backend state and lifecycle requirements.

## Choose a Migration Path

| Starting point | Recommended path | Downtime profile |
| --- | --- | --- |
| Existing unencrypted ZFS dataset or zvol that already meets the import contract | Import in place, create a retained static PV, then cut over | Transport cutover only |
| TrueNAS or democratic-csi NFS volume | Run both drivers, copy into a validated ZFS filesystem with `rsync -aHAX`, import it, then cut over | Short final cutover |
| TrueNAS or democratic-csi block volume with a mounted filesystem | Mount source and destination filesystems in a migration pod or maintenance host, copy files, then cut over | Short final cutover |
| Raw block volume or appliance zvol | Use application-native backup/restore where possible; otherwise perform an offline block copy into an equal-or-larger raw destination | Full copy window unless the application supports replication |
| Existing OpenZFS pool moving from TrueNAS to Linux | Export and import the pool on the new storage host, then import eligible unencrypted datasets or copy into validated destinations | Host transition plus per-volume cutover |

Prefer application-native replication or backup/restore for databases. A file or block copy
can be crash-consistent only; an application-native operation can preserve transaction and
recovery guarantees.

## Prerequisites

Before starting:

- inventory every source `StorageClass`, `PersistentVolume`, `PersistentVolumeClaim`, access
  mode, volume mode, requested capacity, and reclaim policy;
- record which workloads require an application-consistent backup or maintenance window;
- verify that the destination storage node and all consumer nodes meet the
  [compatibility requirements](../reference/compatibility.md);
- take an independent backup and test that it can be restored;
- ensure no destination claim is smaller than its source data or raw block device; and
- decide how long the old driver and storage endpoint will remain available for rollback.

Do not uninstall democratic-csi, disconnect TrueNAS, export the source pool, or delete source
claims before the destination workload has passed validation.

## Path A: Move an Existing OpenZFS Pool to the Storage Node

Use this path only when replacing the TrueNAS host software and moving the same pool to the
Linux storage node. Pool import moves the backing storage; a separate `VolumeImport` validates
and registers each eligible dataset or zvol with zfs-csi.

### Step 1: Stop Writers and Export the Pool

Stop every workload and service using the pool. Export it cleanly from TrueNAS using its
supported administration workflow. Confirm that no other host still imports or serves the
pool.

Never import the same writable pool on two hosts. Doing so can corrupt it.

### Step 2: Import and Verify on Linux

Attach the disks to the prepared Linux storage node, import the pool with OpenZFS, and verify:

- the expected datasets, zvols, snapshots, and properties are present;
- imported candidates are unencrypted, or encrypted sources remain migration sources rather
  than Phase 1 imports;
- no legacy NFS, SMB, or iSCSI export is unintentionally active; and
- a scrub or your normal pool-health checks report no unresolved errors.

The exact pool export/import commands depend on the source TrueNAS version, device naming,
encryption, and host layout. Follow the TrueNAS and OpenZFS procedures for those operations;
do not treat this guide as a substitute for a tested pool-move runbook.

### Step 3: Classify Imported Datasets

zfs-csi creates managed volumes below predictable paths such as `tank/csi/block` and
`tank/csi/filesystem`. Imports must remain outside `tank/csi/**`. Keep encrypted objects,
objects with ambiguous ownership, and filesystems without a finite `refquota` as migration
sources. Eligible unencrypted objects can follow
[Import an Existing ZFS Volume](import-existing-zfs-volume.md).

Do not manually create or import a dataset at a name zfs-csi derives for a dynamic claim.
`VolumeImport` explicitly rejects the managed subtree.

## Path B: Run Both Drivers and Copy Data

This is the default migration path and works whether the source storage stays on TrueNAS or
remains managed by democratic-csi during the transition.

### Step 1: Install zfs-csi Alongside the Source Driver

Prepare the nodes and [install zfs-csi with Helm](install-with-helm.md). Give its
StorageClasses distinct names and leave the existing default StorageClass unchanged until
the migration is complete.

Verify a disposable block or NFS claim end to end before moving production data. The two CSI
drivers can coexist because they register different provisioner names.

### Step 2: Create Destination Claims

For each source claim, create a zfs-csi destination with compatible semantics:

| Source requirement | zfs-csi destination |
| --- | --- |
| Single-writer filesystem on block storage | NVMe-TCP claim, `ReadWriteOnce`, default `volumeMode: Filesystem` |
| Raw block device | NVMe-TCP claim, `ReadWriteOnce`, `volumeMode: Block` |
| Shared filesystem | NFS claim, `ReadWriteMany` |

Request at least the source capacity. Select the destination pool, filesystem, block size,
compression, and encryption before provisioning; some ZFS properties cannot be changed later.

### Step 3: Perform the Initial Copy

For filesystem data, mount the source claim read-only and the destination claim read-write in
a purpose-built migration pod, Job, or maintenance host. Use a copy tool that preserves the
metadata your workload needs, including numeric ownership, modes, timestamps, hard links,
ACLs, and extended attributes where applicable.

For databases, prefer native replication, dump/restore, or physical backup tooling. For raw
block volumes, stop the workload before copying and copy to an equal-or-larger raw block
destination. Do not file-copy a mounted database and assume the result is application
consistent.

### Step 4: Quiesce and Finalise

Stop or scale down every source writer. Confirm that no Job, sidecar, maintenance pod, or
external client can still modify the source. Then:

1. run the final incremental file sync or application-native catch-up;
2. flush and unmount the source cleanly;
3. update the workload to reference the destination PVC; and
4. start the workload against zfs-csi.

PVC names are references in workload specs and cannot be swapped atomically. You can either
update the workload to a differently named destination claim or, after preserving the source,
recreate the claim name and workload in a controlled maintenance window.

### Step 5: Validate the Destination

Validate more than pod startup:

- read and write representative application data;
- verify ownership, permissions, ACLs, and extended attributes;
- run the application's integrity or recovery checks;
- restart or reschedule the pod and confirm the volume reattaches or remounts;
- take and restore a zfs-csi snapshot if snapshots are part of the recovery plan; and
- observe driver events, logs, and metrics during the validation window.

Keep the source read-only and intact until the rollback window expires.

## Roll Back a Cutover

If validation fails, stop all writers on the destination before returning to the source. Any
writes accepted after cutover create two divergent copies; do not simply start the old
workload and hope to reconcile them later.

Choose one authoritative copy, account for any writes made after cutover, update the workload
back to the source PVC, and restart it. Record the failure before retrying the migration so
that the next attempt addresses the cause rather than repeating the same procedure.

## Retire the Source

After the rollback window:

1. confirm that no workload, PersistentVolume, VolumeSnapshot, or backup job references the
   old driver;
2. change `Retain` policies or make final backups where required;
3. remove old claims and backend datasets according to the source driver's deletion policy;
4. uninstall democratic-csi or retire the TrueNAS Kubernetes storage services; and
5. remove obsolete iSCSI, NFS, API, and SSH credentials from consumer nodes and automation.

Review deletion carefully. A `Delete` reclaim policy can remove backend data when its claim
or PersistentVolume is deleted, while `Retain` deliberately leaves cleanup to the operator.

## Further Reading

- [Use Cases and Deployment Fit](../explanation/use-cases.md) (explanation)
- [Prepare Nodes](prepare-nodes.md) (how-to)
- [Install with Helm](install-with-helm.md) (how-to)
- [Provision Block Storage](provision-block-storage.md) (how-to)
- [Provision a Shared Filesystem](provision-shared-filesystem.md) (how-to)
- [Import an Existing ZFS Volume](import-existing-zfs-volume.md) (how-to)
- [Migrate Data into zfs-csi](migrate-data-into-zfs-csi.md) (how-to)
- [VolumeImport Reference](../reference/volume-import.md) (reference)
- [StorageClass Reference](../reference/storage-classes.md) (reference)

---

**Last Updated:** July 2026
