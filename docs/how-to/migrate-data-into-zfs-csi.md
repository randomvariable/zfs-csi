# Migrate Data into zfs-csi

This guide shows storage administrators how to stage data from another storage system,
validate it, and then import the destination with zfs-csi. Data transfer is an explicit
operator workflow outside CSI RPCs. zfs-csi does not run `zfs send`, `zfs receive`, `rsync`,
or application migration tools.

## Prerequisites

Before you begin, verify that you have the following:

- A prepared zfs-csi storage node with an imported destination pool.
- Storage-administrator access to both source and destination.
- Enough destination capacity for source data and transfer overhead.
- An application quiesce or native backup/restore plan.
- An independent backup and a tested rollback window.
- The prerequisites from [Import an Existing ZFS Volume](import-existing-zfs-volume.md).

## Choose a Transfer Path

| Source | Transfer path | Destination before import |
| --- | --- | --- |
| ZFS dataset or zvol | `zfs send` and `zfs receive` | Unencrypted ZFS dataset or zvol outside `<pool>/csi/**` |
| CephFS PVC or another mounted filesystem | Read source and copy with `rsync -aHAX` | Unencrypted ZFS filesystem dataset with non-zero `refquota` |
| Database or application with native replication | Application backup, restore, or replication | Backend validated by the application before import |

## Migrate ZFS to ZFS

### Step 1: Create and Validate a Source Snapshot

Quiesce the source application, create a named migration snapshot, and inspect it:

```bash
sudo zfs snapshot sourcepool/services/app@zfs-csi-migration
sudo zfs list -t snapshot sourcepool/services/app@zfs-csi-migration
```

Do not send a snapshot that was taken while the application was writing unless the
application explicitly supports crash-consistent recovery.

### Step 2: Receive Outside the Managed Subtree

Send the source snapshot and receive it into an operator-owned destination path. `-u` keeps
the received dataset unmounted while you validate properties and prevents accidental service:

```bash
sudo zfs send -v sourcepool/services/app@zfs-csi-migration | \
  sudo zfs receive -u archive/migration/app
```

For a final incremental catch-up, create a second quiesced snapshot and send from the first:

```bash
sudo zfs snapshot sourcepool/services/app@zfs-csi-cutover
sudo zfs send -v -i sourcepool/services/app@zfs-csi-migration \
  sourcepool/services/app@zfs-csi-cutover | \
  sudo zfs receive -u archive/migration/app
```

**Caution:** Do not add `zfs receive -F` by habit. `-F` rolls back the destination to the
most recent matching snapshot and destroys destination changes that conflict with the stream.
Use it only after proving the destination is disposable and recording the rollback point.

### Step 3: Normalize and Validate Destination Properties

Phase 1 cannot import encrypted objects. Confirm encryption is off, set a finite `refquota`
for a filesystem dataset, and set an authoritative mountpoint:

```bash
sudo zfs get -Hp -o property,value type,encryption,refquota,mountpoint \
  archive/migration/app
sudo zfs set refquota=200G archive/migration/app
sudo zfs set mountpoint=/archive/migration/app archive/migration/app
sudo zfs mount archive/migration/app
```

**If the destination is a zvol,** do not set `refquota` or `mountpoint`. Verify `volsize` and
probe its existing filesystem signature instead:

```bash
sudo zfs get -Hp -o property,value type,encryption,volsize \
  archive/migration/app-disk
sudo blkid -p -s TYPE -o value /dev/zvol/archive/migration/app-disk || true
```

Validate source and destination snapshots, file counts, representative checksums, and
application recovery before import:

```bash
sudo zfs diff sourcepool/services/app@zfs-csi-cutover || true
sudo find /archive/migration/app -xdev -type f -print0 | \
  sudo xargs -0 sha256sum > /var/tmp/app-destination.sha256
```

`zfs diff` describes changes relative to a local snapshot; it is not a cross-host checksum.
Use application verification or a separately generated source checksum manifest for the
authoritative comparison.

### Step 4: Import and Bind the Destination

Create the `VolumeImport`, wait for `Ready`, and create the static PV/PVC by following
[Import an Existing ZFS Volume](import-existing-zfs-volume.md).

## Migrate a CephFS PVC

### Step 1: Mount Source and Destination in a Copy Pod

Create a destination ZFS dataset outside `<pool>/csi/**`, set a non-zero `refquota`, and
mount it on the storage node. Export or otherwise mount it into a controlled migration pod
alongside the source CephFS PVC. Keep the source read-only during the final copy.

The pod should expose the source at `/source` and destination at `/destination`. Verify the
mounts before copying:

```bash
kubectl -n migration exec cephfs-copy -- \
  findmnt -no SOURCE,FSTYPE,OPTIONS /source /destination
```

### Step 2: Perform the Initial Copy

Copy files while preserving hard links, ACLs, and extended attributes:

```bash
kubectl -n migration exec cephfs-copy -- \
  rsync -aHAX --numeric-ids --info=progress2 /source/ /destination/
```

`rsync -aHAX` preserves ordinary metadata but does not provide application consistency.
Use database-native or application-native migration when file-level copying is insufficient.

### Step 3: Quiesce Writers and Perform the Final Copy

Stop all source writers, verify no maintenance job or sidecar can modify the CephFS PVC,
then run a checksum-verified final pass:

```bash
kubectl -n migration exec cephfs-copy -- \
  rsync -aHAX --numeric-ids --delete --checksum --itemize-changes \
  /source/ /destination/
kubectl -n migration exec cephfs-copy -- \
  rsync -aHAX --numeric-ids --delete --checksum --dry-run --itemize-changes \
  /source/ /destination/
```

The dry run must report no changes. Review every deletion from the final write pass before
accepting the destination.

### Step 4: Validate Data and Metadata

Compare byte totals, file counts, numeric ownership, modes, ACLs, and extended attributes:

```bash
kubectl -n migration exec cephfs-copy -- sh -ceu '
  du -sx --block-size=1 /source /destination
  find /source -xdev -printf "%y\n" | sort | uniq -c
  find /destination -xdev -printf "%y\n" | sort | uniq -c
  getfacl -p /source /destination >/tmp/acl-sample.txt
  getfattr -R -d -m- /source /destination >/tmp/xattr-sample.txt
'
```

Run application-specific integrity or recovery checks against the destination before
cutover. Retain the source CephFS PVC read-only until the rollback window expires.

### Step 5: Import and Bind the Destination

Create the `VolumeImport`, wait for `Ready`, and create the static NFS PV/PVC by following
[Import an Existing ZFS Volume](import-existing-zfs-volume.md). Use the exact destination
mountpoint reported in `status.exportPath`.

## Roll Back a Migration

Stop destination writers before switching back. Follow the de-adoption sequence in
[Roll Back the Import](import-existing-zfs-volume.md#roll-back-the-import) to remove zfs-csi
transport state while retaining the destination dataset. Restart the source only after
choosing it as the authoritative copy and accounting for any writes accepted after cutover.

Do not destroy source snapshots, CephFS claims, exports, or credentials until application
validation and the rollback window complete.

## Related Practices

- **Adopt destination storage**: [Import an Existing ZFS Volume](import-existing-zfs-volume.md) (how-to)
- **Migration planning**: [Migrate from TrueNAS or democratic-csi](migrate-from-truenas-or-democratic-csi.md) (how-to)
- **Import contract**: [VolumeImport Reference](../reference/volume-import.md) (reference)
- **Storage invariants**: [Storage Model](../explanation/storage-model.md) (explanation)
