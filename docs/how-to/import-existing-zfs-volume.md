# Import an Existing ZFS Volume

This guide shows storage administrators how to expose an existing, unencrypted ZFS
dataset or zvol as a retained static Kubernetes volume. Phase 1 imports validate and
adopt storage in place. They do not copy data, change dataset ownership, or transfer
deletion authority to zfs-csi.

**Caution:** Import is a storage-administrator operation. A wrong dataset path, volume
type, filesystem declaration, or NFS export policy can expose the wrong data or permit
unexpected access. Application teams must not create `VolumeImport` resources.

## Prerequisites

Before you begin, verify that you have the following:

- Cluster-admin access and storage-node shell access.
- zfs-csi installed with a retain-aware version on every storage node.
- An existing unencrypted ZFS dataset or zvol outside `<pool>/csi`.
- Exclusive control of the source. Stop legacy NFS, iSCSI, NVMe, or CSI exports before
  zfs-csi serves it.
- An independent backup and a tested rollback window.
- For filesystem imports, an authoritative ZFS `mountpoint`, a non-zero `refquota`, and
  the consumer-node IPv4/IPv6 CIDRs permitted to mount the NFS export.

See [VolumeImport Reference](../reference/volume-import.md) for all validation rules and
[Imported Volume Safety Model](../explanation/imported-volume-safety.md) for ownership
and deletion semantics.

## Step 1: Inspect the Existing Backend

Inspect properties on the storage node before enabling imports. This example uses
fictional pool `archive` and datasets below `archive/migration`.

For a filesystem dataset, run:

```bash
sudo zfs get -Hp -o property,value \
  type,encryption,refquota,mountpoint,sharenfs \
  archive/migration/team-share
sudo zfs list -Hp -o name,used,available,referenced,mountpoint \
  archive/migration/team-share
```

Continue only when `type` is `filesystem`, `encryption` is `off`, `refquota` is a
non-zero byte value, and `mountpoint` is an absolute path controlled by this dataset.
Record the mountpoint; it becomes the authoritative NFS `exportPath`.

For a zvol, run:

```bash
sudo zfs get -Hp -o property,value type,encryption,volsize \
  archive/migration/app-disk
sudo blkid -p -s TYPE -o value /dev/zvol/archive/migration/app-disk || true
```

Continue only when `type` is `volume`, `encryption` is `off`, and the filesystem probe
returns exactly `ext4`, `xfs`, or no output for raw block. Record `volsize` in bytes.

**Important:** Reject any candidate at `archive/csi` or below `archive/csi/**`. That
subtree belongs exclusively to dynamic provisioning.

## Step 2: Enable the Import Controller

Enable imports only after the retain-aware storage-agent image is deployed everywhere.
Set the feature gate in the Helm values file:

```yaml
storage:
  enabled: true
  enableVolumeImports: true
```

Apply the updated release:

```bash
helm upgrade --install zfs-csi ./charts/zfs-csi \
  --namespace zfs-csi-system \
  --values zfs-csi-values.yaml
```

Confirm that the storage agent received the gate:

```bash
kubectl -n zfs-csi-system get daemonset zfs-csi-storage \
  -o jsonpath='{.spec.template.spec.containers[0].args}'
```

The output must contain `--enable-volume-imports=true`.

## Step 3: Create the VolumeImport

Create one immutable, cluster-scoped `VolumeImport`. For an NFS filesystem,
use the observed dataset size as the minimum requested capacity:

```yaml
apiVersion: zfs.csi.randomvariable.co.uk/v1alpha1
kind: VolumeImport
metadata:
  name: team-share-import
spec:
  pool: archive
  backendPath: archive/migration/team-share
  type: filesystem
  capacity: 107374182400
  ownerNode: storage-node-a
  nfsExportCIDRs:
    - 192.0.2.0/24
    - 2001:db8:42::/64
  nfsExportAccessMode: rw
  deletionPolicy: Retain
```

For a formatted zvol, declare its existing format and `nvme-tcp` transport:

```yaml
apiVersion: zfs.csi.randomvariable.co.uk/v1alpha1
kind: VolumeImport
metadata:
  name: app-disk-import
spec:
  pool: archive
  backendPath: archive/migration/app-disk
  type: block
  capacity: 53687091200
  ownerNode: storage-node-a
  transport: nvme-tcp
  fsType: ext4
  deletionPolicy: Retain
```

Set `fsType: ""` for a raw zvol. Apply only the resource that matches the backend:

```bash
kubectl apply -f volume-import.yaml
```

## Step 4: Verify Validation and Materialisation

Wait for validation to finish, then inspect the generated handle and internal `Volume`:

```bash
kubectl -n zfs-csi-system wait --for=condition=Ready \
  volumeimport/team-share-import --timeout=2m
kubectl -n zfs-csi-system get volumeimport team-share-import -o yaml
kubectl -n zfs-csi-system get volume \
  "$(kubectl -n zfs-csi-system get volumeimport team-share-import \
    -o jsonpath='{.status.volumeRef}')" -o yaml
```

Verify all of the following before creating a `PersistentVolume`:

- `status.state` is `Ready` and `status.observedGeneration` matches
  `metadata.generation`.
- `status.volumeHandle`, `status.volumeRef`, and `status.actualCapacity` are non-empty.
- A filesystem import has the expected authoritative `status.exportPath`.
- The materialised `Volume` reports `spec.provenance: Imported`, the exact
  `spec.backendPath`, and `spec.deletionPolicy: Retain`.

**If validation reports `Failed`,** do not edit the immutable object or create a static
PV. Read `status.conditions`, delete the failed `VolumeImport`, correct the backend or
manifest, and create a new import.

## Step 5: Create a Static PV and PVC

Copy `status.volumeHandle` exactly. Do not derive or sanitize it yourself.

For the imported NFS filesystem, create a retained static PV and a matching PVC. Copy
the authoritative `status.exportPath` into `volumeAttributes.exportPath`:

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: imported-team-share
spec:
  capacity:
    storage: 100Gi
  accessModes:
    - ReadWriteMany
  volumeMode: Filesystem
  persistentVolumeReclaimPolicy: Retain
  storageClassName: zfs-imported-nfs
  mountOptions:
    - nfsvers=4.2
  csi:
    driver: zfs.csi.randomvariable.co.uk
    volumeHandle: csi:archive:filesystem:team-share-import
    volumeAttributes:
      provenance: Imported
      exportPath: /archive/migration/team-share
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: imported-team-share
  namespace: application-a
spec:
  accessModes:
    - ReadWriteMany
  volumeMode: Filesystem
  storageClassName: zfs-imported-nfs
  volumeName: imported-team-share
  resources:
    requests:
      storage: 100Gi
```

For an imported formatted zvol, use `ReadWriteOnce` and the validated filesystem type. Block
PV volume attributes do not carry provenance; the driver resolves it from the materialised
`Volume` during controller publish:

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: imported-app-disk
spec:
  capacity:
    storage: 50Gi
  accessModes:
    - ReadWriteOnce
  volumeMode: Filesystem
  persistentVolumeReclaimPolicy: Retain
  storageClassName: zfs-imported-block
  csi:
    driver: zfs.csi.randomvariable.co.uk
    volumeHandle: csi:archive:block:app-disk-import
    fsType: ext4
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: imported-app-disk
  namespace: application-a
spec:
  accessModes:
    - ReadWriteOnce
  volumeMode: Filesystem
  storageClassName: zfs-imported-block
  volumeName: imported-app-disk
  resources:
    requests:
      storage: 50Gi
```

Replace only the fictional handles with values copied from the corresponding import.
Apply the PV and PVC together:

```bash
kubectl apply -f imported-volume.yaml
kubectl -n application-a get pvc
```

The claim must report `Bound` to the named static PV.

## Step 6: Validate the Mounted Volume

Mount the claim in a maintenance pod before starting the application. Validate known files,
numeric ownership, permissions, ACLs, extended attributes, and application-level integrity.

For a block-backed filesystem, identify the stable attached device through udev rather than
using a transient `/dev/nvmeXnY` name:

```bash
kubectl -n application-a exec imported-volume-check -- \
  findmnt -no SOURCE,TARGET /mnt/imported
kubectl -n application-a exec imported-volume-check -- \
  ls -l /dev/disk/by-id/
```

For NFS, confirm the mounted source uses the expected server and authoritative export path:

```bash
kubectl -n application-a exec imported-volume-check -- \
  findmnt -no SOURCE,FSTYPE,OPTIONS /mnt/imported
```

Keep the old export disabled but recoverable until application validation and the rollback
window finish.

## Roll Back the Import

Stop all writers before rollback. Delete workload references, PVC, and PV first. A retained
static PV does not call CSI `DeleteVolume`, and deleting `VolumeImport` alone does not cascade
to the decoupled internal `Volume`. Delete the materialised `Volume` named by
`status.volumeRef` to remove transport state, wait for its finalizer to clear, and then delete
the `VolumeImport`. First verify no `VolumeAttachment` still references the PV; deletion stays
blocked while the volume remains published:

```bash
kubectl -n application-a delete pvc imported-team-share
kubectl delete pv imported-team-share
VOLUME_REF=$(kubectl -n zfs-csi-system get volumeimport team-share-import \
  -o jsonpath='{.status.volumeRef}')
kubectl get volumeattachment \
  --field-selector spec.source.persistentVolumeName=imported-team-share \
  -o custom-columns=NAME:.metadata.name,PV:.spec.source.persistentVolumeName,NODE:.spec.nodeName
kubectl -n zfs-csi-system delete volume "$VOLUME_REF"
kubectl -n zfs-csi-system wait --for=delete volume/"$VOLUME_REF" --timeout=2m
kubectl -n zfs-csi-system delete volumeimport team-share-import
```

Deletion of the materialised imported `Volume` removes zfs-csi transport state. For block
imports it removes the NVMe export. For filesystem imports it sets `sharenfs=off` and removes
the driver-created NFS exposure without unmounting the imported dataset. It does not delete or
unmount the dataset, zvol, encryption key, snapshots, clones, or data. Imported filesystem
sharing also preserves the dataset root mode, UID, and GID.

After transport removal, verify the backend still exists before restoring the legacy service:

```bash
sudo zfs list archive/migration/team-share
sudo zfs get sharenfs archive/migration/team-share
```

## Related Practices

- **Data migration**: [Migrate Data into zfs-csi](migrate-data-into-zfs-csi.md) (how-to)
- **Import contract**: [VolumeImport Reference](../reference/volume-import.md) (reference)
- **Safety rationale**: [Imported Volume Safety Model](../explanation/imported-volume-safety.md) (explanation)
- **General migration planning**: [Migrate from TrueNAS or democratic-csi](migrate-from-truenas-or-democratic-csi.md) (how-to)
