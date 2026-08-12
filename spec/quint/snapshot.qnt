// Snapshot CR/ZFS lifecycle.
// Source: internal/driver/controller.go:846-912,1223-1323;
// internal/agent/snapshot_reconciler.go:217-286,300-353;
// internal/agent/volume_reconciler.go:2326-2348;
// internal/libzfs/libzfs.go:474-479,634-656,853-880.
module snapshot {
  const Snapshots: Set[str]

  var volumeExists: bool
  var volumeDeleteRequested: bool
  var volumeFinalizer: bool
  var zfsSnapshot: Set[str]
  var snapshotCR: Set[str]
  var snapshotFinalizer: Set[str]
  var snapshotReady: Set[str]
  var snapshotDeleteRequested: Set[str]
  var cloneDependsOn: Set[str]

  // CreateSnapshot persists intent first. The controller rejects a source whose
  // Volume CR is deleting; the agent's idempotent ZFS Snapshot then makes
  // ReadyToUse true and records status.datasetPath.
  action createSnapshot(s: str): bool = all {
    volumeExists,
    not(volumeDeleteRequested),
    not(snapshotCR.contains(s)),
    volumeExists' = volumeExists,
    volumeDeleteRequested' = volumeDeleteRequested,
    volumeFinalizer' = volumeFinalizer,
    zfsSnapshot' = zfsSnapshot.union(Set(s)),
    snapshotCR' = snapshotCR.union(Set(s)),
    snapshotFinalizer' = snapshotFinalizer.union(Set(s)),
    snapshotReady' = snapshotReady.union(Set(s)),
    snapshotDeleteRequested' = snapshotDeleteRequested,
    cloneDependsOn' = cloneDependsOn
  }

  // DeleteVolume requests parent deletion even with active Snapshot CRs. The
  // Volume finalizer defers backend destruction until reconciliation sees no
  // remaining Snapshot references.
  action requestVolumeDelete = all {
    volumeExists,
    volumeExists' = volumeExists,
    volumeDeleteRequested' = true,
    volumeFinalizer' = true,
    zfsSnapshot' = zfsSnapshot,
    snapshotCR' = snapshotCR,
    snapshotFinalizer' = snapshotFinalizer,
    snapshotReady' = snapshotReady,
    snapshotDeleteRequested' = snapshotDeleteRequested,
    cloneDependsOn' = cloneDependsOn
  }

  // Parent destruction runs only after every referencing Snapshot CR is gone.
  action destroyParent = all {
    volumeExists,
    volumeDeleteRequested,
    snapshotCR == Set(),
    zfsSnapshot == Set(),
    volumeExists' = false,
    volumeDeleteRequested' = volumeDeleteRequested,
    volumeFinalizer' = false,
    zfsSnapshot' = zfsSnapshot,
    snapshotCR' = snapshotCR,
    snapshotFinalizer' = snapshotFinalizer,
    snapshotReady' = snapshotReady,
    snapshotDeleteRequested' = snapshotDeleteRequested,
    cloneDependsOn' = Set()
  }

  // Explicit snapshot deletion is blocked by a clone, unlike parent deletion.
  action requestSnapshotDelete(s: str): bool = all {
    snapshotCR.contains(s),
    volumeExists' = volumeExists,
    volumeDeleteRequested' = volumeDeleteRequested,
    volumeFinalizer' = volumeFinalizer,
    zfsSnapshot' = zfsSnapshot,
    snapshotCR' = snapshotCR,
    snapshotFinalizer' = snapshotFinalizer,
    snapshotReady' = snapshotReady,
    snapshotDeleteRequested' = snapshotDeleteRequested.union(Set(s)),
    cloneDependsOn' = cloneDependsOn
  }

  action finalizeSnapshot(s: str): bool = all {
    snapshotDeleteRequested.contains(s),
    not(cloneDependsOn.contains(s)),
    volumeExists' = volumeExists,
    volumeDeleteRequested' = volumeDeleteRequested,
    volumeFinalizer' = volumeFinalizer,
    zfsSnapshot' = zfsSnapshot.exclude(Set(s)),
    snapshotCR' = snapshotCR.exclude(Set(s)),
    snapshotFinalizer' = snapshotFinalizer.exclude(Set(s)),
    snapshotReady' = snapshotReady.exclude(Set(s)),
    snapshotDeleteRequested' = snapshotDeleteRequested.exclude(Set(s)),
    cloneDependsOn' = cloneDependsOn
  }

  action init = all {
    volumeExists' = true,
    volumeDeleteRequested' = false,
    volumeFinalizer' = true,
    zfsSnapshot' = Set(),
    snapshotCR' = Set(),
    snapshotFinalizer' = Set(),
    snapshotReady' = Set(),
    snapshotDeleteRequested' = Set(),
    cloneDependsOn' = Set()
  }

  action step = any {
    all { createSnapshot("snap-a") },
    all { requestVolumeDelete },
    all { destroyParent },
    all { requestSnapshotDelete("snap-a") },
    all { finalizeSnapshot("snap-a") }
  }

  // A Ready Snapshot CR always represents restoreable backend data.
  val readySnapshotsBacked = snapshotReady.subseteq(zfsSnapshot)
  val liveSnapshotsHaveFinalizers = snapshotCR.subseteq(snapshotFinalizer)

  run snapshotDeletionFreesParent = init
    .then(createSnapshot("snap-a"))
    .then(requestSnapshotDelete("snap-a"))
    .then(finalizeSnapshot("snap-a"))
    .then(requestVolumeDelete)
    .then(destroyParent)
    .then(all {
      assert(not(volumeExists)),
      assert(snapshotCR == Set()),
      assert(zfsSnapshot == Set()),
      volumeExists' = volumeExists,
      volumeDeleteRequested' = volumeDeleteRequested,
      volumeFinalizer' = volumeFinalizer,
      zfsSnapshot' = zfsSnapshot,
      snapshotCR' = snapshotCR,
      snapshotFinalizer' = snapshotFinalizer,
      snapshotReady' = snapshotReady,
      snapshotDeleteRequested' = snapshotDeleteRequested,
      cloneDependsOn' = cloneDependsOn
    })
}
