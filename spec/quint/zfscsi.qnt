// Whole-system bounded model linking persistent CSI intent, ZFS snapshots,
// kernel export authorization, node staging, and OpenBao key cleanup.
// Individual modules retain implementation-specific models and witnesses.
module zfscsi {
  type Volume = NoVolume | Pending | Ready | Deleting | Destroyed
  type Snapshot = NoSnapshot | SnapshotReady
  type Auth = TokenValid | TokenMissing
  type Cache = NoCache | PositiveCache

  var volume: Volume
  var volumeFinalizer: bool
  var dataset: bool
  var snapshot: Snapshot
  var zfsSnapshot: bool
  var clientAuthorized: bool
  var kernelCache: Cache
  var auth: Auth
  var transitKey: bool
  var nodeAttached: bool
  var nodeIdentity: bool
  var agentAlive: bool

  action createVolume = all {
    volume == NoVolume,
    volume' = Pending,
    volumeFinalizer' = true,
    dataset' = false,
    snapshot' = snapshot,
    zfsSnapshot' = zfsSnapshot,
    clientAuthorized' = clientAuthorized,
    kernelCache' = kernelCache,
    auth' = auth,
    transitKey' = true,
    nodeAttached' = nodeAttached,
    nodeIdentity' = nodeIdentity,
    agentAlive' = agentAlive
  }

  action materialize = all {
    volume == Pending,
    volume' = Ready,
    volumeFinalizer' = volumeFinalizer,
    dataset' = true,
    snapshot' = snapshot,
    zfsSnapshot' = zfsSnapshot,
    clientAuthorized' = clientAuthorized,
    kernelCache' = kernelCache,
    auth' = auth,
    transitKey' = transitKey,
    nodeAttached' = nodeAttached,
    nodeIdentity' = nodeIdentity,
    agentAlive' = agentAlive
  }

  action createSnapshot = all {
    volume == Ready,
    dataset,
    snapshot == NoSnapshot,
    volume' = volume,
    volumeFinalizer' = volumeFinalizer,
    dataset' = dataset,
    snapshot' = SnapshotReady,
    zfsSnapshot' = true,
    clientAuthorized' = clientAuthorized,
    kernelCache' = kernelCache,
    auth' = auth,
    transitKey' = transitKey,
    nodeAttached' = nodeAttached,
    nodeIdentity' = nodeIdentity,
    agentAlive' = agentAlive
  }

  action cacheAuthorization = all {
    volume == Ready,
    clientAuthorized,
    volume' = volume,
    volumeFinalizer' = volumeFinalizer,
    dataset' = dataset,
    snapshot' = snapshot,
    zfsSnapshot' = zfsSnapshot,
    clientAuthorized' = clientAuthorized,
    kernelCache' = PositiveCache,
    auth' = auth,
    transitKey' = transitKey,
    nodeAttached' = nodeAttached,
    nodeIdentity' = nodeIdentity,
    agentAlive' = agentAlive
  }


  // CSI DeleteVolume accepts the request while a Snapshot CR exists. The Volume
  // finalizer retains the backend; the agent only destroys it after Snapshot
  // finalization removes the final reference.
  action requestParentDelete = all {
    volume == Ready,
    volume' = Deleting,
    volumeFinalizer' = true,
    dataset' = dataset,
    snapshot' = snapshot,
    zfsSnapshot' = zfsSnapshot,
    clientAuthorized' = clientAuthorized,
    kernelCache' = kernelCache,
    auth' = auth,
    transitKey' = transitKey,
    nodeAttached' = nodeAttached,
    nodeIdentity' = nodeIdentity,
    agentAlive' = agentAlive
  }

  action finishParentDelete = all {
    volume == Deleting,
    snapshot == NoSnapshot,
    volume' = Destroyed,
    volumeFinalizer' = false,
    dataset' = false,
    snapshot' = snapshot,
    zfsSnapshot' = zfsSnapshot,
    clientAuthorized' = clientAuthorized,
    kernelCache' = kernelCache,
    auth' = auth,
    transitKey' = transitKey,
    nodeAttached' = nodeAttached,
    nodeIdentity' = nodeIdentity,
    agentAlive' = agentAlive
  }

  action deleteSnapshot = all {
    snapshot == SnapshotReady,
    volume' = volume,
    volumeFinalizer' = volumeFinalizer,
    dataset' = dataset,
    snapshot' = NoSnapshot,
    zfsSnapshot' = false,
    clientAuthorized' = clientAuthorized,
    kernelCache' = kernelCache,
    auth' = auth,
    transitKey' = transitKey,
    nodeAttached' = nodeAttached,
    nodeIdentity' = nodeIdentity,
    agentAlive' = agentAlive
  }

  action nodeAttach = all {
    volume == Ready,
    volume' = volume,
    volumeFinalizer' = volumeFinalizer,
    dataset' = dataset,
    snapshot' = snapshot,
    zfsSnapshot' = zfsSnapshot,
    clientAuthorized' = clientAuthorized,
    kernelCache' = kernelCache,
    auth' = auth,
    transitKey' = transitKey,
    nodeAttached' = true,
    nodeIdentity' = true,
    agentAlive' = agentAlive
  }

  action failNodeStage = all {
    nodeAttached,
    volume' = volume,
    volumeFinalizer' = volumeFinalizer,
    dataset' = dataset,
    snapshot' = snapshot,
    zfsSnapshot' = zfsSnapshot,
    clientAuthorized' = clientAuthorized,
    kernelCache' = kernelCache,
    auth' = auth,
    transitKey' = transitKey,
    nodeAttached' = nodeAttached,
    nodeIdentity' = nodeIdentity,
    agentAlive' = agentAlive
  }

  action cancelAuthRefresh = all {
    auth == TokenValid,
    volume' = volume,
    volumeFinalizer' = volumeFinalizer,
    dataset' = dataset,
    snapshot' = snapshot,
    zfsSnapshot' = zfsSnapshot,
    clientAuthorized' = clientAuthorized,
    kernelCache' = kernelCache,
    auth' = auth,
    transitKey' = transitKey,
    nodeAttached' = nodeAttached,
    nodeIdentity' = nodeIdentity,
    agentAlive' = agentAlive
  }

  action stutter = all {
    volume' = volume,
    volumeFinalizer' = volumeFinalizer,
    dataset' = dataset,
    snapshot' = snapshot,
    zfsSnapshot' = zfsSnapshot,
    clientAuthorized' = clientAuthorized,
    kernelCache' = kernelCache,
    auth' = auth,
    transitKey' = transitKey,
    nodeAttached' = nodeAttached,
    nodeIdentity' = nodeIdentity,
    agentAlive' = agentAlive
  }

  action init = all {
    volume' = NoVolume,
    volumeFinalizer' = false,
    dataset' = false,
    snapshot' = NoSnapshot,
    zfsSnapshot' = false,
    clientAuthorized' = true,
    kernelCache' = NoCache,
    auth' = TokenValid,
    transitKey' = false,
    nodeAttached' = false,
    nodeIdentity' = false,
    agentAlive' = true
  }

  action step = any {
    all { createVolume }, all { materialize }, all { createSnapshot },
    all { cacheAuthorization }, all { requestParentDelete }, all { deleteSnapshot },
    all { finishParentDelete }, all { nodeAttach }, all { failNodeStage },
    all { cancelAuthRefresh }, all { stutter }
  }

  val readySnapshotHasBackend = snapshot != SnapshotReady or zfsSnapshot
  val attachedNodeHasIdentity = not(nodeAttached) or nodeIdentity
  val liveDatasetFinalized = not(dataset) or volumeFinalizer

  run snapshotDeletionAllowsParentCleanup = init
    .then(createVolume)
    .then(materialize)
    .then(createSnapshot)
    .then(requestParentDelete)
    .then(deleteSnapshot)
    .then(finishParentDelete)
    .then(all {
      assert(volume == Destroyed),
      assert(snapshot == NoSnapshot),
      assert(not(zfsSnapshot)),
      volume' = volume,
      volumeFinalizer' = volumeFinalizer,
      dataset' = dataset,
      snapshot' = snapshot,
      zfsSnapshot' = zfsSnapshot,
      clientAuthorized' = clientAuthorized,
      kernelCache' = kernelCache,
      auth' = auth,
      transitKey' = transitKey,
      nodeAttached' = nodeAttached,
      nodeIdentity' = nodeIdentity,
      agentAlive' = agentAlive
    })
}
