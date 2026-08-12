// Volume CR reconciliation and finalizer lifecycle.
// Source: internal/driver/controller.go:706-740,889-911;
// internal/agent/volume_reconciler.go:950-1255,1563-1630,2225-2425.
module agent {
  type Phase = Pending | Ready | Deleting | Destroyed | Error
  type Backend = Absent | Dataset | DatasetExported

  var crPresent: bool
  var finalizer: bool
  var phase: Phase
  var backend: Backend
  var ownerMatches: bool
  var poolGUIDMatches: bool
  var mapped: bool
  var createSideEffectFails: bool
  var deletionPolicyRetain: bool
  var processAlive: bool

  // Controller atomically publishes immutable intent and its finalizer before
  // any agent may create a ZFS dataset. src: controller.go:706-740.
  action createIntent = all {
    not(crPresent),
    crPresent' = true,
    finalizer' = true,
    phase' = Pending,
    backend' = Absent,
    ownerMatches' = ownerMatches,
    poolGUIDMatches' = poolGUIDMatches,
    mapped' = mapped,
    createSideEffectFails' = createSideEffectFails,
    deletionPolicyRetain' = deletionPolicyRetain,
    processAlive' = processAlive
  }

  action materializeDataset = all {
    crPresent, finalizer, phase == Pending, ownerMatches, poolGUIDMatches,
    backend == Absent,
    crPresent' = crPresent,
    finalizer' = finalizer,
    phase' = Pending,
    backend' = Dataset,
    ownerMatches' = ownerMatches,
    poolGUIDMatches' = poolGUIDMatches,
    mapped' = mapped,
    createSideEffectFails' = createSideEffectFails,
    deletionPolicyRetain' = deletionPolicyRetain,
    processAlive' = processAlive
  }

  action configureExport = all {
    crPresent, finalizer, phase == Pending, backend == Dataset,
    not(createSideEffectFails),
    crPresent' = crPresent,
    finalizer' = finalizer,
    phase' = Ready,
    backend' = DatasetExported,
    ownerMatches' = ownerMatches,
    poolGUIDMatches' = poolGUIDMatches,
    mapped' = mapped,
    createSideEffectFails' = createSideEffectFails,
    deletionPolicyRetain' = deletionPolicyRetain,
    processAlive' = processAlive
  }

  // Re-entry after a crash sees the dataset and continues export/status work.
  action recoverAfterCrash = all {
    crPresent, finalizer, phase == Pending, backend == Dataset,
    processAlive,
    crPresent' = crPresent,
    finalizer' = finalizer,
    phase' = Pending,
    backend' = backend,
    ownerMatches' = ownerMatches,
    poolGUIDMatches' = poolGUIDMatches,
    mapped' = mapped,
    createSideEffectFails' = false,
    deletionPolicyRetain' = deletionPolicyRetain,
    processAlive' = processAlive
  }

  action requestDelete = all {
    crPresent,
    crPresent' = crPresent,
    finalizer' = finalizer,
    phase' = Deleting,
    backend' = backend,
    ownerMatches' = ownerMatches,
    poolGUIDMatches' = poolGUIDMatches,
    mapped' = mapped,
    createSideEffectFails' = createSideEffectFails,
    deletionPolicyRetain' = deletionPolicyRetain,
    processAlive' = processAlive
  }

  // Mapped initiators are a deletion guard: the reconciler leaves finalizer and
  // backend untouched until an external unpublish event changes mapping.
  action deleteBlocked = all {
    phase == Deleting, mapped,
    crPresent' = crPresent,
    finalizer' = finalizer,
    phase' = phase,
    backend' = backend,
    ownerMatches' = ownerMatches,
    poolGUIDMatches' = poolGUIDMatches,
    mapped' = mapped,
    createSideEffectFails' = createSideEffectFails,
    deletionPolicyRetain' = deletionPolicyRetain,
    processAlive' = processAlive
  }

  action finishDelete = all {
    phase == Deleting, not(mapped),
    crPresent' = false,
    finalizer' = false,
    phase' = Destroyed,
    backend' = if (deletionPolicyRetain) Dataset else Absent,
    ownerMatches' = ownerMatches,
    poolGUIDMatches' = poolGUIDMatches,
    mapped' = mapped,
    createSideEffectFails' = createSideEffectFails,
    deletionPolicyRetain' = deletionPolicyRetain,
    processAlive' = processAlive
  }

  action crash = all {
    processAlive,
    crPresent' = crPresent,
    finalizer' = finalizer,
    phase' = phase,
    backend' = backend,
    ownerMatches' = ownerMatches,
    poolGUIDMatches' = poolGUIDMatches,
    mapped' = mapped,
    createSideEffectFails' = createSideEffectFails,
    deletionPolicyRetain' = deletionPolicyRetain,
    processAlive' = false
  }

  action restart = all {
    not(processAlive),
    crPresent' = crPresent,
    finalizer' = finalizer,
    phase' = phase,
    backend' = backend,
    ownerMatches' = ownerMatches,
    poolGUIDMatches' = poolGUIDMatches,
    mapped' = mapped,
    createSideEffectFails' = createSideEffectFails,
    deletionPolicyRetain' = deletionPolicyRetain,
    processAlive' = true
  }

  action init = all {
    crPresent' = false,
    finalizer' = false,
    phase' = Pending,
    backend' = Absent,
    ownerMatches' = true,
    poolGUIDMatches' = true,
    mapped' = false,
    createSideEffectFails' = false,
    deletionPolicyRetain' = false,
    processAlive' = true
  }

  action step = any {
    all { createIntent }, all { materializeDataset }, all { configureExport },
    all { recoverAfterCrash }, all { requestDelete }, all { deleteBlocked },
    all { finishDelete }, all { crash }, all { restart }
  }

  val liveDatasetHasFinalizer = backend != Absent and crPresent implies finalizer
  val readyImpliesExported = phase != Ready or backend == DatasetExported
  val deleteOnlyDropsDynamicBackend = phase != Destroyed or deletionPolicyRetain or backend == Absent

  run createCrashRecovery = init
    .then(createIntent)
    .then(materializeDataset)
    .then(crash)
    .then(restart)
    .then(recoverAfterCrash)
    .then(configureExport)
    .then(all {
      assert(phase == Ready),
      assert(backend == DatasetExported),
      assert(finalizer),
      crPresent' = crPresent,
      finalizer' = finalizer,
      phase' = phase,
      backend' = backend,
      ownerMatches' = ownerMatches,
      poolGUIDMatches' = poolGUIDMatches,
      mapped' = mapped,
      createSideEffectFails' = createSideEffectFails,
      deletionPolicyRetain' = deletionPolicyRetain,
      processAlive' = processAlive
    })
}
