// CSI node staging/publishing model.
// Source: internal/driver/node.go:154-377,402-458;
// internal/stage/server.go:122-237,366-429;
// internal/mount/impl/mount.go:112-325.
module node {
  type Stage = NoStage | DirectoryOnly | IdentityWritten | Attached | Staged
  type Target = NoTarget | Published

  var stage: Stage
  var target: Target
  var nvmeAttached: bool
  var identity: bool
  var identityValid: bool
  var deviceReadOnly: bool
  var requestedReadOnly: bool
  var stageFailure: bool

  action makeStageDirectory = all {
    stage == NoStage,
    stage' = DirectoryOnly,
    target' = target,
    nvmeAttached' = nvmeAttached,
    identity' = identity,
    identityValid' = identityValid,
    deviceReadOnly' = deviceReadOnly,
    requestedReadOnly' = requestedReadOnly,
    stageFailure' = stageFailure
  }

  // Block identity is persisted before the attach. src: node.go:258-270.
  action writeIdentity = all {
    stage == DirectoryOnly,
    stage' = IdentityWritten,
    target' = target,
    nvmeAttached' = nvmeAttached,
    identity' = true,
    identityValid' = true,
    deviceReadOnly' = deviceReadOnly,
    requestedReadOnly' = requestedReadOnly,
    stageFailure' = stageFailure
  }

  action attach = all {
    stage == IdentityWritten,
    stage' = Attached,
    target' = target,
    nvmeAttached' = true,
    identity' = identity,
    identityValid' = identityValid,
    deviceReadOnly' = deviceReadOnly,
    requestedReadOnly' = requestedReadOnly,
    stageFailure' = stageFailure
  }

  action mountStage = all {
    stage == Attached,
    not(stageFailure),
    stage' = Staged,
    target' = target,
    nvmeAttached' = nvmeAttached,
    identity' = identity,
    identityValid' = identityValid,
    deviceReadOnly' = deviceReadOnly,
    requestedReadOnly' = requestedReadOnly,
    stageFailure' = stageFailure
  }

  // The real error path returns without detach after any post-attach failure.
  action failAfterAttach = all {
    stage == Attached,
    stageFailure,
    stage' = Attached,
    target' = target,
    nvmeAttached' = nvmeAttached,
    identity' = identity,
    identityValid' = identityValid,
    deviceReadOnly' = deviceReadOnly,
    requestedReadOnly' = requestedReadOnly,
    stageFailure' = stageFailure
  }

  action publish = all {
    stage == Staged,
    target == NoTarget,
    stage' = stage,
    target' = Published,
    nvmeAttached' = nvmeAttached,
    identity' = identity,
    identityValid' = identityValid,
    deviceReadOnly' = (deviceReadOnly or requestedReadOnly),
    requestedReadOnly' = requestedReadOnly,
    stageFailure' = stageFailure
  }

  action unpublish = all {
    target == Published,
    stage' = stage,
    target' = NoTarget,
    nvmeAttached' = nvmeAttached,
    identity' = identity,
    identityValid' = identityValid,
    deviceReadOnly' = deviceReadOnly,
    requestedReadOnly' = requestedReadOnly,
    stageFailure' = stageFailure
  }

  // The atomically written identity remains available for teardown after a
  // post-attach stage failure.
  action unstage = all {
    target == NoTarget,
    identity,
    identityValid,
    stage' = NoStage,
    target' = target,
    nvmeAttached' = false,
    identity' = false,
    identityValid' = false,
    deviceReadOnly' = deviceReadOnly,
    requestedReadOnly' = requestedReadOnly,
    stageFailure' = stageFailure
  }

  action init = all {
    stage' = NoStage,
    target' = NoTarget,
    nvmeAttached' = false,
    identity' = false,
    identityValid' = false,
    deviceReadOnly' = false,
    requestedReadOnly' = true,
    stageFailure' = true
  }

  action step = any {
    all { makeStageDirectory },
    all { writeIdentity },
    all { attach },
    all { mountStage },
    all { failAfterAttach },
    all { publish },
    all { unpublish },
    all { unstage }
  }

  val attachedNodeHasCleanupIdentity = not(nvmeAttached) or (identity and identityValid)
  val publicationRequiresStage = target == NoTarget or stage == Staged

  run postAttachFailureCanUnstage = init
    .then(makeStageDirectory)
    .then(writeIdentity)
    .then(attach)
    .then(failAfterAttach)
    .then(unstage)
    .then(all {
      assert(not(nvmeAttached)),
      assert(not(identity)),
      stage' = stage,
      target' = target,
      nvmeAttached' = nvmeAttached,
      identity' = identity,
      identityValid' = identityValid,
      deviceReadOnly' = deviceReadOnly,
      requestedReadOnly' = requestedReadOnly,
      stageFailure' = stageFailure
    })
}
