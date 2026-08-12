// NFS export and NVMe-oF access-control model.
// Source: internal/nfsexport/channels.go:81-166, table.go:263-317,
// server.go:277-328; internal/nvmet/export_reconciler.go:62-170.
module transport {
  type ExportState = NotExported | Exported | Withdrawn
  type KernelCache = NoCache | Positive | Negative

  var exportState: ExportState
  var clientAuthorized: bool
  var rootSquash: bool
  var kernelCache: KernelCache
  var cacheEpoch: int
  var policyEpoch: int
  var daemonAlive: bool
  var exportPathExists: bool

  action registerExport = all {
    exportPathExists,
    exportState' = Exported,
    clientAuthorized' = clientAuthorized,
    rootSquash' = rootSquash,
    kernelCache' = kernelCache,
    cacheEpoch' = cacheEpoch,
    policyEpoch' = policyEpoch,
    daemonAlive' = daemonAlive,
    exportPathExists' = exportPathExists
  }

  // A successful auth upcall writes a kernel positive cache entry (TTL 30 min).
  action authorizeAndCache = all {
    daemonAlive,
    exportState == Exported,
    clientAuthorized,
    exportState' = exportState,
    clientAuthorized' = clientAuthorized,
    rootSquash' = rootSquash,
    kernelCache' = Positive,
    cacheEpoch' = policyEpoch,
    policyEpoch' = policyEpoch,
    daemonAlive' = daemonAlive,
    exportPathExists' = exportPathExists
  }

  // The only admission-permitted tightening is root-squash. It changes export
  // flags, not the client's CIDR authorization, so an old cache does not grant
  // a newly revoked client.
  action tightenRootSquashWithFailedFlush = all {
    exportState == Exported,
    not(rootSquash),
    exportState' = exportState,
    clientAuthorized' = clientAuthorized,
    rootSquash' = true,
    kernelCache' = kernelCache,
    cacheEpoch' = cacheEpoch,
    policyEpoch' = policyEpoch + 1,
    daemonAlive' = daemonAlive,
    exportPathExists' = exportPathExists
  }

  action cacheExpires = all {
    kernelCache == Positive,
    exportState' = exportState,
    clientAuthorized' = clientAuthorized,
    rootSquash' = rootSquash,
    kernelCache' = NoCache,
    cacheEpoch' = cacheEpoch,
    policyEpoch' = policyEpoch,
    daemonAlive' = daemonAlive,
    exportPathExists' = exportPathExists
  }

  action daemonCrash = all {
    daemonAlive,
    exportState' = exportState,
    clientAuthorized' = clientAuthorized,
    rootSquash' = rootSquash,
    kernelCache' = kernelCache,
    cacheEpoch' = cacheEpoch,
    policyEpoch' = policyEpoch,
    daemonAlive' = false,
    exportPathExists' = exportPathExists
  }

  action init = all {
    exportState' = NotExported,
    clientAuthorized' = true,
    rootSquash' = false,
    kernelCache' = NoCache,
    cacheEpoch' = 0,
    policyEpoch' = 0,
    daemonAlive' = true,
    exportPathExists' = true
  }

  action step = any {
    all { registerExport },
    all { authorizeAndCache },
    all { tightenRootSquashWithFailedFlush },
    all { cacheExpires },
    all { daemonCrash }
  }

  // A failed root-squash flush can retain old flags, never grant a client that
  // the desired CIDR policy has revoked.
  val clientAuthorizationStable = clientAuthorized

  run failedFlushRetainsOnlyRootFlags = init
    .then(registerExport)
    .then(authorizeAndCache)
    .then(tightenRootSquashWithFailedFlush)
    .then(all {
      assert(clientAuthorized),
      assert(rootSquash),
      assert(kernelCache == Positive),
      assert(cacheEpoch < policyEpoch),
      exportState' = exportState,
      clientAuthorized' = clientAuthorized,
      rootSquash' = rootSquash,
      kernelCache' = kernelCache,
      cacheEpoch' = cacheEpoch,
      policyEpoch' = policyEpoch,
      daemonAlive' = daemonAlive,
      exportPathExists' = exportPathExists
    })
}
