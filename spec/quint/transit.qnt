// Transit key deletion against the configured active OpenBao endpoint.
// Source: internal/crypto/openbao/openbao.go:172-202.
module transit {
  type Key = Absent | Present | DeletionAllowed

  var active: Key
  var result: str

  action configureActive = all {
    active == Present,
    active' = DeletionAllowed,
    result' = "configured"
  }

  action deleteActive = all {
    active == DeletionAllowed,
    active' = Absent,
    result' = "success"
  }

  action stutter = all {
    active' = active,
    result' = result
  }

  action init = all {
    active' = Present,
    result' = "none"
  }

  action step = any {
    all { configureActive }, all { deleteActive }, all { stutter }
  }

  val successfulDeleteMeansNoActiveKey = result != "success" or active == Absent

  run configuredEndpointDeletesKey = init
    .then(configureActive)
    .then(deleteActive)
    .then(all {
      assert(result == "success"),
      assert(active == Absent),
      active' = active,
      result' = result
    })
}
