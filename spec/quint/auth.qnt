// OpenBao projected-JWT and client-token lifecycle.
// Source: internal/crypto/openbao/openbao.go:273-432,459-486.
module auth {
  var token: str
  var generation: int
  var refreshInFlight: bool
  var leader: str
  var leaderCancelled: bool
  var requestLive: Set[str]
  var requestResponse: str -> str

  action electRefresh(r: str): bool = all {
    requestLive.contains(r),
    not(refreshInFlight),
    token != "",
    token' = token,
    generation' = generation,
    refreshInFlight' = true,
    leader' = r,
    leaderCancelled' = false,
    requestLive' = requestLive,
    requestResponse' = requestResponse
  }

  // A refresh is tied to the elected caller's context. Caller cancellation
  // ends that request, but must not evict a previously valid shared token.
  action cancelLeader = all {
    refreshInFlight,
    requestLive.contains(leader),
    token' = token,
    generation' = generation,
    refreshInFlight' = refreshInFlight,
    leader' = leader,
    leaderCancelled' = true,
    requestLive' = requestLive.exclude(Set(leader)),
    requestResponse' = requestResponse
  }

  action cancelledRefresh = all {
    refreshInFlight,
    leaderCancelled,
    token' = token,
    generation' = generation,
    refreshInFlight' = false,
    leader' = "",
    leaderCancelled' = true,
    requestLive' = requestLive,
    requestResponse' = requestResponse.set(leader, "cancelled")
  }

  action successfulRefresh = all {
    refreshInFlight,
    not(leaderCancelled),
    token' = token,
    generation' = generation,
    refreshInFlight' = false,
    leader' = "",
    leaderCancelled' = false,
    requestLive' = requestLive,
    requestResponse' = requestResponse.set(leader, "refreshed")
  }

  action init = all {
    token' = "token-a",
    generation' = 0,
    refreshInFlight' = false,
    leader' = "",
    leaderCancelled' = false,
    requestLive' = Set("request-a", "request-b"),
    requestResponse' = Set("request-a", "request-b").mapBy(r => "none")
  }

  action step = any {
    all { electRefresh("request-a") },
    all { electRefresh("request-b") },
    all { cancelLeader },
    all { cancelledRefresh },
    all { successfulRefresh }
  }

  // Cancellation of one caller cannot evict still-valid shared credentials.
  val cancellationDoesNotEvictSharedToken = not(leaderCancelled) or token != ""

  run cancelledLeaderKeepsSharedToken = init
    .then(electRefresh("request-a"))
    .then(cancelLeader)
    .then(cancelledRefresh)
    .then(all {
      assert(token == "token-a"),
      assert(generation == 0),
      token' = token,
      generation' = generation,
      refreshInFlight' = refreshInFlight,
      leader' = leader,
      leaderCancelled' = leaderCancelled,
      requestLive' = requestLive,
      requestResponse' = requestResponse
    })
}
