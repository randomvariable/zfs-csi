// CSI controller model. src: internal/driver/controller.go:485-765,846-912,1223-1323,1493-1531.
module controller {
  type Kind = Filesystem | Block
  type Phase = Pending | Ready | Deleting | Destroyed
  type Source = NoSource | VolumeSource | SnapshotSource

  var live: Set[str]
  var phase: str -> Phase
  var kind: str -> Kind
  var capacity: str -> int
  var blockSize: str -> int
  var source: str -> Source
  var finalizer: Set[str]
  var response: str -> str

  pure def alignUp(requested: int, unit: int): int =
    if (requested % unit == 0) requested else requested + unit - requested % unit

  pure def requestAllowed(kind: Kind, requested: int, limit: int, unit: int): bool =
    requested > 0 and unit > 0 and
    (limit == 0 or (if (kind == Block) alignUp(requested, unit) else requested) <= limit)

  action create(v: str, request: str, requestedKind: Kind, requested: int, limit: int, unit: int, requestedSource: Source): bool = all {
    not(live.contains(v)),
    requestAllowed(requestedKind, requested, limit, unit),
    live' = live.union(Set(v)),
    phase' = phase.set(v, Pending),
    kind' = kind.set(v, requestedKind),
    capacity' = capacity.set(v, if (requestedKind == Block) alignUp(requested, unit) else requested),
    blockSize' = blockSize.set(v, unit),
    source' = source.set(v, requestedSource),
    finalizer' = finalizer.union(Set(v)),
    response' = response.set(request, "created")
  }

  action duplicate(v: str, request: str, requestedKind: Kind, requested: int, unit: int, requestedSource: Source): bool = all {
    live.contains(v),
    kind.get(v) == requestedKind,
    capacity.get(v) == (if (requestedKind == Block) alignUp(requested, unit) else requested),
    source.get(v) == requestedSource,
    live' = live,
    phase' = phase,
    kind' = kind,
    capacity' = capacity,
    blockSize' = blockSize,
    source' = source,
    finalizer' = finalizer,
    response' = response.set(request, "existing")
  }

  action materialize(v: str): bool = all {
    live.contains(v), phase.get(v) == Pending,
    live' = live,
    phase' = phase.set(v, Ready),
    kind' = kind, capacity' = capacity, blockSize' = blockSize, source' = source,
    finalizer' = finalizer, response' = response
  }

  action deleteRequest(v: str): bool = all {
    live.contains(v),
    live' = live,
    phase' = phase.set(v, Deleting),
    kind' = kind, capacity' = capacity, blockSize' = blockSize, source' = source,
    finalizer' = finalizer, response' = response
  }

  action finalizeDelete(v: str): bool = all {
    phase.get(v) == Deleting,
    live' = live.exclude(Set(v)),
    phase' = phase.set(v, Destroyed),
    kind' = kind, capacity' = capacity, blockSize' = blockSize, source' = source,
    finalizer' = finalizer.exclude(Set(v)), response' = response
  }

  action init = all {
    live' = Set(),
    phase' = Set("vol-a").mapBy(v => Pending),
    kind' = Set("vol-a").mapBy(v => Filesystem),
    capacity' = Set("vol-a").mapBy(v => 0),
    blockSize' = Set("vol-a").mapBy(v => 512),
    source' = Set("vol-a").mapBy(v => NoSource),
    finalizer' = Set(),
    response' = Set("request-a", "request-b").mapBy(r => "none")
  }

  action step = any {
    all { create("vol-a", "request-a", Block, 513, 1024, 512, NoSource) },
    all { duplicate("vol-a", "request-b", Block, 513, 512, NoSource) },
    all { materialize("vol-a") },
    all { deleteRequest("vol-a") },
    all { finalizeDelete("vol-a") }
  }

  val blockCapacityAligned = Set("vol-a").forall(v =>
    live.contains(v).implies(kind.get(v) != Block or capacity.get(v) % blockSize.get(v) == 0))
  val liveVolumesHaveFinalizers = Set("vol-a").forall(v => live.contains(v).implies(finalizer.contains(v)))

  run blockLimitUsesRoundedCapacity = all {
    assert(not(requestAllowed(Block, 513, 1023, 512))),
    assert(requestAllowed(Block, 513, 1024, 512))
  }
}
