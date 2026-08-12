// Shared model vocabulary for zfs-csi. The finite domains keep simulation and
// bounded checking tractable while still allowing duplicate CSI requests and
// inter-agent races.
module types {
  type VolumeId = str
  type SnapshotId = str
  type NodeId = str
  type PoolId = str
  type Dataset = str
  type RequestName = str

  type VolumeKind = Filesystem | Block
  type VolumePhase = Pending | Creating | Ready | Deleting | Failed
  type SnapshotPhase = SnapshotPending | SnapshotReady | SnapshotDeleting | SnapshotFailed
  type Transport = Nfs | Nvme
  type AccessMode = Rwo | Rwx

  const BlockSize: Set[int]
  const Capacity: Set[int]

  pure def alignedCapacity(requested: int, blockSize: int): int =
    if (requested % blockSize == 0) requested
    else requested + blockSize - (requested % blockSize)

  pure def validBlockSize(blockSize: int): bool =
    BlockSize.contains(blockSize) and blockSize >= 512 and blockSize <= 131072

  pure def validCapacity(capacity: int): bool = Capacity.contains(capacity) and capacity > 0

  pure def capacityAllowed(requested: int, limit: int, kind: VolumeKind, blockSize: int): bool =
    if (kind == Filesystem) limit == 0 or requested <= limit
    else limit == 0 or alignedCapacity(requested, blockSize) <= limit

  // CSI treats the source-specific effective block size as authoritative for zvol
  // clone and restore operations. src: internal/driver/controller.go:348-371.
  pure def sourceBlockSizeAllowed(kind: VolumeKind, sourceBlockSize: int): bool =
    kind == Filesystem or validBlockSize(sourceBlockSize)

  pure def volumes: Set[VolumeId] = Set("vol-a", "vol-b")
  pure def snapshots: Set[SnapshotId] = Set("snap-a", "snap-b")
  pure def nodes: Set[NodeId] = Set("node-a", "node-b")
  pure def pools: Set[PoolId] = Set("pool-a", "pool-b")

  run alignmentRoundsUp =
    all {
      assert(alignedCapacity(1, 512) == 512),
      assert(alignedCapacity(1024, 512) == 1024)
    }

  run blockLimitChecksRoundedValue =
    all {
      assert(not(capacityAllowed(513, 1023, Block, 512))),
      assert(capacityAllowed(513, 1024, Block, 512))
    }

  run sourceBlockSizeRequiredForBlock =
    all {
      assert(not(sourceBlockSizeAllowed(Block, 0))),
      assert(sourceBlockSizeAllowed(Filesystem, 0))
    }
}
