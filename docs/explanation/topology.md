# Topology and Scheduling

This document explains why zfs-csi models consumer reachability as a network domain rather than
storage-host affinity, and how that model interacts with Kubernetes scheduling.

## Storage Is Remote, but Not Necessarily Reachable Everywhere

Consumer pods access storage over NFS or NVMe-TCP. They do not need to run on the host that owns
the ZFS pool, so storage-owner identity is not a CSI topology segment. They do need a network
path to the owner's published protocol endpoints.

zfs-csi represents that path with one topology segment:

```text
topology.zfs.csi.randomvariable.co.uk/network-domain
```

The CSI node plugin reports its consumer node's configured domain. Storage-agent inventory
publishes which domains can reach each logical owner. Placement intersects those facts rather
than pinning applications to storage hosts.

## Accessibility Requirements

`CreateVolume` treats CSI accessibility requirements as constraints on storage placement:

- `requisite` is mandatory;
- `preferred` ranks candidates already allowed by `requisite`;
- an empty requirement allows deterministic selection from all fresh reachable inventory;
- no reachable candidate returns CSI `ResourceExhausted`.

The selected domain is persisted in `Volume.spec.networkDomain`. Clone and restore requests stay
with their source owner and pool and must remain reachable under the request.

## Current Deployment Limitation

!!! warning "Pending runtime rollout"

    `CreateVolumeResponse` returns the persisted domain in `AccessibleTopology`, and
    `NodeGetInfo` publishes the same network-domain key. The remaining limitation is deployment:
    the current Helm chart does not expose distinct per-consumer domains or a multi-owner
    rollout. Do not treat a hand-built multi-agent configuration as production-supported until
    the final chart and two-storage-node IPv4/IPv6 fault E2E gate are complete.

Consumer node labels use the topology key and stable Kubernetes label values, for example:

```bash
kubectl label node worker-a \
  topology.zfs.csi.randomvariable.co.uk/network-domain=storage-east
```

Code-level node topology is supplied with `--network-domain` (or its `NETWORK_DOMAIN`
environment default). The stock chart does not yet wire distinct values per consumer domain.

## Volume Binding Modes

The bundled StorageClasses retain their existing binding modes:

- block uses `WaitForFirstConsumer`, allowing the provisioner to receive consumer accessibility
  requirements before choosing reachable storage;
- filesystem uses `Immediate`, so creation may have empty accessibility requirements and select
  a deterministic reachable domain from inventory.

Once selected, the domain, owner, and pool GUID remain immutable on the volume. The owning agent
may refresh valid endpoint status for future publish and stage operations, but active mounts and
attachments do not migrate automatically. No transparent owner/domain failover exists.

## Further Reading

- [Multi-Storage-Agent Topology and Placement](multi-storage-agent-topology.md) (explanation)
- [Operate Storage Topology Safely](../how-to/operate-storage-topology.md) (how-to)
- [Storage Model](storage-model.md) (explanation)
- [StorageClass Reference](../reference/storage-classes.md) (reference)

---

**Last Updated:** July 2026
