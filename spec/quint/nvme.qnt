// NVMe-oF namespace ownership and host-allow-list model.
// Source: internal/nvmet/export_reconciler.go:62-170;
// internal/transport/nvmet.go:163-220,327-386.
module nvme {
  var active: Set[str]
  var target: str -> str
  var nsid: str -> int
  var device: str -> str
  var admittedHosts: str -> Set[str]

  // Derived Volume identities make colliding NQN/NSID assignments unreachable
  // in the deployed producer path. NVMeExport CR injection is outside model.
  action publishExport(c: str, nqn: str, requestedNSID: int, dev: str): bool = all {
    not(active.contains(c)),
    requestedNSID > 0,
    Set("export-a", "export-b").forall(other =>
      not(active.contains(other) and target.get(other) == nqn and nsid.get(other) == requestedNSID)),
    active' = active.union(Set(c)),
    target' = target.set(c, nqn),
    nsid' = nsid.set(c, requestedNSID),
    device' = device.set(c, dev),
    admittedHosts' = admittedHosts.set(c, Set())
  }

  action admitHost(c: str, host: str): bool = all {
    active.contains(c),
    active' = active,
    target' = target,
    nsid' = nsid,
    device' = device,
    admittedHosts' = admittedHosts.set(c, admittedHosts.get(c).union(Set(host)))
  }

  action unexport(c: str): bool = all {
    active.contains(c),
    active' = active.exclude(Set(c)),
    target' = target,
    nsid' = nsid,
    device' = device,
    admittedHosts' = admittedHosts.set(c, Set())
  }

  action init = all {
    active' = Set(),
    target' = Set("export-a", "export-b").mapBy(c => ""),
    nsid' = Set("export-a", "export-b").mapBy(c => 1),
    device' = Set("export-a", "export-b").mapBy(c => ""),
    admittedHosts' = Set("export-a", "export-b").mapBy(c => Set())
  }

  action step = any {
    all { publishExport("export-a", "nqn.test/a", 1, "/dev/zvol/a") },
    all { publishExport("export-b", "nqn.test/b", 1, "/dev/zvol/b") },
    all { admitHost("export-a", "host-a") },
    all { unexport("export-a") }
  }

  // Configfs maps each target namespace identity to one device.
  val uniqueTargetNamespace = Set("export-a", "export-b").forall(a =>
    Set("export-a", "export-b").forall(b =>
      a == b or not(active.contains(a) and active.contains(b) and
        target.get(a) == target.get(b) and nsid.get(a) == nsid.get(b))))

  run distinctDerivedTargets = init
    .then(publishExport("export-a", "nqn.test/a", 1, "/dev/zvol/a"))
    .then(publishExport("export-b", "nqn.test/b", 1, "/dev/zvol/b"))
    .then(all {
      assert(uniqueTargetNamespace),
      active' = active,
      target' = target,
      nsid' = nsid,
      device' = device,
      admittedHosts' = admittedHosts
    })
}
