# E2E static lane (pre-existing cluster) — prerequisites

The static E2E lane runs the same helm-install → smoke → conformance flow as
the KubeVirt and AWS lanes, but against a **pre-existing** Kubernetes cluster
reached via its own kubeconfig. Nothing is provisioned: there is no CAPI
management cluster, no clusterctl template, and — critically — **no cluster
teardown**. It exists for validating the driver on real shared hardware (for
example a home or lab cluster) without giving the harness any authority over
that cluster's lifecycle.

Two properties are non-negotiable and enforced in code:

1. **No infrastructure identity in the repository.** Node names, IPs, pool
   device identities, registry references, and taint inventories all arrive at
   runtime from a **gitignored** `InfrastructureConfig` and environment
   variables. `mage e2e:static` fails closed if they are missing; there are no
   in-repo defaults.
2. **Cleanup can only ever touch the harness's own footprint.** See the
   cleanup-safety contract below.

## Prerequisites (external to this repo)

- An existing Kubernetes cluster, **v1.36 or newer**, with the feature gates
  the driver relies on enabled (the project targets alpha/beta APIs by policy;
  in particular `PodCertificateRequest` on the API server, controller manager,
  and kubelets when transport TLS is exercised, and the stable
  `VolumeAttributesClass` API served).
- A dedicated **ZFS storage node** in that cluster: ZFS installed, the target
  pool imported and healthy, and the pool device identity known. The node
  should carry a `NoSchedule` taint so ordinary workloads avoid it; list that
  taint (and any other site taints) in `E2E_NON_BLOCKING_TAINTS`.
- Required kernel modules available on the storage node for the transports
  under test: `zfs`, `nvmet`, `nvmet_tcp`, `nvme_fabrics`, `nvme_tcp` for
  NVMe-TCP; an NFS server (`nfsd`) for NFS; the kernel handshake/TLS stack
  (`tls`, `CONFIG_NET_HANDSHAKE`, a compatible `tlshd`) when transport TLS is
  enabled.
- A **container registry reachable from every participating node**, holding
  the driver image referenced by `E2E_DRIVER_IMAGE`.
- At least two schedulable consumer nodes (for RWX cross-node smokes and the
  multi-node conformance specs) whose network is covered by the NFS export
  CIDRs in the infrastructure configuration.
- A Linux workstation with Docker for the conformance container (`host`
  network mode is Linux-only; unset `HTTP(S)_PROXY` while it runs).

## Quick start

```bash
# All values below come from your gitignored configuration — never the repo.
export E2E_RUN_ID=static-dev
export E2E_INFRASTRUCTURE_CONFIG=/path/to/gitignored/infrastructure-static.yaml
export E2E_WORKLOAD_KUBECONFIG=/path/to/cluster.kubeconfig
export E2E_DRIVER_IMAGE=<registry>/<repo>@sha256:<digest>

mage e2e:static          # helm install + smokes (+ conformance when enabled)
mage e2e:staticReapCheck # read-only: list run-labeled leftovers
mage e2e:staticDown      # remove the driver release + run-labeled objects only
```

`mage e2e:static` selects `E2E_INFRASTRUCTURE_PROVIDER=static` and applies two
safety defaults (explicit env values win):

- `E2E_SKIP_CLEANUP=1` — the suite never tears anything down implicitly;
  teardown is the explicit `staticDown` target.
- `E2E_ENCRYPTION=0` — the encryption path deploys a dev-mode OpenBao into the
  `openbao` namespace, which is unacceptable on a shared cluster that may run
  a real OpenBao. Leave encryption disabled on the static lane.

## InfrastructureConfig notes

Each `storageOwners[]` entry needs a `machineDeploymentSuffix` value even
though the static lane never creates MachineDeployments — the shared config
validator requires it as the owner identity/selector field. Any unique,
non-empty string works (use the node name).

On the static lane the harness additionally:

- renders the taints from `E2E_NON_BLOCKING_TAINTS` as `NoSchedule`
  tolerations on the storage-owner Deployments (in addition to the canonical
  storage taint), so a storage node carrying a site role taint (for example a
  NAS role) still runs its storage agent;
- restricts the node-plugin DaemonSet to the first consumer worker group's
  `nodeSelector`, so mixed-architecture fleets and unrelated NotReady nodes do
  not block `helm --wait`. Ensure that selector matches at least two Ready,
  schedulable consumer nodes of one architecture.

## Configuration knobs

| Env | Required | Purpose |
| --- | --- | --- |
| `E2E_WORKLOAD_KUBECONFIG` | Yes | Kubeconfig of the pre-existing workload cluster. The static lane's only cluster handle. |
| `E2E_INFRASTRUCTURE_CONFIG` | Yes | Gitignored `InfrastructureConfig` YAML: storage owners (name, `nodeSelector` owner→node mapping, pool name/device, endpoints, network domain) and consumer worker groups. |
| `E2E_DRIVER_IMAGE` | Yes | Driver image (libzfs-enabled) in a registry every node can pull. |
| `E2E_RUN_ID` | Recommended | Stable run identity; all harness-created objects carry it as an ownership label, and `staticDown`/`staticReapCheck` operate on it. |
| `E2E_STORAGE_CLASS_OVERRIDES` | Recommended | Comma-separated `chartKey=name` StorageClass renames (e.g. `tankNVMe=zfs-e2e-nvme,tankNFS=zfs-e2e-nfs`) so chart classes cannot collide with same-named classes another driver already owns. Testdriver copies are generated into `test/e2e/_artifacts/` with matching references. |
| `E2E_NON_BLOCKING_TAINTS` | Recommended | Comma-separated taint keys the conformance node-readiness preflight ignores (site taints: storage, control-plane, and any other tainted-but-healthy roles). On the static lane these are also rendered as `NoSchedule` tolerations on storage-owner Deployments. Unset keeps the in-tree default. |
| `E2E_ALLOWED_NOT_READY_NODES` | No | Conformance `--allowed-not-ready-nodes`. Defaults to `1` on the static lane (a shared cluster may carry an unrelated NotReady node), `0` elsewhere. |
| `E2E_RUN_CONFORMANCE` | No | Set `1` to run the external-storage conformance suite after the smokes. |
| `E2E_CONFORMANCE_DISRUPTIVE` | No | Default off: static conformance skips `[Disruptive]`/`[Serial]` specs. Set `1` (with `E2E_SSH_PRIVATE_KEY_PATH`) only in a dedicated maintenance window. |
| `E2E_SSH_PRIVATE_KEY_PATH` | Conditional | Optional for static non-disruptive conformance; required when `E2E_CONFORMANCE_DISRUPTIVE=1` (kubelet-restart specs SSH into nodes). |
| `E2E_SKIP_SNAPSHOT_BUNDLE` | Implied | The static lane never force-applies the vendored external-snapshotter bundle when the `VolumeSnapshot` CRDs already exist; only the driver's `VolumeSnapshotClass` is ensured. |
| `E2E_NFS_EXPORT_CIDRS` | Recommended | Consumer-network CIDRs for the NFS StorageClasses; must cover every consumer node subnet. |
| `E2E_ZPOOL` | No | Pool name (default `tank`); must match the pool in the infrastructure configuration. |
| `E2E_ENCRYPTION` | Defaulted `0` | Keep `0` on shared clusters (see above). |
| `E2E_SKIP_CLEANUP` | Defaulted `1` | Keep `1`; the lane has no implicit teardown path. |

Chart-side, set `node.nodeSelector` (via the infrastructure-driven helm
overrides) when the node plugin DaemonSet must be restricted to a subset of
nodes — for example single-arch driver images on a mixed-arch cluster, or
consumer-labelled nodes only. Empty keeps the run-everywhere CSI default.

## Cleanup-safety contract

The cluster is not the harness's to destroy. Every namespaced and
cluster-scoped object the harness creates — smoke PVCs and consumer pods,
node-command pods, the VolumeSnapshotClass, and the VolumeAttributesClass —
carries the run ownership labels
(`app.kubernetes.io/name=zfs-csi-e2e`,
`app.kubernetes.io/managed-by=ginkgo-e2e`,
`zfs-csi.randomvariable.co.uk/e2e-run-id=<run>`), and cleanup operates
**exclusively** through that label selector plus the helm release:

- `mage e2e:staticDown` = deletion of run-labeled objects, then `helm uninstall`
  of the driver release (only when the release carries this run's ownership
  label). Nothing else matches; nothing else can be deleted. Requires Helm
  **3.13+** (`--ignore-not-found` on uninstall, `--labels` on install).
- `mage e2e:staticReapCheck` = read-only listing of run-labeled objects. It
  never deletes.
- The suite's `AfterAll` performs only the read-only pre-teardown inventory.

The static lane **NEVER**:

- calls `framework.DeleteClusterAndWait` / `DeleteAllClustersAndWait` (there
  is no CAPI cluster to delete, and the code path is skipped entirely);
- calls `cleanupAWSCRSAddons`;
- deletes foreign namespaces or any object without the run ownership labels
  (only run-labeled objects and the helm release are in scope);
- force-applies the external-snapshotter bundle into `kube-system` when the
  snapshot CRDs pre-exist (the cluster's own snapshot-controller keeps SSA
  ownership);
- applies into the `openbao` namespace (`E2E_ENCRYPTION=0` on this lane);
- enables the chart's default StorageClass names verbatim when a same-named
  class already exists — use `E2E_STORAGE_CLASS_OVERRIDES` so the run's
  classes are unambiguous and collision-free.

## Node mutation (not reverted by cleanup)

Setup patches the pre-existing cluster's Node objects and cleanup does **not**
revert them:

- owner nodes gain the canonical `zfs.csi.randomvariable.co.uk/storage`
  `NoSchedule` taint so ordinary workloads avoid the storage host;
- when a single network domain is configured, every otherwise-unlabelled node
  is stamped with that domain label for topology-aware scheduling.

These patches are idempotent and additive. Pre-label and pre-taint the
participating nodes yourself if you want the cluster left byte-for-byte
unchanged, because `mage e2e:staticDown` removes only the driver release and
run-labeled objects — never Node metadata.

## Conformance on a shared cluster

The defaults are chosen for co-tenancy: focus `External.Storage.*co\.uk`,
skip `[Disruptive]|[Serial]` (unless `E2E_CONFORMANCE_DISRUPTIVE=1`),
`--allowed-not-ready-nodes=1`, and site taints ignored via
`E2E_NON_BLOCKING_TAINTS`. Per-spec test namespaces are ephemeral and cleaned
by the upstream framework; run `mage e2e:staticReapCheck` afterwards to verify
no cluster-scoped leftovers (StorageClass/VolumeSnapshotClass copies, PVs)
remain from the run.

Artifacts (logs, kubeconfig copies, JUnit, generated testdrivers) land under
`test/e2e/_artifacts/`, which is gitignored.
