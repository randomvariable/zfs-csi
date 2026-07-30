# Performance acceptance suite

This package provides the performance-only acceptance framework for the zfs-csi
Ginkgo E2E lane. Parser and statistical tests are safe in ordinary CI. Live
Kubernetes execution is compiled with `-tags=e2e,performance` and additionally
requires `ZFS_CSI_PERF=1`; neither condition alone can create workloads.

The suite is comparative, not a source of portable absolute performance
numbers. Every control and candidate is measured in the same cluster run and
stored with one environment fingerprint. A changed fingerprint invalidates the
run rather than normalising unlike machines.

## Runtime contract

Required live settings:

- `ZFS_CSI_PERF=1`
- `ZFS_CSI_PERF_FIO_IMAGE=<registry>/<image>@sha256:<digest>`
- `ZFS_CSI_PERF_GIT_COMMIT=<full commit>` for the evidence record

The reporting orchestrator calls `e2e.RegisterPerformanceAcceptance` after the
driver, scheduling, health, and storage correctness scenarios are wired. The
registered closure executes in one performance-labelled Ginkgo `It`, producing
one JUnit case. Build tags prevent that case from existing in ordinary E2E
binaries; `ZFS_CSI_PERF=1` is a second runtime guard.

Each lifecycle variant runs five unreported warmups and twenty measured cycles.
The attach interval starts before PVC creation and ends only after the consumer
is Running and the NVMe VolumeAttachment is attached; Running proves kubelet
completed device discovery and the mount. NFS additionally requires consumers
on two distinct non-storage nodes to be Running. Detach starts before deleting
all consumers and ends after pods (and therefore their kubelet mounts) are gone;
NVMe additionally requires the VolumeAttachment to report no attached path.

Each fio workload creates an 8 GiB PVC, writes a 4 GiB precondition file, runs a
30-second warmup, then captures three independent 60-second JSON+ samples:

- 4 KiB random read, four jobs, queue depth 32
- 4 KiB random write, four jobs, queue depth 32
- 1 MiB sequential read, one job, queue depth 32
- 1 MiB sequential write, one job, queue depth 32

## Acceptance contract

The deterministic evaluator reports median, p95, p99, MAD, CV, paired relative
median change, and a fixed-seed bootstrap 95% confidence interval. Lifecycle
results require at least 18 of 20 samples per variant and CV no greater than
20%. fio requires all three samples and throughput CV no greater than 10%.

The run is invalid if any sample fingerprint differs from `run.json`. Live
execution fails closed unless Kubernetes node facts and privileged diagnostic
facts provide kernel, runtime, CPU model/count, NIC, MTU and link speed for all
participating nodes. ZFS pool health, version, topology, size/free space and
fragmentation are required inputs and are included in the fingerprint.

## Evidence

The artifact directory contains:

- `run.json`: schema, environment, scenarios, assessments, and validity
- `samples.jsonl`: every measured lifecycle and fio observation
- `fio/*.json`: unmodified fio JSON+ output
- `events/lifecycle.jsonl`: timestamped create/ready/delete/detach boundaries

Raw evidence is append-only during execution. Summaries are regenerated from
these files; selecting best samples manually is not supported.

## Scenario availability

Executable controls compare NFS `nconnect=1`/`8`, NFS 4.1/4.2, zvol 16 KiB
against 8 KiB and 128 KiB, and XFS against ext4. Dataset `atime`/`xattr`, NVMe
queue/digest, inline-data, ZFS cache/durability, thin provisioning, and nfsd
thread candidates remain explicit skips until the driver exposes a safe API.
Finite `ctrl_loss_tmo` is permanently skipped because retry-forever is a
recovery invariant, not a performance candidate.
