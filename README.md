<!--
Copyright 2026 Naadir Jeewa

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0
-->

# zfs-csi

A Kubernetes [Container Storage Interface (CSI)](https://kubernetes-csi.github.io/docs/)
driver that provisions [OpenZFS](https://openzfs.org/) storage: **block volumes over
NVMe-TCP** and **shared filesystems over NFS**, with optional **per-volume ZFS native
encryption** keyed from [OpenBao](https://openbao.org/).

## Overview

zfs-csi is a disaggregated storage driver. ZFS pools live on one or more dedicated
storage nodes, and volumes are consumed over the network:

- **Block** — a ZFS zvol is exported as an NVMe-oF target over TCP; a consumer node in the
  selected network domain attaches it as an NVMe initiator. Supports `ReadWriteOnce`,
  `ReadWriteOncePod`, `ReadOnlyMany` (kernel-enforced read-only), and `ReadWriteMany`
  (raw block only, and the consumer must coordinate writes — see the docs).
- **Filesystem** — a ZFS dataset is exported over NFS and mounted by consumer nodes in the
  selected network domain. Supports `ReadWriteMany`, `ReadOnlyMany`, `ReadWriteOnce`, and
  `ReadWriteOncePod`.

Block volumes may be consumed in either filesystem or raw block `volumeMode`; filesystem
volumes are always mounted, and raw block mode is rejected for them.

Because volumes are reached over the network, consumer placement depends on the selected
storage owner's published network-domain reachability. The chart supports multiple logical
owners through `storageOwners`; each owner has its own endpoint and reachability configuration,
while each node-plugin pod advertises one configured network domain. Each dynamic volume can be encrypted at rest with its own key, generated
through OpenBao Transit and loaded into ZFS on the owning storage node.

## Features

- Dynamic provisioning of block (zvol) and filesystem (dataset) volumes, in filesystem or
  raw block `volumeMode`.
- Any number of ZFS pools, selected per StorageClass through the `pool` parameter.
- Volume snapshots and restore, and PVC cloning (same-pool).
- Online volume expansion.
- Mutable volume parameters through `VolumeAttributesClass`.
- Capacity reporting for topology-aware scheduling, and volume health monitoring.
- Retained static import of existing unencrypted ZFS datasets and zvols, on storage
  administrator opt-in.
- Per-volume ZFS native encryption with keys from OpenBao, including crypto-shred on
  delete.
- Transport security in flight: NVMe-TCP TLS with per-volume pre-shared keys, and NFS
  mutual TLS (`xprtsec=mtls`) with node client certificates delivered via Kubernetes
  PodCertificate projection.
- Prometheus metrics and OpenTelemetry tracing.

## Architecture at a Glance

zfs-csi is a single binary that runs in several modes:

| Mode          | Workload                    | Responsibility                                               |
| ------------- | --------------------------- | ----------------------------------------------------------- |
| `controller`  | Deployment                  | CSI Identity + Controller gRPC; provisioning orchestration   |
| `storage`     | DaemonSet (storage node)    | Materialises ZFS datasets/zvols, encryption, NFS exports     |
| `nvmet`       | DaemonSet (storage node)    | Reconciles NVMe-oF exports into the kernel `nvmet` target    |
| `node`        | DaemonSet (all nodes)       | CSI Node gRPC; attaches transports and mounts volumes        |
| `nvmet-stage` | node sidecar                | NVMe-TCP attach/detach helper for the node plugin            |
| `nfs-stage`   | node sidecar                | NFS mount/unmount helper for the node plugin                 |
| `tls-signer`  | Deployment (signing ns)     | Signs node client certificates for NFS mutual TLS            |

When transport security is enabled, a `tlshd` sidecar also runs in the node and storage
DaemonSets to answer the kernel's TLS handshake upcalls.

ZFS pool, dataset, and zvol operations use the OpenZFS `libzfs` library through a cgo
binding — there is no `zfs`/`zpool` command-line wrapping. Filesystem format and mount
operations use standard Linux userland tools. NFS exports do **not** go through OpenZFS
libshare: datasets are mounted with `sharenfs=off` and the storage agent answers the
kernel NFS server's export cache upcalls directly from an in-process responder, which is
what allows per-volume export policy (including `xprtsec=mtls`) that the `sharenfs`
property cannot express.

## Documentation

Full documentation follows the [Diátaxis](https://diataxis.fr/) framework — tutorials,
how-to guides, reference, and explanation — and is published as a documentation site.

- Browse the sources under [`docs/`](docs/).
- Assess replacement and migration scenarios in [Use Cases and Deployment Fit](docs/explanation/use-cases.md)
  and [Migrate from TrueNAS or democratic-csi](docs/how-to/migrate-from-truenas-or-democratic-csi.md).
- Adopt retained existing storage with [Import an Existing ZFS Volume](docs/how-to/import-existing-zfs-volume.md),
  or stage a ZFS/CephFS copy first with [Migrate Data into zfs-csi](docs/how-to/migrate-data-into-zfs-csi.md).
- Understand [Transport Security](docs/explanation/transport-security.md) before enabling
  NVMe-TCP TLS or NFS mutual TLS — in particular what certificate identity does and does not
  authorize.
- Read [Multi-Storage-Agent Topology and Placement](docs/explanation/multi-storage-agent-topology.md)
  before planning multiple logical storage owners; note the production-readiness caveat there,
  as this release permits only one NFS-exportable pool root per owner.
- Build the site locally with `mage docs:serve` (see below).

## Building the Documentation

The documentation site is built with [MkDocs](https://www.mkdocs.org/) and the
[Material for MkDocs](https://squidfunk.github.io/mkdocs-material/) theme. MkDocs is a
[mage](https://magefile.org/)-managed tool (installed on demand as a `uvx` shim; see
`.tools.yaml`), so no system Python or pip setup is required:

```bash
# Serve with live reload at http://127.0.0.1:8000
mage docs:serve

# Build the static site into site/ with strict link checking
mage docs:build
```

## Building the Driver

```bash
# Compile all packages (pure Go, fake ZFS backend — no libzfs needed)
make build

# Compile including the real libzfs cgo binding (needs libzfs dev headers)
make build-storage

# Unit and property tests
make test

# Regenerate CRDs and deepcopy code
mage generate:all
```

The container image is built from the repository `Dockerfile` and is **not** distroless.
The driver dynamically links `libzfs`, so the runtime base ships the OpenZFS userland; it
also ships filesystem and mount utilities for formatting and mounting volumes, the
`nfs-kernel-server` package for the kernel nfsd support files and its procfs interface
(the storage agent drives that kernel server directly rather than shelling out to
`exportfs`), and `tlshd` — the userspace TLS handshake agent the kernel calls up to for
NVMe-TCP and NFS transport security. See the documentation for the OpenZFS version
compatibility requirements.

## License

zfs-csi is licensed under the [Apache License 2.0](LICENSE).
