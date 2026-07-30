# zfs-csi

zfs-csi is a Kubernetes [Container Storage Interface (CSI)](https://kubernetes-csi.github.io/docs/)
driver that provisions storage from [OpenZFS](https://openzfs.org/) pools. It serves
two kinds of volumes from a dedicated storage node:

- **Block volumes** over **NVMe-TCP** — a ZFS zvol exported as an NVMe-oF target that
  a consumer node in the selected network domain attaches as an initiator (`ReadWriteOnce`).
- **Shared filesystems** over **NFS** — a ZFS dataset exported and mounted by any
  number of consumer nodes in the selected network domain (`ReadWriteMany`).

Each volume is reachable only from consumer nodes in the network domain selected for its
storage owner. The current Helm chart configures one owner endpoint/domain set and one global
node-plugin domain; it does not provide a production multi-owner rollout. Each volume can be
encrypted at rest with its own [ZFS native encryption](https://openzfs.github.io/openzfs-docs/man/master/8/zfs-load-key.8.html)
key sourced from [OpenBao](https://openbao.org/).

## The Documentation

This documentation follows the [Diátaxis](https://diataxis.fr/) framework. It is
organised by what you are trying to do, so start from the heading that matches your
current need:

- **Tutorials** — Learning-oriented lessons. Start here if you are new to zfs-csi and
  want a guided, end-to-end first experience.
- **How-to Guides** — Task-oriented recipes for operators who already know the basics
  and need to accomplish a specific goal (install, provision, snapshot, expand,
  encrypt, migrate, troubleshoot).
- **Reference** — Information-oriented, authoritative descriptions of the machinery:
  StorageClass parameters, Helm values, command-line flags, the version compatibility
  matrix, and the driver's Kubernetes API surface.
- **Explanation** — Understanding-oriented discussion of how and why zfs-csi works the
  way it does: its architecture, storage model, transport choices, encryption design,
  scheduling topology, and the deployments it fits.

## Common Starting Points

- [Assess use cases and deployment fit](explanation/use-cases.md), including when zfs-csi
  can replace the Kubernetes storage role of TrueNAS or democratic-csi.
- [Migrate from TrueNAS or democratic-csi](how-to/migrate-from-truenas-or-democratic-csi.md)
  with explicit data-copy, cutover, rollback, and retirement stages.
- [Import an existing ZFS volume](how-to/import-existing-zfs-volume.md) as retained static
  storage after administrator validation.
- [Migrate data into zfs-csi](how-to/migrate-data-into-zfs-csi.md) with ZFS replication or
  a validated CephFS file copy before import.
- [Understand multi-storage-agent topology and placement](explanation/multi-storage-agent-topology.md),
  including the current production-readiness gate.
- [Inspect and operate storage topology safely](how-to/operate-storage-topology.md) without
  changing immutable placement identity or assuming endpoint changes migrate active sessions.
- [Provision your first volume](tutorials/getting-started.md) in a guided tutorial.
- [Install zfs-csi with Helm](how-to/install-with-helm.md) after preparing the storage and
  consumer nodes.
- [Understand transport security](explanation/transport-security.md) before enabling NVMe-TCP TLS
  or NFS mutual TLS, including what certificate identity does and does not authorize.

The navigation grows as this documentation is written; sections appear in the top
navigation bar as they become available.

## Project Status

zfs-csi is developed in the open at
[github.com/randomvariable/zfs-csi](https://github.com/randomvariable/zfs-csi). The
Kubernetes API group `zfs.csi.randomvariable.co.uk` is at version `v1alpha1`; the API
may change before it stabilises.

## License

zfs-csi is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
