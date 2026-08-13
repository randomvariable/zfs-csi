# Version Compatibility

This reference lists the version requirements for running zfs-csi. The most important
constraint is the OpenZFS version on the storage node: the driver links the OpenZFS
`libzfs` library and must handshake with the running OpenZFS kernel module.

## OpenZFS

The storage node must run an OpenZFS release whose `libzfs` the driver binary can
initialise. The driver image is built against OpenZFS 2.2 and loads the `libzfs.so.4`
runtime library by soname.

| OpenZFS on the Storage Node | Supported | Notes |
| --- | --- | --- |
| 2.3.x | Yes | Runs against the 2.2-built driver by soname. |
| 2.2.x | Yes | The version the driver image is built and tested against. Ubuntu 24.04 `zfsutils-linux` is 2.2. |
| 2.1.x | No | `libzfs.so.4` from 2.1 cannot handshake with the driver; startup fails with `Failed to initialize the libzfs library`. Debian 12 ships 2.1. |
| 2.0.x and earlier | No | Unsupported. |

**Important:** This is a storage-node requirement, not a cluster-wide one. Consumer nodes do
not run OpenZFS; they attach volumes over NVMe-TCP and NFS.

## Kubernetes

| Component | Version |
| --- | --- |
| Kubernetes | 1.36 minimum; developed and tested against 1.36 |
| CSI specification | 1.13.0 |

The driver requires Kubernetes 1.36 or later. It uses recent CSI sidecar releases (for example
`csi-provisioner` v6.3.0 and `csi-snapshotter` v8.2.0). Earlier releases are not tested.

### Required Feature Gates

This project targets a forward-looking Kubernetes testbed and depends on alpha and beta features
by design. The following gate is required for the feature noted:

| Feature gate | Required for | Enable on |
| --- | --- | --- |
| `PodCertificateRequest` | NFS mutual TLS (node client certificates) | API server, controller manager, kubelet |

Node client certificates for NFS mutual TLS are delivered **exclusively** through PodCertificate
projection. Without this gate enabled on all three components, nodes cannot obtain credentials
and TLS volumes will never become ready. On Kubernetes 1.36 this API is served at
`certificates.k8s.io/v1beta1`.

If you are not using transport security, this gate is not required. See
[Transport Security](../explanation/transport-security.md).

## Consumer Node Kernel Modules

Consumer nodes attach block volumes as NVMe-TCP initiators and mount filesystem volumes over
NFS. They require the following kernel modules, all of which are mainline and
architecture-independent:

| Module | Purpose |
| --- | --- |
| `nvme-tcp` | NVMe-over-TCP initiator transport (block volumes). |
| `nvme-fabrics` | NVMe-over-Fabrics core (loaded as a dependency of `nvme-tcp`). |
| `nvme-keyring` | Holds NVMe-TCP TLS pre-shared keys (transport security only). |

Consumer nodes must **not** load `nvmet` — that is the target-side module and belongs only on
storage nodes. The chart's init containers load the correct set for each role.

The node plugin connects to targets by writing to the kernel `/dev/nvme-fabrics` interface
directly, so the `nvme-cli` package is **not** required on consumer nodes. Filesystem
volumes require an NFS version 4 client (`nfs-common` or equivalent).

## Storage Node Kernel Modules and Packages

| Requirement | Purpose |
| --- | --- |
| `nvmet`, `nvmet-tcp` kernel modules | NVMe-over-TCP target (exports zvols). |
| OpenZFS 2.2 or 2.3 userland and kernel module | ZFS pool, dataset, and zvol operations. |
| `nfs-kernel-server` | Provides the kernel nfsd support files and procfs interface that the storage agent drives directly. |
| `nvme-keyring` kernel module | Holds NVMe-TCP TLS pre-shared keys (transport security only). |

## Container Image Base

The driver container image is based on Ubuntu 24.04 and is **not** distroless. The driver
dynamically links `libzfs`, so the runtime base ships the OpenZFS userland (`zfsutils-linux`).
It also ships `util-linux`, `e2fsprogs` and `xfsprogs` for formatting and mounting volumes,
`nfs-common` and `nfs-kernel-server` for the kernel nfsd support files and procfs interface,
and `tlshd` for transport security handshakes. A distroless base cannot run the driver.

Note that `nfs-kernel-server` is present for the kernel server's support files, not because the
driver shells out to `exportfs` — the storage agent answers the kernel's export cache upcalls
in-process. See [Transport](../explanation/transport.md).

## Architecture

The driver builds for `amd64` and `arm64`. The NVMe-TCP transport is mainline kernel code on
both architectures.

## See Also

- [Prepare Nodes for zfs-csi](../how-to/prepare-nodes.md) (how-to)
- [Troubleshooting](../how-to/troubleshooting.md) (how-to)
- [Architecture](../explanation/architecture.md) (explanation)
- [Transport Security](../explanation/transport-security.md) (explanation)

---

**Last Updated:** July 2026
**Version Compatibility:** OpenZFS 2.2 or 2.3; Kubernetes 1.36 minimum
