# Prepare Nodes for zfs-csi

This guide shows you how to prepare the storage node and the consumer nodes before
installing zfs-csi. Platform operators perform this preparation once per node, before the
driver is deployed. Preparing nodes incorrectly is the most common cause of a failed
first install, so complete every step that applies to each node role.

## Node Roles

zfs-csi distinguishes two node roles:

- The **storage node** hosts the ZFS pool and runs the privileged storage-owning
  components (the storage agent and the `nvmet` controller). A cluster has at least one.
- **Consumer nodes** run workloads that attach volumes. Every node that may schedule a
  pod using a zfs-csi volume — including the storage node itself — is a consumer node.

A single node can be both. The node plugin runs on every node, including the storage node, so
that any pod scheduled onto it can attach volumes. In the default deployment the storage node
is dedicated — the storage taint keeps ordinary workloads off it — but the node plugin still
runs there because it tolerates all taints.

## Prerequisites

Before you begin, verify that you have the following:

- Root or `sudo` access to each node.
- A running Kubernetes cluster. The driver is developed and tested against Kubernetes 1.36;
  see [Version Compatibility](../reference/compatibility.md).
- The ability to set node labels and taints with `kubectl`.

## Prepare the Storage Node

The storage node exports block volumes over NVMe-TCP and shared filesystems over NFS. It
must run a compatible OpenZFS release, own a ZFS pool, and carry the label and taint that
pin the storage-owning components to it.

### Step 1: Install a Compatible OpenZFS Release

The driver links the OpenZFS `libzfs` library and must handshake with the running OpenZFS
kernel module. Install **OpenZFS 2.2 or 2.3**. OpenZFS 2.1 (shipped by, for example,
Debian 12) is **not** compatible and fails driver startup with
`Failed to initialize the libzfs library`. See the
[Version Compatibility](../reference/compatibility.md) reference for the full matrix.

On Ubuntu 24.04, the packaged `zfsutils-linux` is OpenZFS 2.2:

```bash
sudo apt-get update
sudo apt-get install -y zfsutils-linux
zfs version
```

Example output:

```
zfs-2.2.2-0ubuntu9
zfs-kmod-2.2.2-0ubuntu9
```

### Step 2: Create the ZFS Pool

zfs-csi provisions volumes as datasets and zvols under a pool. Create a pool named `tank`
on the storage node's data disk. Substitute your own device for `/dev/disk/by-id/...`.

```bash
sudo zpool create -f tank /dev/disk/by-id/<your-data-disk>
zpool status tank
```

**Important:** Use a stable `/dev/disk/by-id/` path, not a `/dev/sdX` name, so the pool
survives device renumbering across reboots.

**If you use the `flash` tier,** create a second pool named `flash` the same way.

### Step 3: Install the NFS Server Tooling

Shared filesystem volumes are exported through the node's in-kernel NFS server. OpenZFS
drives that server through `exportfs`, which ships with the kernel NFS server package.

```bash
sudo apt-get install -y nfs-kernel-server
sudo systemctl enable --now nfs-server
```

**Note:** If you serve only block (NVMe-TCP) volumes from this node, the NFS server is not
required. Install it whenever the node serves any filesystem volume.

### Step 4: Verify the NVMe-TCP Target Modules

The storage agent exports zvols through the kernel `nvmet` target. Confirm the target
modules are available:

```bash
sudo modprobe nvmet nvmet-tcp
lsmod | grep nvmet
```

The Helm chart repeats this load in a privileged storage-agent init container, so the
modules are loaded automatically before the agent starts. The command above remains
useful for checking module availability before installation.

Example output:

```
nvmet_tcp              28672  0
nvmet                 143360  1 nvmet_tcp
```

### Step 5: Label and Taint the Storage Node

The storage-owning components schedule only onto nodes carrying the storage label, and the
matching taint keeps other workloads off. Apply both to the storage node.

```bash
kubectl label node <storage-node> zfs.csi.randomvariable.co.uk/storage=true
kubectl taint node <storage-node> zfs.csi.randomvariable.co.uk/storage=true:NoSchedule
```

**Note:** The label key and value must be exactly `zfs.csi.randomvariable.co.uk/storage`
and `true` — these are the defaults the Helm chart selects on. If you change them, override
`storageNode.selector` and `storageNode.tolerations` at install time.

## Prepare the Consumer Nodes

Consumer nodes attach block volumes as NVMe-TCP initiators and mount filesystem volumes
over NFS. They do not need OpenZFS or a pool.

### Step 1: Verify the NVMe-TCP Initiator Modules

The node plugin connects to NVMe-TCP targets by writing directly to the kernel
`/dev/nvme-fabrics` interface. It does **not** use the `nvme` command-line tool, so no
`nvme-cli` package is required — only the kernel modules must load.

```bash
sudo modprobe nvme-tcp nvme-fabrics
lsmod | grep nvme_tcp
```

The node DaemonSet loads `nvme-keyring`, `nvme-fabrics`, and `nvme-tcp` automatically
before the node plugin starts. The manual command remains useful for checking module
availability before installation.

Example output:

```
nvme_tcp               45056  0
nvme_fabrics           40960  1 nvme_tcp
```

### Step 2: Install the NFS Client

Filesystem volumes are mounted with NFS version 4. Install the NFS client utilities:

```bash
sudo apt-get install -y nfs-common
```

**Note:** If your consumer nodes will only ever attach block volumes, the NFS client is
not strictly required. Installing it keeps the node ready for either volume type.

## Verify Node Readiness

Confirm the storage node carries the label and taint:

```bash
kubectl get node <storage-node> --show-labels | grep zfs.csi
kubectl describe node <storage-node> | grep Taints
```

The label `zfs.csi.randomvariable.co.uk/storage=true` and a `NoSchedule` taint with the
same key should both appear. The nodes are now ready for the driver.

## Related Practices

- **Installation**: [Install zfs-csi with Helm](install-with-helm.md) (how-to)
- **First volume**: [Provision Your First Volume](../tutorials/getting-started.md) (tutorial)
- **Version support**: [Version Compatibility](../reference/compatibility.md) (reference)
- **Why these node roles exist**: [Architecture](../explanation/architecture.md) (explanation)
