# Install zfs-csi with Helm

!!! note "Storage owner configuration"

    The chart supports multiple logical storage owners through `storageOwners`; each enabled
    owner needs its own storage-node placement, pool GUID set, network domain, and NFS/NVMe
    endpoints. When `storageOwners` is empty, the chart uses the legacy single-owner
    `storageNode` configuration. Configure consumer-node network domains to match the
    `reachableFrom` domains of the owners they can access.

This guide shows you how to install the zfs-csi driver into a Kubernetes cluster using the
bundled Helm chart. Platform operators perform this after the nodes are prepared. The chart
installs the custom resource definitions, the controller, the storage agent, the `nvmet`
controller, the node plugin, and the default StorageClasses.

## Prerequisites

Before you begin, verify that you have the following:

- A prepared storage node and prepared consumer nodes. See
  [Prepare Nodes for zfs-csi](prepare-nodes.md).
- `kubectl` configured against the target cluster with cluster-admin permissions.
- `helm` 3.8 or later.
- A container registry reachable from the cluster that hosts the zfs-csi image, and the
  image pushed to it. Build and push the multi-architecture image with `make docker`
  (set `IMAGE` to your registry, for example
  `make docker IMAGE=registry.example.com/zfs-csi/zfs-csi:v0.1.0`), which builds from the
  repository `Dockerfile`.

## Step 1: Clone the Repository

The chart ships in the repository under `charts/zfs-csi`.

```bash
git clone https://github.com/randomvariable/zfs-csi.git
cd zfs-csi
```

## Step 2: Identify the Storage Node Address

The controller, storage agent, and node plugin need the storage node's IP address to reach
the NVMe-TCP portal and the NFS server. Retrieve the storage node's internal address:

```bash
kubectl get node <storage-node> -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}'
```

Example output:

```
10.42.0.7
```

This RFC1918 address is an example. Note your storage-node address; you supply it as
`network.portalHost` and `network.nfsServer`.

## Step 3: Write a Values File

Create `zfs-csi-values.yaml`. At minimum, set the image repository, the storage node name,
and the storage node address. The example below enables the default `tank` StorageClasses.

```yaml
image:
  # Point this at the registry you pushed the image to.
  repository: registry.example.com/zfs-csi/zfs-csi
  tag: "v0.1.0"

controller:
  enabled: true
storage:
  enabled: true
node:
  enabled: true
  # Current chart reports this one domain from every node-plugin pod.
  networkDomain: workers

storageNode:
  # The Kubernetes node name hosting this chart's storage-agent workload.
  name: <storage-node>
  # Obtain this immutable identity with: zpool get -H -o value guid tank
  authoritativePoolGUIDs:
    - "12345678901234567890"
  # Stable consumer reachability class, not an IP address.
  networkDomain: workers
  # The pool's mountpoint on the host. A pool named "tank" mounts at /tank.
  poolMountRoot: /tank

network:
  # Example RFC1918 storage-node InternalIP from Step 2; replace it.
  portalHost: 10.42.0.7
  nfsServer: 10.42.0.7

storageClasses:
  tankNVMe:
    enabled: true
  tankNFS:
    enabled: true
    # Consumer-node network; required for an enabled filesystem StorageClass.
    nfsExportCIDRs:
      - 10.42.0.0/16
      - 2001:db8:42::/64
```

**If your pool is named something other than `tank`,** set `storageNode.poolMountRoot` to
that pool's mountpoint and adjust the StorageClass `pool` parameters. See the
[Helm Values Reference](../reference/helm-values.md) for every setting.

The default chart intentionally creates neither driver workloads nor StorageClasses. This
prevents an unconfigured install from selecting a storage host or network. Enable only the
workloads and transports you use; a pure NFS deployment does not need `network.portalHost`,
and a pure NVMe deployment does not need `network.nfsServer`.

## Step 4: Install the Chart

Install into a dedicated namespace. The `--create-namespace` flag creates it if it does not
exist.

```bash
helm upgrade --install zfs-csi ./charts/zfs-csi \
  --namespace zfs-csi-system \
  --create-namespace \
  --values zfs-csi-values.yaml
```

Example output:

```
Release "zfs-csi" does not exist. Installing it now.
NAME: zfs-csi
LAST DEPLOYED: ...
NAMESPACE: zfs-csi-system
STATUS: deployed
REVISION: 1
```

## Step 5: Verify the Deployment

Confirm the controller Deployment and the three DaemonSets are running.

```bash
kubectl get pods -n zfs-csi-system
```

Example output:

```
NAME                                  READY   STATUS    RESTARTS   AGE
zfs-csi-controller-6b9c9f8b7d-abcde   1/1     Running   0          60s
zfs-csi-node-2xk9p                    4/4     Running   0          60s
zfs-csi-node-7vh4t                    4/4     Running   0          60s
zfs-csi-storage-q8n2r                 1/1     Running   0          60s
nvmet-controller-q8n2r                1/1     Running   0          60s
```

**Note:** The `zfs-csi-storage` and `nvmet-controller` pods run only on the storage node.
The `zfs-csi-node` DaemonSet runs on every node and reports four containers ready (driver,
`nvmet-stage`, `nfs-stage`, and the node-driver-registrar).

Confirm the CSI driver registered and the StorageClasses were created:

```bash
kubectl get csidriver zfs.csi.randomvariable.co.uk
kubectl get storageclass
```

Example output:

```
NAME                           ATTACHREQUIRED   PODINFOONMOUNT   AGE
zfs.csi.randomvariable.co.uk   true             false            90s

NAME            PROVISIONER                    RECLAIMPOLICY   VOLUMEBINDINGMODE
zfs-tank-nvme   zfs.csi.randomvariable.co.uk   Delete          WaitForFirstConsumer
zfs-tank-nfs    zfs.csi.randomvariable.co.uk   Delete          Immediate
```

The driver is installed. Provision your first volume with the
[Getting Started tutorial](../tutorials/getting-started.md).

## Enabling Snapshots

The driver ships the CSI snapshotter sidecar, but the cluster-scoped snapshot machinery
(the `VolumeSnapshot` CRDs and the snapshot controller) is a separate, cluster-wide
component. Install it before creating snapshots. See
[Snapshot and Restore a Volume](snapshot-and-restore.md).

## Related Practices

- **Node preparation**: [Prepare Nodes for zfs-csi](prepare-nodes.md) (how-to)
- **First volume**: [Provision Your First Volume](../tutorials/getting-started.md) (tutorial)
- **Encryption**: [Enable Per-Volume Encryption](enable-encryption.md) (how-to)
- **Uninstall**: [Uninstall zfs-csi](uninstall.md) (how-to)
- **All settings**: [Helm Values Reference](../reference/helm-values.md) (reference)
- **Topology operations**: [Operate Storage Topology Safely](operate-storage-topology.md) (how-to)
- **Multi-owner model**: [Multi-Storage-Agent Topology and Placement](../explanation/multi-storage-agent-topology.md) (explanation)
