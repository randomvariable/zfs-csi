# Troubleshooting

This guide helps you diagnose and resolve common zfs-csi failures. Each section states the
problem, the symptoms, how to confirm the root cause, and how to resolve it.

## The Storage Agent Fails to Start with a libzfs Error

### Problem

The storage agent pod crash-loops immediately on startup because the OpenZFS version on the
storage node is incompatible with the driver's `libzfs` binding.

### Symptoms

You may be experiencing this issue if you observe any of the following:

- The `zfs-csi-storage` pod is in `CrashLoopBackOff`.
- The pod log contains `Failed to initialize the libzfs library`.

### Confirm the Root Cause

Check the storage agent log:

```bash
kubectl logs -n zfs-csi-system -l app.kubernetes.io/component=storage
```

**If the log shows `Failed to initialize the libzfs library`,** the storage node is running an
incompatible OpenZFS release. Confirm the node's version:

```bash
zfs version
```

**If it reports OpenZFS 2.1 or earlier, this is your issue.** The driver requires OpenZFS 2.2
or 2.3.

### Solution

Install a compatible OpenZFS release on the storage node. On Ubuntu 24.04, `zfsutils-linux` is
OpenZFS 2.2:

```bash
sudo apt-get install -y zfsutils-linux
zfs version
```

Ensure the node is not running Debian 12, which ships OpenZFS 2.1. Restart the storage agent
pod after upgrading:

```bash
kubectl delete pod -n zfs-csi-system -l app.kubernetes.io/component=storage
```

### Prevention

- Follow [Prepare Nodes for zfs-csi](prepare-nodes.md) before installing.
- Review the [Version Compatibility](../reference/compatibility.md) matrix.

## NFS Mounts Fail with "Access Denied by Server"

### Problem

A pod using a filesystem (NFS) volume cannot mount it because the consumer node's IP is outside
the export's permitted CIDR.

### Symptoms

You may be experiencing this issue if you observe any of the following:

- A pod using an NFS-backed claim is stuck in `ContainerCreating`.
- `kubectl describe pod` shows a mount error containing `access denied by server`.

### Confirm the Root Cause

Inspect the pod's events:

```bash
kubectl describe pod <pod-name>
```

**If the events show `access denied by server` on the NFS mount,** the configured export CIDR
does not cover the node's network. Compare the StorageClass `nfsExportCIDRs` with the consumer
nodes' actual addresses:

```bash
kubectl get nodes -o wide
```

**If the node IPs fall outside the export CIDR, this is your issue.**

### Solution

Set the export CIDR to cover the consumer nodes' network through the Helm value
`storageClasses.tankNFS.nfsExportCIDRs`, then upgrade the release:

```bash
helm upgrade zfs-csi ./charts/zfs-csi \
  --namespace zfs-csi-system \
  --reuse-values \
  --set storageClasses.tankNFS.nfsExportCIDRs=10.0.0.0/16
```

Recreate any claim that was provisioned with the wrong CIDR so the new export applies.

### Prevention

- Set `nfsExportCIDRs` at install time. See [Provision a Shared Filesystem](provision-shared-filesystem.md).

## A Volume Is Stuck Deleting

### Problem

A `PersistentVolumeClaim` or the driver's `Volume`/`NVMeExport` custom resource remains in a
deleting state because a finalizer is holding it.

### Symptoms

You may be experiencing this issue if you observe any of the following:

- A deleted PVC's `Volume` (`zv`) or `NVMeExport` (`nvex`) resource lingers with a deletion
  timestamp.
- The resource has a finalizer such as `nvmet.randomvariable.co.uk/export-protect`.

### Confirm the Root Cause

List the driver's custom resources and inspect the stuck one:

```bash
kubectl get zv,nvex
kubectl describe nvex <name>
```

**If the `NVMeExport` reports `activeConnection: true`,** an initiator still holds a live
NVMe-TCP connection. The `export-protect` finalizer deliberately blocks teardown while a
connection is live, to avoid tearing down storage out from under a running consumer.

### Solution

Ensure no pod still uses the volume:

```bash
kubectl get pods --all-namespaces -o json | grep <claim-name>
```

Delete any remaining consumer pod. Once the last initiator disconnects, `activeConnection`
becomes `false`, the finalizer releases, and the resource deletes automatically.

**Caution:** Do not force-remove the finalizer while a connection is live. Doing so can tear
down the target under a running consumer and cause I/O errors in the pod.

### Prevention

- Follow the ordered [Uninstall zfs-csi](uninstall.md) procedure: remove workloads first, then
  claims, then the driver.

## Getting More Detail

Increase your diagnostic visibility with the driver's observability signals:

- Component logs: `kubectl logs -n zfs-csi-system -l app.kubernetes.io/name=zfs-csi`
- Metrics and tracing: see the [Observability Reference](../reference/observability.md).

## Related Practices

- **Node preparation**: [Prepare Nodes for zfs-csi](prepare-nodes.md) (how-to)
- **Uninstall order**: [Uninstall zfs-csi](uninstall.md) (how-to)
- **Version support**: [Version Compatibility](../reference/compatibility.md) (reference)
