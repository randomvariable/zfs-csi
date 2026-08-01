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

Helm chart for the zfs-csi driver: CSI controller, node plugin, storage agent,
`nvmet` controller, CRDs, and the generated StorageClasses.

Full documentation lives in the repository docs, not here:

- [Helm values reference](../../docs/reference/helm-values.md)
- [StorageClass reference](../../docs/reference/storage-classes.md)
- [Components](../../docs/reference/components.md)

## Requirements

- Kubernetes >= 1.36 (declared as `kubeVersion: ">=1.36.0-0"`).
- Helm 3.8+ (OCI registry support is required for the chart dependency).
- For transport TLS (`network.tls.enabled=true`): the `PodCertificateRequest`
  feature gate enabled on kube-apiserver, kube-controller-manager, and kubelet.
  The gate is beta and off by default in Kubernetes 1.36.

## Dependencies

The chart depends on the pinned [bitnami/common][common] library chart, resolved
from OCI and recorded in `Chart.lock`. The vendored tarball under `charts/` is
not committed, so a source checkout must vendor it before any Helm command:

```console
helm dependency build charts/zfs-csi
```

`mage image:chartDeps` runs the same command, and `mage image:chart` runs it
automatically before packaging. Charts pulled from the OCI registry already
contain the dependency.

[common]: https://github.com/bitnami/charts/tree/main/bitnami/common

## Installing

```console
helm dependency build charts/zfs-csi
helm install zfs-csi charts/zfs-csi \
  --namespace zfs-csi --create-namespace \
  --set controller.enabled=true \
  --set node.enabled=true \
  --set storage.enabled=true \
  --set storageNode.name=<zfs-host-node> \
  --set-string storageNode.authoritativePoolGUIDs[0]=<pool-guid> \
  --set network.portalHost=<nvme-tcp-portal> \
  --set network.nfsServer=<nfs-server>
```

All workloads are disabled by default, so an unconfigured install renders CRDs
and RBAC only.

## Cluster default StorageClass

The chart marks no StorageClass as the cluster default unless asked to. Set
`storageClasses.defaultClass` to exactly one chart StorageClass key to annotate
that one class with `storageclass.kubernetes.io/is-default-class: "true"`:

```yaml
storageClasses:
  defaultClass: tankNVMeTLS
  # "plain" (default) or "encrypted" when encryption.enabled renders both variants.
  defaultClassVariant: encrypted
```

Rendering fails if the selected key is unknown, disabled, or not rendered by the
current release. See the [StorageClass reference](../../docs/reference/storage-classes.md#cluster-default-storageclass).

## Uninstalling

```console
helm uninstall zfs-csi --namespace zfs-csi
```

The TLS signing Namespace carries `helm.sh/resource-policy: keep` and survives
uninstall along with the private CA authority. Delete it only when intentionally
retiring that trust authority.

## Development

Chart render tests are Go tests that shell out to `helm template`:

```console
helm dependency build charts/zfs-csi
go test ./charts/zfs-csi/...
helm lint charts/zfs-csi
```
