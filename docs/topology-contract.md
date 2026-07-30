# E2E Topology Contract

The E2E VM topology is data, not lifecycle behaviour. Phase 1 records the KubeVirt-first contract
that later renderers and mutating targets must follow; current mage targets remain non-mutating.

## Substrate Decision

- **Target substrate:** KubeVirt. The nested test cluster is a kubeadm cluster running inside
  KubeVirt `VirtualMachine` guests on an existing Kubernetes host cluster.
- **Local-dev reference:** the checked-in libvirt XML scaffold is frozen as a safe local rendering
  reference. It is not a second provider abstraction and should not grow feature parity promises.
- **Topology source:** `test/e2e/topology.yaml` is the contract for KubeVirt work. It captures roles,
  hostnames, join roles, architecture, machine type, resources, management/fabric addresses, stable
  MACs, disks, Kubernetes labels/taints, placement, readiness sentinels, image mode, kubeadm order,
  kubeconfig retrieval, and teardown ownership labels.

## First KubeVirt Lane

- **VMs:** `cp0`, `storage0`, `worker0`, and `worker1`.
- **Placement:** all VMs are pinned to one KubeVirt node for the first full lane.
- **Fabric:** storage traffic uses an L2-local Multus Linux bridge network. This is honest for a
  single-node KubeVirt lane; cross-node jumbo fabric is deferred until an underlay-specific design
  exists.
- **Storage media:** `storage0` gets dedicated data disks for `tank*` and `flash*`. Pool media must
  not be files inside the root disk.
- **Architecture:** amd64 is the first lane. arm64 remains explicit deferred parity until the host
  KubeVirt substrate has arm64 nodes and images.

## Image And Root Disk Rules

- `test/e2e/packer/` owns only zfs-csi customizations for upstream
  `kubernetes-sigs/image-builder/images/capi/packer/qemu`; it does not carry a local Packer template.
- `e2e:check` always runs topology validation, parses `.tekton/*.yaml`, and verifies the rendered
  image-builder JSON var file. With `IMAGE_BUILDER_CAPI_DIR` set, it also verifies the checkout has
  the expected upstream CAPI image-builder shape. Libvirt reference host checks are opt-in with
  `E2E_LIBVIRT_REFERENCE=1` or `e2e:libvirtReference` so the KubeVirt-first path is not blocked by
  local-dev-only tools.
- `e2e:imageFactoryCheck` is the full read-only image-builder lane. It renders/copies
  `zfs-csi-e2e.pkrvars.json` and the local `zfs_csi_e2e` Ansible role into image-builder, then runs
  `make validate-kubevirt-qemu-ubuntu-2404` with the mage-common-managed Packer binary and
  `PACKER_VAR_FILES`/`VAR_FILES` pointing at that explicit JSON var file. It does not run `packer init`,
  `packer build`, require KubeVirt credentials, or create host-cluster resources. `e2e:checkMutating`
  is the CI-facing alias for this lane and requires `IMAGE_BUILDER_CAPI_DIR` because it stages files
  into the external image-builder checkout.
- The golden node image gets kubeadm, kubelet, kubectl, and containerd from image-builder. The
  `zfs_csi_e2e` post-role adds NVMe tooling, NFS client/server packages, and OpenZFS/ZFS userspace so
  per-role cloud-init only wires identity, kubeadm tokens, labels, taints, and readiness sentinels.
- `containerDisk` is boot-only smoke. Root writes are ephemeral across VMI restart, so it is not valid
  for kubeadm, reboot, or full E2E lanes.
- Full E2E uses per-run root PVCs cloned from a golden KubeVirt DataSource/DataVolume through a
  clone-capable StorageClass.
- The runner must clone the golden root once per VM per run; it must not CDI-import the full base image
  separately for every VM.
- Rendered image-builder var files under `test/e2e/_rendered/packer/` are documentation/check artifacts
  only; they do not create DataSources, DataVolumes, root PVCs, or KubeVirt resources. The current
  image-factory lane renders and validates amd64 Ubuntu 24.04 only. arm64 remains deferred parity, not
  removed product support, and should add its own artifacts when an arm64 KubeVirt substrate is
  available.

## CAPI/CAPK And Teardown Rules

- CAPI owns the nested cluster lifecycle through CAPK infrastructure resources and the standard
  kubeadm bootstrap/control-plane APIs. The rendered design must express one `KubeadmControlPlane` for
  `cp0` and worker `MachineDeployment`/`KubeadmConfigTemplate` resources for `storage0`, `worker0`, and
  `worker1`; the runner must not hand-run `kubeadm init` or `kubeadm join` over SSH.
- Kubeconfig retrieval is part of readiness. The runner waits for the CAPI Cluster kubeconfig Secret,
  uses it for all nested-cluster mutations, and treats `infrastructure-ready`, `control-plane-ready`,
  `workers-ready`, `kubeconfig-secret-ready`, and `cluster-mutation-ready` as the ordered readiness
  gates.
- Teardown must delete only resources carrying the expected ownership labels. `e2e:down` must refuse
  to delete namespaces or shared resources missing those labels.
