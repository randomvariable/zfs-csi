# KubeVirt E2E Harness Scaffolding

This directory tracks the small, renderable pieces of the KubeVirt-first Ubuntu E2E harness from
`E2E-KVM-PLAN.md`. The current mage targets are intentionally non-mutating: they render and check
local reference assets, but they do not define libvirt networks, start domains, create disks, or
mutate a KubeVirt host cluster.

The target topology is four Ubuntu 24.04 KubeVirt VMs: one kubeadm control-plane node, one
ZFS/NFS/nvmet storage node, and two workers. The first full KubeVirt lane pins all VMs to one
KubeVirt node and uses a Multus Linux-bridge fabric for L2-local NVMe-TCP/NFS traffic. Cross-node
jumbo fabric is deferred until an underlay-specific design exists. The libvirt XML remains a frozen
local-dev reference only, not a second provider track.

## Layout

- `cloud-init/`: role fragments for `cp0`, `storage0`, and workers plus common node prep.
- `topology.yaml`: KubeVirt-first topology-as-data contract for roles, addresses, disks, image mode,
  kubeadm ordering, readiness, placement, and teardown labels.
- `packer/`: upstream `kubernetes-sigs/image-builder` integration notes plus the local Ansible role
  copied into image-builder when building Ubuntu 24.04 kubeadm node golden images.
- `libvirt/`: frozen local-dev reference network and domain templates for `mgmt0`, `fabric0`, and VM
  roles.
- `testdriver-*.yaml`: external-storage CSI driver manifests with only currently honest capabilities.

## Current Scope

- `e2e:render` writes frozen libvirt XML and the amd64 image-builder JSON var file under
  `test/e2e/_rendered/`. Mage no longer renders or mutates CAPK/CAPI resources.
- `e2e:check` always validates the topology contract, parses `.tekton/*.yaml`, and verifies the
  rendered image-builder Packer vars. With `IMAGE_BUILDER_CAPI_DIR` set, it also verifies the
  checkout has the expected upstream CAPI image-builder shape. Set `E2E_LIBVIRT_REFERENCE=1` or run
  `e2e:libvirtReference` to include local-dev libvirt reference tools and `/dev/kvm` availability.
- `e2e:imageFactoryCheck` is the full read-only image-factory check for manual/incoming CI lanes. It
  renders/copies the amd64 JSON var file and local Ansible role into the upstream image-builder
  checkout, then runs `make validate-kubevirt-qemu-ubuntu-2404` with `PACKER_VAR_FILES`/`VAR_FILES`
  set to that explicit JSON file and `PACKER_BIN` set to the mage-common-managed Packer binary. It
  still does not run `packer init`, `packer build`, require KubeVirt credentials, or create KubeVirt
  resources. `e2e:checkMutating` is the CI-facing alias and fails unless `IMAGE_BUILDER_CAPI_DIR` is
  set, because this lane stages files into that external checkout.
- `test/e2e` owns mutating CAPI/CAPK lifecycle through the CAPI Go E2E framework. The suite uses
  Ginkgo/Gomega, `ClusterProxy`, per-run `E2EConfig`, `ApplyClusterTemplateAndWait`, workload
  kubeconfig retrieval, and framework cleanup helpers. CAPI/CAPK provider installation is external
  to this repository; the suite fails loudly if the management cluster lacks required providers.
- `scripts/e2e-up`, `scripts/e2e-down`, `scripts/e2e-up-down`, and `scripts/e2e-test` are wrappers
  around `go test ./test/e2e/...`; they do not call `kubectl apply` or `kubectl delete` directly and
  they do not invent E2E identity. Direct `go test` and direct script runs require
  `E2E_RUN_ID=<dns-label>` and `E2E_CONFIG=/path/to/e2e-config.yaml`. Optional
  `E2E_HOST_KUBECONFIG` selects the management cluster kubeconfig.
- Mage `e2e:up`, `e2e:down`, `e2e:test`, and `e2e:testUpDown` run the existing shell wrappers as the
  local convenience path. They generate one DNS-label-safe `E2E_RUN_ID` with `uniqueE2ERunID` when it
  is not supplied and default `E2E_CONFIG` to `test/e2e/e2e-config.yaml`. `magefiles/e2e-test-up-down`
  is a shell-friendly spelling for callers that need the issue #19 `e2e:test:up:down` wrapper name.
- `e2e:load`, `e2e:deploy`, and `e2e:reboot` are explicit stubs.
- The first mutating lifecycle slice intentionally omits storage0 CAPK data disks. `topology.yaml`
  still records the desired ZFS pools, but rendered CAPK resources boot only root PVC clones until the
  correct data-disk attachment shape is proven on the target CAPK version.

## Useful Commands

```sh
mage -l
mage e2e:check
mage e2e:imageFactoryCheck
mage e2e:checkMutating
E2E_RUN_ID=pr-19-a E2E_CONFIG=/path/to/e2e-config.yaml go test -count=1 ./test/e2e/...
E2E_RUN_ID=pr-19-a E2E_CONFIG=/path/to/e2e-config.yaml scripts/e2e-up
E2E_RUN_ID=pr-19-a E2E_CONFIG=/path/to/e2e-config.yaml scripts/e2e-down
E2E_RUN_ID=pr-19-a E2E_CONFIG=/path/to/e2e-config.yaml scripts/e2e-test
scripts/e2e-up-down
mage e2e:testUpDown
mage e2e:test:up:down
E2E_LIBVIRT_REFERENCE=1 mage e2e:check
mage e2e:libvirtReference
mage e2e:render
IMAGE_BUILDER_CAPI_DIR=/path/to/image-builder/images/capi mage e2e:check
IMAGE_BUILDER_CAPI_DIR=/path/to/image-builder/images/capi mage e2e:imageFactoryCheck
IMAGE_BUILDER_CAPI_DIR=/path/to/image-builder/images/capi mage e2e:checkMutating
cp -a test/e2e/packer/image-builder/ansible/roles/zfs_csi_e2e /path/to/image-builder/images/capi/ansible/roles/
cp test/e2e/_rendered/packer/zfs-csi-e2e.pkrvars.json /path/to/image-builder/images/capi/packer/qemu/zfs-csi-e2e.pkrvars.json
make -C /path/to/image-builder/images/capi build-kubevirt-qemu-ubuntu-2404
```

Run Mage commands from the repository root. Mage automatically discovers
`magefiles/magefile.go`; do not pass `-d magefiles`, because that makes the
targets run from `magefiles/` and breaks their repository-relative paths.
`magefiles/mage_output_file.go` is Mage's temporary generated dispatch source;
it is ignored and must not be added to the repository.

Rendered files are written under `test/e2e/_rendered/`, which is ignored by git.

The upstream KubeVirt build target uses `images/capi/packer/qemu/qemu-ubuntu-2404.json`,
`packer/qemu/packer.json.tmpl`, and `packer/qemu/scripts/build_kubevirt_image.sh`. The output qcow2 is
wrapped into a KubeVirt containerDisk image; import that image or qcow2 into CDI once and publish a
golden DataSource before cloning full E2E root PVCs.

## Current Assumptions

- Ubuntu 24.04 is the guest baseline; image-builder pre-bakes kubeadm, kubelet, kubectl, and
  containerd, while the `zfs_csi_e2e` post-role adds NVMe tooling, NFS client/server tools, and
  OpenZFS packages into golden CAPK node images. Cloud-init is kept for per-role identity and
  readiness probes, not hand-run kubeadm lifecycle.
- KubeVirt is the target substrate; pre-installed CAPI with CAPK plus
  `KubeadmControlPlane`/kubeadm bootstrap resources owns the nested cluster contract.
- `storage0` stands in for the real storage host and is expected to carry the ZFS, NFS, and nvmet
  kernel state; worker nodes are consumers only.
- `containerDisk` is boot-only smoke. Full E2E needs per-run root PVC clones from a golden
  DataSource/DataVolume through a clone-capable StorageClass; rendered image vars under
  `_rendered/packer/` document the amd64 golden image root and installer ISO DataVolume name. arm64
  remains deferred parity until a matching KubeVirt substrate and image-factory lane are available.
- Management traffic uses the KubeVirt pod/management path; fabric traffic uses an L2-local Multus
  Linux bridge with deterministic IP/MAC assignments and 9000 MTU in the first lane.
- CAPI readiness ordering is explicit: infrastructure ready, control plane ready, workers ready,
  CAPI kubeconfig Secret ready, then nested-cluster mutation ready.
- Teardown must delete only host-cluster resources carrying the expected ownership labels.
- arm64 is deferred parity unless the KubeVirt substrate has arm64 hosts and images available.
- Testdriver manifests are capability-honest, not aspirational; enable snapshots, clones, topology,
  and controller expansion only after the driver implements them.

## Safe Phase Boundaries

1. Topology contract: keep `e2e:render`, `e2e:check`, and current `e2e:up` non-mutating while the
   KubeVirt contract is documented and validated.
2. Image factory: add Packer templates and checks without changing libvirt lifecycle behavior or
   creating KubeVirt resources.
3. KubeVirt render/check: render manifests from `topology.yaml` and run read-only prerequisite checks;
   do not maintain dual libvirt+KubeVirt renderers beyond the frozen libvirt reference.
4. CAPI/CAPK lifecycle design: lock down `KubeadmControlPlane`, worker bootstrap templates,
   kubeconfig Secret retrieval, readiness sentinels, and teardown ownership-label guard before mutation.
5. Mutating lifecycle: prove `down(up(x))` cleans and `down(clean)` is a no-op before create paths
   are enabled.
6. Test execution: wire deploy/conformance/scenarios after CAPI reports all guest Machines ready,
   image loading works, and teardown is reliable.
