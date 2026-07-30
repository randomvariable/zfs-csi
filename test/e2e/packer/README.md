# Upstream image-builder integration

This directory intentionally does not carry a local Packer template. Build the
E2E kubeadm node image from `kubernetes-sigs/image-builder/images/capi` using the
upstream QEMU/KubeVirt target:

```sh
git clone https://github.com/kubernetes-sigs/image-builder.git /tmp/image-builder
export IMAGE_BUILDER_CAPI_DIR=/tmp/image-builder/images/capi
mage e2e:render
cp test/e2e/_rendered/packer/zfs-csi-e2e.pkrvars.json \
  "$IMAGE_BUILDER_CAPI_DIR/packer/qemu/zfs-csi-e2e.pkrvars.json"
cp -a test/e2e/packer/image-builder/ansible/roles/zfs_csi_e2e \
  "$IMAGE_BUILDER_CAPI_DIR/ansible/roles/"
export PACKER_BIN="$PWD/hack/bin/$(go env GOOS)/$(go env GOARCH)/packer"
export PACKER_VAR_FILES="$IMAGE_BUILDER_CAPI_DIR/packer/qemu/zfs-csi-e2e.pkrvars.json"
export VAR_FILES="$PACKER_VAR_FILES"
make -C "$IMAGE_BUILDER_CAPI_DIR" build-kubevirt-qemu-ubuntu-2404
```

`mage e2e:imageFactoryCheck` performs the non-mutating Phase-A path: it renders
`test/e2e/_rendered/packer/zfs-csi-e2e.pkrvars.json`, copies it into
`$IMAGE_BUILDER_CAPI_DIR/packer/qemu/`, copies the `zfs_csi_e2e` Ansible role into
`$IMAGE_BUILDER_CAPI_DIR/ansible/roles/`, sets `PACKER_BIN` to the mage-common-managed
Packer binary, points `PACKER_VAR_FILES`/`VAR_FILES` at the explicit JSON var file, and
runs the upstream validation target. It does not run `packer init`, `packer build`,
require KubeVirt credentials, or create host-cluster resources. `mage e2e:checkMutating`
is the CI-facing alias and requires `IMAGE_BUILDER_CAPI_DIR` because it stages files into
that external checkout.

The future mutating build target will build
`output/ubuntu-2404-kube-v1.36.2/ubuntu-2404-kube-v1.36.2` as a qcow2 artifact and then
run `packer/qemu/scripts/build_kubevirt_image.sh`, which wraps that qcow2 at
`/disk/image.qcow2` in a KubeVirt containerDisk image tagged
`ubuntu-2404-kube-v1.36.2-container-disk`.

Use the containerDisk tag only for boot smoke or import the qcow2/containerDisk into
CDI once, publish it as a golden `DataSource`, and clone per-run root PVCs from that
source for full kubeadm/reboot E2E runs.
