# E2E golden node image: build and debug guide

The zfs-csi E2E harness (issue #19) runs on a nested KubeVirt/CAPK cluster whose
nodes boot a golden Ubuntu image built with upstream
[`kubernetes-sigs/image-builder`](https://github.com/kubernetes-sigs/image-builder),
customized to bake OpenZFS + the NVMe-oF / NFS kernel facilities the driver
needs.

The golden image is built **once per Kubernetes minor release**, not per CI run.
CI consumes the published CDI `DataSource` by name and clones per-run root PVCs;
it never rebuilds the image. The build is therefore an occasional, operator-run
task — this document is the runbook for it.

All build configuration (Kubernetes version, ISO override, VM resources, goss
disable, the `zfs_csi_e2e` customization role) lives in this repo. mage owns a
pinned, gitignored image-builder checkout under
`test/e2e/_artifacts/image-builder` (cloned at `ZFS_CSI_IMAGE_BUILDER_REF`,
default `v0.1.53`); it stages our customization role and packer var-file into
that clone and passes the var-file last so it wins. mage never writes into a
developer's personal image-builder tree.

The `zfs_csi_e2e` role is injected through image-builder's documented custom
role hook `node_custom_roles_post` (runs after the `node`/`kubernetes` roles,
before sysprep, where `ansible_kernel` is the final boot kernel). The other
hooks (`firstboot_custom_roles_pre/post`, `node_custom_roles_pre`,
`node_custom_roles_post_sysprep`) are available if a future step needs a
different phase.

To point at an existing local checkout instead (offline / iterating on
image-builder itself), set `IMAGE_BUILDER_CAPI_DIR` to its `images/capi`
directory; mage will use it and stage into it rather than cloning.

## Prerequisites

### QEMU/KVM

The build runs a QEMU VM with KVM acceleration. You need `/dev/kvm` (be in the
`kvm`/`libvirt` group) and the emulator binary:

```bash
sudo pacman -S --needed qemu-base   # Arch/CachyOS; use your distro's qemu package
```

Confirm the build VM is KVM-accelerated (not TCG) once it is running:

```bash
QPID=$(pgrep -f 'qemu-system-x86_64.*ubuntu-2404')
tr '\0' ' ' < /proc/$QPID/cmdline | grep -oE '\-machine [^ ]+|\-cpu [^ ]+'
# expect: -machine type=pc,accel=kvm   and   -cpu host
ls -l /proc/$QPID/fd | grep /dev/kvm    # qemu holds /dev/kvm open when KVM is active
```

Note: modern QEMU expresses KVM as `-machine ...,accel=kvm`, not the legacy
`-enable-kvm` / standalone `-accel kvm`. A literal grep for `-accel kvm` will
miss it. There is no `accel=kvm:tcg` fallback, so if KVM failed QEMU would
hard-error rather than silently drop to software emulation.

### Pinned Ansible (critical)

image-builder **v0.1.53** is tested against **ansible-core 2.18.6 on Python
3.11** — the ref/version are a matched pair and must move together:

- v0.1.53's `node` role uses boolean-correct conditionals (e.g.
  `when: enable_containerd_audit|default(false)|bool`), which strict-boolean
  `when` enforcement (ansible-core >= 2.18) requires. The older v0.1.42 tag +
  ansible-core 2.15 combination is stale and wrong: v0.1.42 passes string
  conditionals that only lenient pre-2.18 ansible tolerates.
- The build runs packer's ansible provisioner with `ansible-playbook` from
  `PATH`, so the pinned venv must win over the host ansible (e.g. host 2.21.1).
- The venv drifts; verify `ansible-playbook --version` == 2.18.6 before building.
- python3.11 is required (2.18 needs a py3.11+ controller; older ansible also
  breaks on py3.12+ via the removed `ast.Str`).

Create a gitignored venv with the exact combo and put it first on `PATH`:

```bash
cd /home/naadir/go/src/github.com/randomvariable/zfs-csi
rm -rf hack/venv/ansible
python3.11 -m venv hack/venv/ansible
hack/venv/ansible/bin/pip install 'ansible-core==2.18.6'
hack/venv/ansible/bin/ansible-galaxy collection install community.general ansible.posix
```

(A nix flake to pin all of this hermetically is a tracked backlog item.)

## Build command

```bash
cd /home/naadir/go/src/github.com/randomvariable/zfs-csi
export PATH="$PWD/hack/venv/ansible/bin:$PATH"   # pinned ansible FIRST on PATH

# mage clones image-builder v0.1.53 into test/e2e/_artifacts/image-builder on
# first run and reuses it thereafter. packer refuses to overwrite an existing
# output dir, so clear it before a rebuild:
rm -rf test/e2e/_artifacts/image-builder/images/capi/output/ubuntu-2404-kube-v1.36.2

go run github.com/magefile/mage@latest -f e2e:imageBuild
```

(To use an existing local image-builder checkout instead of the mage-managed
clone, `export IMAGE_BUILDER_CAPI_DIR=.../image-builder/images/capi` first.)

`e2e:imageBuild`:

1. renders our packer var-file to `test/e2e/_rendered/packer/` (Kubernetes
   version, ISO URL + checksum override, `cpus`/`memory`, `goss_entry_file`,
   `node_custom_roles_post=zfs_csi_e2e`);
2. stages the `zfs_csi_e2e` ansible role and the no-op gossfile into the
   image-builder checkout;
3. runs `make -C $IMAGE_BUILDER_CAPI_DIR build-kubevirt-qemu-ubuntu-2404`, which
   runs `packer build ... --var kubevirt=true` with our var-file passed last.

On success the kubevirt post-processor (`build_kubevirt_image.sh`) produces a
local containerdisk OCI image `ubuntu-2404-container-disk` (a `FROM scratch`
image with the qcow2 at `/disk/image.qcow2`).

### What our overrides change (all in `magefiles/magefile.go`, `e2ePackerTemplateData`)

| var | value | why |
| --- | --- | --- |
| `kubernetes_semver` / series / deb / rpm | `v1.36.2` / `v1.36` / `1.36.2-2.1` / `1.36.2` | target latest stable |
| `artifact_name` | `ubuntu-2404-kube-v1.36.2` | golden DataSource name |
| `iso_url` / `iso_checksum` | Ubuntu 24.04.4 | image-builder pins a point release Ubuntu removes once superseded (24.04.2 -> HTTP 404) |
| `cpus` / `memory` | `4` / `8192` | image-builder defaults 1 vCPU / 2048 MiB |
| `goss_entry_file` | `goss/zfs-csi-noop.yaml` | disable image-builder's brittle release-gate goss suite (see below) |

### goss is disabled

image-builder's goss suite is its upstream release gate and asserts brittle,
drift-prone facts (e.g. `crictl images` returning an exact preloaded-image list
for the pinned Kubernetes version). It fails the build *after* the image is
fully built, and packer then deletes the artifact. We ship a no-op gossfile
(`test/e2e/packer/image-builder/goss/zfs-csi-noop.yaml`, an empty `{}`), stage
it into the checkout, and point `goss_entry_file` at it. goss still runs but has
no tests and returns 0. The image is validated instead by our `zfs_csi_e2e`
role at bake time (a real `modinfo` assertion that the required modules exist
for the boot kernel) and by the real E2E driver run.

## Debugging

### Keep the VM alive on failure

By default packer deletes the VM and output on any error. To inspect a failing
build, make packer pause and keep the guest running (image-builder honors this):

```bash
ON_ERROR_ASK=1 go run github.com/magefile/mage@latest -f e2e:imageBuild
# packer sets -on-error=ask and waits, leaving the guest up for inspection
```

### VNC console

The build VM exposes a VNC console. The display is assigned dynamically — read
it from the live process:

```bash
QPID=$(pgrep -f 'qemu-system-x86_64.*ubuntu-2404')
tr '\0' ' ' < /proc/$QPID/cmdline | grep -oE '\-vnc [^ ]+'
# e.g. 127.0.0.1:81  ->  TCP port 5900 + 81 = 5981 (localhost only)

sudo pacman -S --needed tigervnc
vncviewer localhost:5981
```

Remote (from your workstation):

```bash
ssh -L 5981:127.0.0.1:5981 <build-host>
# then point any VNC client at localhost:5981
```

During the autoinstall phase packer drives the installer over VNC keystrokes, so
you will see the Ubuntu installer. Once SSH is up packer switches to SSH and the
VNC screen just shows the login prompt while ansible runs.

### Verbose packer + streamed log

```bash
PACKER_LOG=1 go run github.com/magefile/mage@latest -f e2e:imageBuild 2>&1 | tee /tmp/golden-build.log
```

### Liveness checks (is the build progressing or wedged?)

```bash
QPID=$(pgrep -f 'qemu-system-x86_64.*ubuntu-2404')

# CPU-time advancing => guest is executing; frozen => wedged
ps -o times= -p "$QPID"; sleep 5; ps -o times= -p "$QPID"

# qcow2 growing => still writing
F="$IMAGE_BUILDER_CAPI_DIR/output/ubuntu-2404-kube-v1.36.2/ubuntu-2404-kube-v1.36.2"
stat -c%s "$F"; sleep 15; stat -c%s "$F"

# is the guest sshd reachable on packer's forwarded port?
timeout 3 bash -c 'cat < /dev/null > /dev/tcp/127.0.0.1/2805' && echo open || echo closed
```

## Known failure mode: post-reboot networking stall

The build reboots the guest after the `firstboot` ansible play, then reconnects
over SSH. On a contended host this has repeatedly stalled: qemu CPU-time freezes,
the qcow2 stops growing, and packer logs
`ssh: handshake failed: ... connection reset by peer` while the guest sits idle.

Networking is the prime suspect, not CPU or disk:

- QEMU uses SLIRP user-mode networking with a host port-forward
  (`-netdev user,id=user.0,hostfwd=tcp::2805-:22`). After the reboot the guest
  must re-bring-up its NIC (netplan) and re-acquire DHCP from SLIRP before sshd
  is reachable again.
- Symptom pattern seen: the forwarded port can appear open while the guest is
  actually mid-boot / wedged, so port-open alone is not proof of liveness — use
  the CPU-time / qcow2-growth checks above.
- A healthy 4-vCPU KVM guest adds only ~4-8 to host load; a single build VM
  driving host load to ~37 with frozen CPU-time indicates the vCPU threads are
  parked (guest wedged on reboot), not CPU-bound.

To investigate: run with `ON_ERROR_ASK=1`, and when it stalls open the VNC
console at the reboot moment and check the guest:

```
systemctl                 # what is blocking boot / degraded
ip a                      # did the NIC come back up
journalctl -b             # boot log since reboot
systemctl status ssh systemd-networkd cloud-init
```

Look at netplan/cloud-init network re-config and any interaction with the
`zfs_csi_e2e` role's `net.ipv4.ip_forward` sysctl or module loads. A KVM
image-builder build should complete in roughly 10 minutes; anything approaching
an hour is this stall, not normal slowness.

## After a successful build

The containerdisk image `ubuntu-2404-container-disk` exists locally. Publishing
it to the cluster as the golden CDI `DataSource` (`ubuntu-2404-kube-v1.36.2`)
that CAPK clones per-run is a separate, mage-owned step (retag + push to Harbor,
apply a golden `DataVolume` with `source.registry` import, create the
`DataSource`). See `docs/e2e-driver-validation-plan.md`.
