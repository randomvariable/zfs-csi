# E2E AWS lane (CAPA) — prerequisites

The CAPA (Cluster API Provider AWS) E2E lane provisions real EC2 nodes so the
zfs-csi driver can be validated on fast, non-Ceph storage. It runs **alongside**
the KubeVirt-on-Ceph lane, not as a replacement.

This lane is mutating and costs real money. The prerequisites below are
**external** to this repo and must be satisfied before `E2E_INFRASTRUCTURE_PROVIDER=aws`
will work. The suite fails fast if they are missing; it does not silently skip.

## Quick start and environment

Copy the repository's safe template, fill only the account-specific values, and
load it with your preferred dotenv tool. `.env` is ignored; `.env.example` is
tracked and intentionally contains no credentials.

```bash
cp .env.example .env
set -a; . ./.env; set +a
mage e2e:check
mage e2e:aws
```

`mage e2e:aws` pins the cluster for iteration (`E2E_SKIP_CLEANUP=1`). It builds
and pushes the libzfs driver image before provisioning, then sets
`E2E_DRIVER_IMAGE` to `ZFS_CSI_IMAGE_REPO:ZFS_CSI_IMAGE_TAG`. The mutable local
development default is `:dev`; set `ZFS_CSI_IMAGE_TAG` for another tag. It does
not create CAPA identities or AWS credentials; those belong to the management
cluster.

| Variable | Required for `mage e2e:aws` | Default / purpose |
| --- | --- | --- |
| `AWS_REGION` | Yes | Target AWS region. Set `AWS_DEFAULT_REGION` too for AWS CLI/SDK consistency. |
| `AWS_SSH_KEY_NAME` | Yes | EC2 key-pair name, not its private-key path. |
| `AWS_AMI_ID` | Yes | Ubuntu 24.04 AMI in the target region. |
| `ZFS_CSI_IMAGE_REPO` | Yes | ECR repository to receive the linux/amd64 driver image. |
| `ZFS_CSI_IMAGE_TAG` | No | Driver tag; defaults to mutable `dev`. |
| `E2E_DRIVER_IMAGE` | No | Set by `mage e2e:aws` from the repository and tag. |
| `AWS_PROFILE` | No | Preferred local AWS SDK credential source; SSO/ambient credentials also work. |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | No | Static/temporary credential alternative; keep exclusively in ignored `.env` or a credential manager. |
| `AWS_IDENTITY_KIND`, `AWS_IDENTITY_NAME` | No | CAPA `AWSCluster.spec.identityRef`; default `AWSClusterControllerIdentity` / `default`. |
| `E2E_RUN_ID` | No | Stable name such as `aws-dev` for cluster reuse. |
| `E2E_RUN_CONFORMANCE` | No | Set `1` for the slower external-storage conformance suite. |

`AWS_IDENTITY_KIND` accepts `AWSClusterControllerIdentity`,
`AWSClusterStaticIdentity`, or `AWSClusterRoleIdentity`. Use a static or role
identity when CAPA's controller identity belongs to a different account than
the E2E resources. The identity resource and its credential Secret are managed
by CAPA in `capa-system`; neither belongs in this repository's template or
dotenv example.

## 1. CAPA installed on the management cluster (external)

Per the issue #19 convention, provider installation is handled out of band —
the suite does **not** `clusterctl init` CAPA. Install it on the management
cluster (the same cluster that runs the KubeVirt lane):

```bash
clusterctl init --infrastructure aws
```

The suite assumes the `aws` provider and the `capa-system` namespace are present
and reconciling. Verify before a run:

```bash
clusterctl describe-cluster --providers | grep aws
kubectl get pods -n capa-system
```

## 2. AWS credentials

CAPA needs an AWS credential Secret in the `capa-system` namespace
(`cluster-api-provider-aws` calls EC2/VPC/IAM APIs). Provide real AWS creds
with sufficient permissions for: VPC/subnet/IGW/route-table creation, EC2
run/terminate, EBS volume create/delete, ENI create/delete/modify, and security
group management. A power-user-style policy is the simplest starting point;
tighten to least-privilege once the lane is stable.

```bash
export AWS_REGION=...
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
clusterctl init --infrastructure aws   # creates the creds Secret if absent
```

The **tag-based reaper** (Phase 3 cost/leak guard) additionally needs
`ec2:TerminateInstances`, `ec2:DeleteVolume`, and
`ec2:DeleteNetworkInterface` on resources tagged with the e2e run-id.

By default the generated `AWSCluster` uses the CAPA controller identity named
`default`. When the management cluster's controller identity belongs to a
different AWS account, create an `AWSClusterStaticIdentity` and select it with
`AWS_IDENTITY_KIND=AWSClusterStaticIdentity` and `AWS_IDENTITY_NAME=<name>`.
The referenced Secret must be in `capa-system` and contain `AccessKeyID`,
`SecretAccessKey`, and, for temporary credentials, `SessionToken`.

## 3. SSH key pair

Create an EC2 key pair in the target region. The public half is registered with
AWS; the private half is used for debugging and by storage conformance tests
that SSH into workload nodes. Pass the key-pair **name** via
`AWS_SSH_KEY_NAME` and its local private-key path via
`E2E_SSH_PRIVATE_KEY_PATH`. The private key remains local and is not stored in
the management cluster.

```bash
aws ec2 create-key-pair --key-name zfs-csi-e2e --query 'KeyMaterial' --output text > ~/.ssh/zfs-csi-e2e.pem
chmod 600 ~/.ssh/zfs-csi-e2e.pem
```

## 4. Ubuntu 24.04 AMI

Use the project image-builder Ubuntu 24.04 AMI for the region and pass its ID
via `AWS_AMI_ID`. Build it with `mage e2e:imageBuildAWS`; the baked role verifies
the ZFS/NVMe modules plus NFS RPC-with-TLS kernel prerequisites. A stock
Canonical AMI is useful only for exploratory runs and is not an accepted PCR
mTLS fixture because its exact kernel and modules can change between builds.

```bash
aws ec2 describe-images --owners 099720109477 \
  --filters "Name=name,Values=ubuntu/images/hvm-ssd/ubuntu-jammy-24.04-amd64-server-*" \
  --query 'sort_by(Images,&CreationDate)[-1].ImageId' --output text
```

(`099720109477` is Canonical's AWS account id.)

The required NFS TLS kernel contract is:

- `CONFIG_NET_HANDSHAKE=y` and the registered generic-netlink `handshake`
  family used by `tlshd`;
- `CONFIG_TLS=y` or `m`, with the `tls` module loadable;
- `CONFIG_NFSD=y` or `m`, `CONFIG_SUNRPC`, and `CONFIG_KEYS`;
- server-side SUNRPC TLS support (`svc_tcp_handshake`) in the running kernel;
- `nfs-kernel-server`/`nfs-utils` with `xprtsec=` export-policy support and a
  `tlshd` userspace compatible with the kernel handshake ABI.

The image-builder role and AWS storage bootstrap assert these symbols against
the kernel that actually boots. A missing module, generic-netlink family, or
server handshake symbol fails image/bootstrap creation; the lane does not try
to repair a kernel capability after Kubernetes starts.

There is no `/proc/fs/nfsd/tls` runtime control in upstream Linux 6.17. The AWS
PCR run previously treated that nonexistent path as a capability signal even
though the retained 6.17 AWS kernel had `CONFIG_NET_HANDSHAKE=y`, loadable
`tls.ko` and `nfsd.ko`, and server-side SUNRPC TLS code. Driver runtime probing
therefore resolves the generic-netlink `handshake` family instead. AWS storage
bootstrap and the baked image both fail closed unless the TLS module and kernel
handshake implementation are present; they never fake this check.

The project image-builder role also installs `linux-modules-extra` for the boot
kernel, resolves the live generic-netlink `handshake` family, and verifies the
boot kernel's `sunrpc` module contains `svc_tcp_handshake`. Runtime storage
bootstrap repeats those checks. It inspects the module because that function is
static and therefore is not guaranteed to appear in `/proc/kallsyms`. OpenZFS
2.2/2.3 libshare does not accept
`xprtsec` in its fixed `sharenfs` option allow-list, even though Noble's
`nfs-utils` 2.6.4 and kernel do. Until OpenZFS gains that option, applying an
`xprtsec=mtls` export must use `exportfs` directly rather than claiming kernel
or AMI support is absent.

## 4b. AWS CCM permission (one-time, persistent)

The workload cluster runs with `cloud-provider: external`; the AWS Cloud
Controller Manager (delivered via ClusterResourceSet) runs on the control-plane
node under the **control-plane** instance role
(`control-plane.cluster-api-provider-aws.sigs.k8s.io`) and calls
`ec2:DescribeAvailabilityZones` to initialise nodes. The clusterawsadm-bootstrapped
role does **not** grant that action, so without this the CCM logs `403
UnauthorizedOperation` and never clears the `uninitialized` taint → nodes stay
NotReady. Add a small inline policy once (it persists across provisions):

```bash
aws iam put-role-policy \
  --role-name control-plane.cluster-api-provider-aws.sigs.k8s.io \
  --policy-name zfs-csi-ccm-describe-azs \
  --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"ec2:DescribeAvailabilityZones","Resource":"*"}]}'
```

## 4c. Driver image in ECR

The workload nodes pull the driver image, but they cannot reach the homelab
Harbor. Create an ECR repo in the same account/region and configure
`ZFS_CSI_IMAGE_REPO`; `mage e2e:aws` pushes the linux/amd64 driver image and
sets `E2E_DRIVER_IMAGE` from its mutable `:dev` tag (or `ZFS_CSI_IMAGE_TAG`).
The harness mints a pull secret from the node IAM role because the CAPA AMI has
no ecr-credential-provider, even though the node role has ECR read permissions.

```bash
aws ecr create-repository --repository-name zfs-csi --region us-east-1
export ZFS_CSI_IMAGE_REPO=<account>.dkr.ecr.us-east-1.amazonaws.com/zfs-csi
# Optional: defaults to dev.
export ZFS_CSI_IMAGE_TAG=dev
mage image:driver
# Prints: E2E_DRIVER_IMAGE=<account>.dkr.ecr.us-east-1.amazonaws.com/zfs-csi:dev
```

The node instance role must include the ECR read set
(`ecr:GetAuthorizationToken`, `ecr:BatchGetImage`, `ecr:GetDownloadUrlForLayer`,
`ecr:BatchCheckLayerAvailability`) — the clusterawsadm `nodes` managed policy
already does.

## 5. CNI: Calico native + disable EC2 source/dest check

**This is the one AWS-specific networking gotcha.** EC2 enables
`SourceDestCheck=true` per ENI by default: the hypervisor drops any frame whose
source or destination MAC is not the ENI's. Calico assigns pod IPs **outside**
the VPC CIDR, so cross-node pod traffic (e.g. the NFS RWX reader on worker1
reading the writer's export on worker0) is dropped unless the check is disabled
on every instance.

Per the Tigera AWS reference, with the check disabled Calico routes **natively
within a VPC subnet with no overlay** — high performance, no VXLAN.

**CAPA disables the check for us.** The CAPA controller (`capa-controller-manager`
identity) calls `ec2:ModifyInstanceAttribute` to turn off source/dest check on
every instance it creates — this is unconditional default behaviour, not a
configurable field (there is no `sourceDestCheck` key on `AWSCluster`/`AWSMachine`
in CAPA v2.11). So no CRD setting, no Felix env, and no extra node-role IAM are
required; the node instance role intentionally lacks
`ec2:ModifyNetworkInterfaceAttribute`.

The CNI manifest uses `CALICO_IPV4POOL_IPIP=CrossSubnet`: pure native routing for
same-subnet node pairs, encapsulating only across subnets/AZs (CAPA's managed VPC
provisions one subnet per AZ). Pin the MachineDeployments to a single failure
domain and it degenerates to fully native.

The AWS lane ships `data/cni/calico-aws.yaml`; the KubeVirt lane keeps its
existing `calico.yaml` (BGP, unchanged). CNI is per-lane config.

## Storage disk path (E2E_DATA_DISK_BY_ID)

The storage node attaches a dedicated gp3 volume for the disposable ZFS pool.
On Nitro instances (t3a) EBS volumes surface as NVMe under a volume-id-specific
name, so the raw `/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_<volid>` path
cannot be hardcoded. The flavor installs `amazon-ec2-utils`, whose udev rules
create a **stable `/dev/xvdb` symlink** from the volume's `deviceName`
(order-independent). Set:

```
E2E_DATA_DISK_BY_ID=/dev/xvdb
```

(the KubeVirt lane default is `/dev/disk/by-id/virtio-tank0`). The AWS mage
target sets this automatically; set it manually only for a bare `go test` run.

Verified by the Phase 0.5 substrate probe on the stock CAPI ubuntu-24.04 AMI:
all required kernel modules (zfs, nvmet-tcp, nvme-fabrics, nvme-tcp,
target_core_mod) load and `zpool create` succeeds after a runtime
`apt-get install zfsutils-linux linux-modules-extra-$(uname -r)` — **no golden
AMI bake is needed**. The AMI root device is `/dev/sda1`; the flavor's
`AWS_ROOT_DEVICE_NAME` must match it or EC2 attaches a phantom extra disk
instead of resizing root.

## Cluster reuse (dev loop)

Bringing up an EC2 cluster is slow and costs money, so during driver iteration
you create the cluster **once** and reuse it across many suite runs, tearing it
down only when finished. The harness already supports this — reuse is keyed on a
stable run ID plus skip-cleanup:

```bash
# First run: create the cluster, run the suite, KEEP it.
E2E_RUN_ID=aws-dev E2E_SKIP_CLEANUP=true  mage e2e:aws

# Subsequent runs: same run ID → same cluster name → clusterctl re-applies via
# SSA and the ready-waits pass instantly (idempotent reuse). Setup pods
# delete-if-exists first and `zpool create` is guarded by `zpool list`, so
# re-runs neither collide nor double-create.
E2E_RUN_ID=aws-dev E2E_SKIP_CLEANUP=true  mage e2e:aws

# When done: destroy the named cluster (skips creation, deletes by name).
E2E_RUN_ID=aws-dev E2E_CLEANUP_ONLY=true  mage e2e:aws
```

The shared namespace and any golden artifacts are intentionally **not** deleted
on teardown — only the per-run cluster resources are. Do not hard-code a random
run ID for the dev loop; a stable `E2E_RUN_ID` is what makes reuse work.

## Cleanup, artifacts, and costs

Destroy a reused cluster explicitly when finished:

```bash
E2E_RUN_ID=aws-dev mage e2e:awsDown
```

`awsDown` only needs the pinned run identity and management-cluster access; it
does not require an AMI, SSH key, or driver image. Failed/deleted CAPI objects
can still leave tagged AWS resources while the provider is unavailable. Run
`mage e2e:awsReapCheck` to classify `zfs-csi-e2e=owned` resources without
deleting anything, then investigate suspected orphans before manual cleanup.

Per-run rendered templates, logs, kubeconfigs, and conformance JUnit output live
under `test/e2e/_artifacts/`, which is ignored. A conformance run also writes
`conformance-run-metadata.json`: git commit/dirty state, Ginkgo seed, provider
and cluster identity, safe non-secret environment identity, driver image
reference, chart reference and overrides, and testdriver SHA-256 hashes.
Credential values, kubeconfigs, and tokens are never written.

## Direct PodCertificate NFS mTLS acceptance

The AWS CAPI template enables Kubernetes 1.36 `PodCertificateRequest` on the
API server, controller manager, and every kubelet. The API server also serves
`certificates.k8s.io/v1beta1/podcertificaterequests` explicitly through
`--runtime-config`; do not remove that setting while the driver projects the
v1beta1 source.

Run the bounded acceptance on a unique run ID. The target preserves the cluster
for diagnostics and enables transport TLS plus the long rotation assertion:

```bash
E2E_RUN_ID=pcr-$(date -u +%Y%m%d-%H%M%S) \
  AWS_REGION=us-east-1 AWS_SSH_KEY_NAME=zfs-csi-e2e \
  E2E_AWS_BASTION_ALLOWED_CIDR=<public-ip>/32 AWS_AMI_ID=<k8s-1.36-ami> \
  ZFS_CSI_IMAGE_REPO=<account>.dkr.ecr.us-east-1.amazonaws.com/zfs-csi \
  mage e2e:awsPodCertificate
```

Acceptance records non-secret evidence in
`test/e2e/_artifacts/<run>/pod-certificate-nfs-mtls.json` and requires:

1. node pod carries separate projected `tls.crt` and `tls.key` paths;
2. signer sets `Issued`, a certificate chain, and `beginRefreshAt` on an
   attested PCR, then kubelet obtains a different certificate during its natural
   refresh window (70-minute test budget for the chart's one-hour leaf);
3. an isolated privileged probe pinned to the same consumer node directly
   attempts the live export with no client certificate and must be rejected;
4. the probe presents an ephemeral foreign-CA client chain and must be rejected
   without changing shared node or server trust state; and
5. the probe presents its directly projected signer chain and must mount the
   same export successfully. The ordinary TLS RWX smoke remains the CSI path
   proof, while this matrix proves the three peer-authentication outcomes.

Probe resources use the current `E2E_RUN_ID` ownership labels and a fixed
`zfs-csi-e2e-pcr-peer-probe` name in the driver namespace. Reset and deferred
cleanup refuse to delete an object that lacks those exact ownership labels.

Before accepting a release, preserve diagnostics while the owned cluster still
exists:

```bash
KUBECONFIG=$(find test/e2e/_artifacts -path "*<run>*" -name kubeconfig -type f -print -quit)
kubectl --kubeconfig "$KUBECONFIG" -n zfs-csi get podcertificaterequests.certificates.k8s.io -o yaml \
  > test/e2e/_artifacts/<run>/pod-certificate-requests.yaml
kubectl --kubeconfig "$KUBECONFIG" -n zfs-csi logs statefulset/zfs-csi-tls-signer \
  > test/e2e/_artifacts/<run>/tls-signer.log
kubectl --kubeconfig "$KUBECONFIG" -n zfs-csi get daemonset zfs-csi-node -o yaml \
  > test/e2e/_artifacts/<run>/zfs-csi-node.yaml
kubectl --kubeconfig "$KUBECONFIG" -n zfs-csi get pods -o wide \
  > test/e2e/_artifacts/<run>/pods.txt
```

Do not delete resources by broad label or namespace search. Use the pinned run
state and `mage e2e:awsDown`; it follows this lane's owned-resource cleanup and
read-only orphan scan.

`E2E_DRIVER_IMAGE` accepts either a tag such as `repo:dev` or an immutable
digest reference. Local AWS iteration uses the mutable `:dev` tag produced by
`mage e2e:aws`; release evidence may use a digest reference when needed.

Cleanup preserves evidence in this order: capture live Kubernetes/backend leak
inventory, tear down the CAPI workload cluster, then run the AWS tag-based
orphan scan and write its result. The post-teardown AWS scan is read-only; it
classifies resources and does not delete them. EC2, EBS gp3, ELB, VPC, and data
transfer charges continue until cleanup completes; use a short-lived account or
budget alarm for exploratory runs.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| CAPA reconciles in the wrong account or returns access denied | Inspect `AWSCluster.spec.identityRef`; select the correct static/role identity with `AWS_IDENTITY_KIND` and `AWS_IDENTITY_NAME`. |
| `MissingParameter: ImageId` or empty key name | Confirm `AWS_AMI_ID` and `AWS_SSH_KEY_NAME`; `mage e2e:aws` validates both before provisioning. |
| Pods cannot pull the driver | Confirm `E2E_DRIVER_IMAGE` is an ECR image reachable by node IAM and that the generated ECR pull secret succeeds. |
| Nodes remain `NotReady` | Check the CAPA controller, AWS CCM permission in section 4b, and the CCM logs for `DescribeAvailabilityZones` authorization. |
| Reusing a run targets an unexpected cluster | Inspect/remove `test/e2e/_artifacts/e2e-run.json`, then choose a deliberate new `E2E_RUN_ID`. |

## Fail-fast contract

The suite checks for the `aws` provider and the required variables before
applying the cluster template. If any are missing it fails immediately with a
message naming the missing prerequisite — it does **not** silently fall back to
the KubeVirt lane or skip the run. This prevents half-configured runs that leak
AWS resources.

## Storage conformance (opt-in)

After the two hand-written smokes (NFS RWX cross-node, NVMe-TCP RWO), an
**optional** third spec runs the upstream Kubernetes external-storage
conformance suite against the deployed driver. It is opt-in because it is long
(~30–60m).

Enable it by setting `E2E_RUN_CONFORMANCE=1`:

```bash
# Reuse a standing cluster (recommended — conformance is long) and run it.
E2E_RUN_ID=aws-dev E2E_SKIP_CLEANUP=true E2E_RUN_CONFORMANCE=1 \
  ZFS_CSI_IMAGE_REPO=<ecr>/zfs-csi ZFS_CSI_IMAGE_TAG=dev \
  AWS_SSH_KEY_NAME=zfs-csi-e2e \
  E2E_SSH_PRIVATE_KEY_PATH="$HOME/.ssh/zfs-csi-e2e.pem" AWS_AMI_ID=<ami> \
  mage e2e:aws
```

How it works: the spec launches the version-matched
`registry.k8s.io/conformance:v<k8s>` image as a **host** Docker container on the
workstation (mirroring the Cluster API `kubetest` runner), mounts the workload
kubeconfig plus the two testdriver manifests
(`test/e2e/data/testdriver/zfs-csi-{nvme,nfs}.yaml`), and runs
`ginkgo -focus='External.Storage.*co\.uk'`. JUnit + e2e output land under
`test/e2e/_artifacts/conformance/<cluster>/`.

Overrides (all optional):

| Env | Effect |
| --- | --- |
| `E2E_RUN_CONFORMANCE=1` | Enable the conformance spec (default: skipped). |
| `E2E_CONFORMANCE_FOCUS` | Override the ginkgo `-focus` regex. |
| `E2E_CONFORMANCE_SKIP` | Override the ginkgo `-skip` regex (default is empty; focused storage edge cases remain included). |
| `E2E_CONFORMANCE_IMAGE` | Override the conformance image (air-gapped / mirror registries). |

The two testdrivers use distinct `DriverInfo.Name` values (`…co.uk-nvme` /
`…co.uk-nfs`) so their results are unambiguous in the JUnit output, and they
reference the chart-installed StorageClasses via `FromExistingClassName` (so the
pool/CIDR parameters are preserved). Snapshot conformance is not yet enabled —
the snapshotter sidecar + VolumeSnapshotClass are a future addition.

Requirements: a Linux workstation with Docker (the `host` network mode is
Linux-only), and no `HTTP(S)_PROXY` env set (the container runtime would try to
inspect the `host` network's subnets and fail).
