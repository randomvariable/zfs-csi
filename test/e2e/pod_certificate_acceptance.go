// Copyright 2026 Naadir Jeewa
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/tlsca"
	"github.com/randomvariable/zfs-csi/test/e2e/e2econfig"

	certificatesv1beta1 "k8s.io/api/certificates/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	podCertificateSignerName     = "zfs.csi.randomvariable.co.uk/nfs-client"
	podCertificateServiceAccount = "zfs-csi-node"
	podCertificateRotationBudget = 70 * time.Minute
	podCertificatePollInterval   = 10 * time.Second
)

type podCertificateEvidence struct {
	RecordedAt                string   `json:"recorded_at"`
	SignerName                string   `json:"signer_name"`
	NodePod                   string   `json:"node_pod"`
	NodeName                  string   `json:"node_name"`
	CertificateSerial         string   `json:"certificate_serial"`
	CertificateNotBefore      string   `json:"certificate_not_before"`
	CertificateNotAfter       string   `json:"certificate_not_after"`
	PCRName                   string   `json:"pcr_name"`
	PCRResourceVersion        string   `json:"pcr_resource_version"`
	PCRBeginRefreshAt         string   `json:"pcr_begin_refresh_at"`
	RotatedPCRName            string   `json:"rotated_pcr_name"`
	RotatedPCRResourceVersion string   `json:"rotated_pcr_resource_version"`
	RotatedCertificateSerial  string   `json:"rotated_certificate_serial"`
	ServerTrustPath           string   `json:"server_trust_path"`
	ServerTrustSHA256         string   `json:"server_trust_sha256"`
	NFSServer                 string   `json:"nfs_server"`
	NFSExportPath             string   `json:"nfs_export_path"`
	NoCertificateResult       string   `json:"no_certificate_result"`
	ForeignCAResult           string   `json:"foreign_ca_result"`
	SharedCAResult            string   `json:"shared_ca_result"`
	Checks                    []string `json:"checks"`
	Complete                  bool     `json:"complete"`
	Pending                   []string `json:"pending,omitempty"`
}

type issuedPodCertificate struct {
	Name            string
	ResourceVersion string
	Certificate     *x509.Certificate
	BeginRefreshAt  time.Time
}

// runPodCertificateAcceptance gathers bounded, non-secret evidence from a real
// v1.36 kubelet and signer. Existing NFS TLS smoke proves shared-CA acceptance;
// this supplements it with PCR issuance/rotation and server trust-path evidence.
func runPodCertificateAcceptance(ctx context.Context, c client.Client, kubeconfig, artifactDir string, storage storageNode) error {
	runID := e2econfig.RunID()
	acceptancePVC := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: "default",
		Name:      podCertificateAcceptancePVCName(runID),
	}}
	defer func() {
		_ = deleteOwnedObject(context.WithoutCancel(ctx), c, acceptancePVC, e2eOwnershipLabels(runID))
	}()
	identity, err := tlsSmokeVolumeIdentity(ctx, c, runID)
	if err != nil {
		return err
	}
	nodePod, err := nodePodForConsumer(ctx, c)
	if err != nil {
		return err
	}
	if err := assertPodCertificateProjection(nodePod); err != nil {
		return err
	}
	first, err := waitForIssuedPodCertificate(ctx, c, nodePod, nil, 5*time.Minute)
	if err != nil {
		return err
	}
	clientTrustHash, err := assertNodeTLSHDMaterial(ctx, kubeconfig, nodePod)
	if err != nil {
		return err
	}
	if err := assertCertificateMatchesProjectedChain(ctx, kubeconfig, nodePod, first.Certificate); err != nil {
		return err
	}

	serverPod, err := storageAgentPodForNode(ctx, c, storage.Name)
	if err != nil {
		return err
	}
	trustHash, err := assertServerTLSHDTrust(ctx, kubeconfig, serverPod)
	if err != nil {
		return err
	}
	if clientTrustHash != trustHash {
		return fmt.Errorf("node and server tlshd trust different public CA files: client=%s server=%s", clientTrustHash, trustHash)
	}

	if err := assertServerRequiresMTLS(ctx, kubeconfig, serverPod, identity.ExportPath); err != nil {
		return err
	}
	foreignCA, err := tlsca.NewCA("zfs-csi E2E foreign NFS TLS CA")
	if err != nil {
		return fmt.Errorf("generate isolated foreign NFS TLS CA: %w", err)
	}
	foreignKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("generate isolated foreign client key: %w", err)
	}
	foreignCert, _, err := foreignCA.SignClientCertificate(nodePod.Spec.NodeName, &foreignKey.PublicKey, time.Now().UTC().Add(-time.Minute), time.Hour)
	if err != nil {
		return fmt.Errorf("sign isolated foreign client certificate: %w", err)
	}
	foreignKeyDER, err := x509.MarshalECPrivateKey(foreignKey)
	if err != nil {
		return fmt.Errorf("encode isolated foreign client key: %w", err)
	}
	probeResults, err := runNFSMTLSPeerProbes(
		ctx, c, kubeconfig, nodePod, runID, identity.NFSServer, identity.ExportPath,
		foreignCA.CertPEM, append(foreignCert, foreignCA.CertPEM...),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: foreignKeyDER}),
	)
	if err != nil {
		return err
	}

	// Successful CSI RWX smoke against zfs-tank-nfs-tls immediately before this
	// spec proves the projected shared-CA client is accepted. Server xprtsec=mtls
	// and the exact CA truststore are asserted here. Together those checks prove
	// peers with no certificate or a certificate outside that CA are rejected
	// without injecting a foreign private key into shared privileged node state.
	evidence := podCertificateEvidence{
		RecordedAt:           time.Now().UTC().Format(time.RFC3339Nano),
		SignerName:           podCertificateSignerName,
		NodePod:              nodePod.Name,
		NodeName:             nodePod.Spec.NodeName,
		CertificateSerial:    first.Certificate.SerialNumber.String(),
		CertificateNotBefore: first.Certificate.NotBefore.UTC().Format(time.RFC3339Nano),
		CertificateNotAfter:  first.Certificate.NotAfter.UTC().Format(time.RFC3339Nano),
		PCRName:              first.Name,
		PCRResourceVersion:   first.ResourceVersion,
		PCRBeginRefreshAt:    first.BeginRefreshAt.UTC().Format(time.RFC3339Nano),
		ServerTrustPath:      "/run/zfs-csi-tls-ca/ca.crt",
		ServerTrustSHA256:    trustHash,
		NFSServer:            identity.NFSServer,
		NFSExportPath:        identity.ExportPath,
		NoCertificateResult:  probeResults.NoCertificate,
		ForeignCAResult:      probeResults.ForeignCA,
		SharedCAResult:       probeResults.SharedCA,
		Checks: []string{
			"node projected key and leaf-to-root certificate chain",
			"PCR Issued status matches projected leaf",
			"no-certificate peer mount rejected by live server",
			"server tlshd config uses signer public CA trust path",
			"foreign-CA peer mount rejected by live server",
			"projected shared-CA peer mount accepted by live server",
		},
		Pending: []string{"natural kubelet PodCertificate rotation"},
	}
	if err := writePodCertificateEvidence(artifactDir, evidence); err != nil {
		return err
	}

	rotationCtx, cancel := context.WithTimeout(ctx, podCertificateRotationBudget)
	defer cancel()
	rotated, err := waitForIssuedPodCertificate(rotationCtx, c, nodePod, first.Certificate.SerialNumber.Bytes(), podCertificateRotationBudget)
	if err != nil {
		return fmt.Errorf("wait for natural kubelet PodCertificate rotation (budget %s): %w", podCertificateRotationBudget, err)
	}
	evidence.RotatedPCRName = rotated.Name
	evidence.RotatedPCRResourceVersion = rotated.ResourceVersion
	evidence.RotatedCertificateSerial = rotated.Certificate.SerialNumber.String()
	evidence.Checks = append(evidence.Checks, "kubelet refresh produced a different signer-issued certificate")
	evidence.Pending = nil
	evidence.Complete = true
	return writePodCertificateEvidence(artifactDir, evidence)
}

type nfsMTLSProbeResults struct {
	NoCertificate string
	ForeignCA     string
	SharedCA      string
}

type tlsNFSVolumeIdentity struct {
	NFSServer  string
	ExportPath string
}

func tlsSmokeVolumeIdentity(ctx context.Context, c client.Client, runID string) (tlsNFSVolumeIdentity, error) {
	pvc := &corev1.PersistentVolumeClaim{}
	name := podCertificateAcceptancePVCName(runID)
	key := client.ObjectKey{Namespace: "default", Name: name}
	ownership := e2eOwnershipLabels(runID)
	if err := c.Get(ctx, key, pvc); err != nil {
		if !apierrors.IsNotFound(err) {
			return tlsNFSVolumeIdentity{}, fmt.Errorf("get TLS smoke PVC: %w", err)
		}
		storageClass, err := smokeStorageClassName("zfs-tank-nfs-tls")
		if err != nil {
			return tlsNFSVolumeIdentity{}, err
		}
		pvc = pvcObject("default", name, storageClass, corev1.ReadWriteMany)
		setObjectLabels(pvc, ownership)
		if err := c.Create(ctx, pvc); err != nil {
			return tlsNFSVolumeIdentity{}, fmt.Errorf("create TLS acceptance PVC: %w", err)
		}
	}
	for key, value := range ownership {
		if pvc.Labels[key] != value {
			return tlsNFSVolumeIdentity{}, fmt.Errorf("refusing to adopt foreign TLS acceptance PVC %s/%s: label %s=%q, want %q", pvc.Namespace, pvc.Name, key, pvc.Labels[key], value)
		}
	}
	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
		if err := waitForTLSAcceptancePVCBound(ctx, c, key, 10*time.Minute); err != nil {
			return tlsNFSVolumeIdentity{}, fmt.Errorf("bind TLS acceptance PVC: %w", err)
		}
		if err := c.Get(ctx, key, pvc); err != nil {
			return tlsNFSVolumeIdentity{}, fmt.Errorf("refresh TLS acceptance PVC: %w", err)
		}
	}
	pv := &corev1.PersistentVolume{}
	if err := c.Get(ctx, client.ObjectKey{Name: pvc.Spec.VolumeName}, pv); err != nil {
		return tlsNFSVolumeIdentity{}, fmt.Errorf("get TLS smoke PV %s: %w", pvc.Spec.VolumeName, err)
	}
	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != zfsCSIDriverName || pv.Spec.CSI.VolumeHandle == "" {
		return tlsNFSVolumeIdentity{}, fmt.Errorf("TLS smoke PV %s lacks zfs-csi volume identity", pv.Name)
	}
	crName, err := volumeCRName(pv.Spec.CSI.VolumeHandle)
	if err != nil {
		return tlsNFSVolumeIdentity{}, err
	}
	volume := &zfscsiv1.Volume{}
	if err := c.Get(ctx, client.ObjectKey{Name: crName}, volume); err != nil {
		return tlsNFSVolumeIdentity{}, fmt.Errorf("get TLS smoke Volume %s: %w", crName, err)
	}
	identity, err := tlsNFSVolumeIdentityFromVolume(volume)
	if err != nil {
		return tlsNFSVolumeIdentity{}, fmt.Errorf("TLS smoke Volume %s lacks ready NFS mTLS endpoint: %w", crName, err)
	}
	return identity, nil
}

func tlsNFSVolumeIdentityFromVolume(volume *zfscsiv1.Volume) (tlsNFSVolumeIdentity, error) {
	if !volume.Spec.NFSTLSEnabled || volume.Status.NFSServer == "" || volume.Status.DatasetPath == "" {
		return tlsNFSVolumeIdentity{}, fmt.Errorf("TLS intent, server, or dataset path is missing")
	}
	return tlsNFSVolumeIdentity{NFSServer: volume.Status.NFSServer, ExportPath: "/" + strings.TrimPrefix(volume.Status.DatasetPath, "/")}, nil
}

func waitForTLSAcceptancePVCBound(ctx context.Context, c client.Client, key client.ObjectKey, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := c.Get(ctx, key, pvc); err != nil {
			return err
		}
		if pvc.Status.Phase == corev1.ClaimBound && pvc.Spec.VolumeName != "" {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return fmt.Errorf("PVC %s/%s did not bind within %s", key.Namespace, key.Name, timeout)
}

func runNFSMTLSPeerProbes(
	ctx context.Context,
	c client.Client,
	kubeconfig string,
	nodePod *corev1.Pod,
	runID string,
	nfsServer, exportPath string,
	foreignCA, foreignCert, foreignKey []byte,
) (nfsMTLSProbeResults, error) {
	const namespace = zfsCSINamespace
	name := podCertificateAcceptanceProbeName(runID)
	ownership := e2eOwnershipLabels(runID)
	probePod := nfsMTLSPeerProbePod(namespace, name, nodePod, nfsServer, exportPath)
	if probePod == nil {
		return nfsMTLSProbeResults{}, fmt.Errorf("source node pod lacks tlshd image or direct PodCertificate projection")
	}
	setObjectLabels(probePod, ownership)
	if err := deleteOwnedObject(ctx, c, probePod, ownership); err != nil {
		return nfsMTLSProbeResults{}, fmt.Errorf("reset owned NFS mTLS peer probe: %w", err)
	}
	if err := waitForPodDeleted(ctx, c, client.ObjectKey{Namespace: namespace, Name: name}, time.Minute); err != nil {
		return nfsMTLSProbeResults{}, err
	}
	if err := c.Create(ctx, probePod); err != nil {
		return nfsMTLSProbeResults{}, fmt.Errorf("create NFS mTLS peer probe: %w", err)
	}
	defer func() { _ = deleteOwnedObject(context.WithoutCancel(ctx), c, probePod, ownership) }()
	if err := waitForPodRunningReady(ctx, c, client.ObjectKey{Namespace: namespace, Name: name}, 3*time.Minute); err != nil {
		return nfsMTLSProbeResults{}, err
	}

	for path, body := range map[string][]byte{
		"/tmp/foreign/ca.crt":  foreignCA,
		"/tmp/foreign/tls.crt": foreignCert,
		"/tmp/foreign/tls.key": foreignKey,
	} {
		command := "umask 077; mkdir -p /tmp/foreign; cat > " + shellQuote(path)
		if _, err := kubectlExecInput(ctx, kubeconfig, namespace, name, "probe", command, body); err != nil {
			return nfsMTLSProbeResults{}, fmt.Errorf("stage foreign probe material %s: %w", path, err)
		}
	}

	out, err := kubectlExecOutput(ctx, kubeconfig, namespace, name, "probe", nfsMTLSPeerProbeScript)
	if err != nil {
		return nfsMTLSProbeResults{}, fmt.Errorf("run live NFS mTLS peer matrix: %w", err)
	}
	text := string(out)
	results := nfsMTLSProbeResults{}
	for _, line := range strings.Split(text, "\n") {
		key, value, ok := strings.Cut(strings.TrimSpace(line), "=")
		if !ok {
			continue
		}
		switch key {
		case "no_certificate":
			results.NoCertificate = value
		case "foreign_ca":
			results.ForeignCA = value
		case "shared_ca":
			results.SharedCA = value
		}
	}
	if results.NoCertificate != "rejected" || results.ForeignCA != "rejected" || results.SharedCA != "accepted" {
		return nfsMTLSProbeResults{}, fmt.Errorf("incomplete NFS mTLS peer matrix output: %q", text)
	}
	return results, nil
}

func podCertificateAcceptancePVCName(runID string) string {
	return podCertificateAcceptanceResourceName("zfs-csi-pcr", runID)
}

func podCertificateAcceptanceProbeName(runID string) string {
	return podCertificateAcceptanceResourceName("zfs-csi-pcr-probe", runID)
}

func podCertificateAcceptanceResourceName(prefix, runID string) string {
	clean := strings.ToLower(runID)
	var b strings.Builder
	for _, r := range clean {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	clean = strings.Trim(b.String(), "-")
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(runID)))[:10]
	name := prefix + "-" + clean + "-" + hash
	if len(name) > 63 {
		name = strings.TrimRight(name[:63], "-")
	}
	return name
}

func nfsMTLSPeerProbePod(namespace, name string, nodePod *corev1.Pod, nfsServer, exportPath string) *corev1.Pod {
	var projected *corev1.ProjectedVolumeSource
	for _, volume := range nodePod.Spec.Volumes {
		if volume.Name == "tls-client" && volume.Projected != nil {
			projected = volume.Projected.DeepCopy()
			break
		}
	}
	image := ""
	for _, container := range nodePod.Spec.Containers {
		if container.Name == "tlshd" {
			image = container.Image
			break
		}
	}
	if projected == nil || image == "" {
		return nil
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.PodSpec{
			ServiceAccountName: podCertificateServiceAccount,
			NodeName:           nodePod.Spec.NodeName,
			HostUsers:          boolPtr(true),
			RestartPolicy:      corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:    "probe",
				Image:   image,
				Command: []string{"sh", "-ceu", "trap : TERM INT; sleep infinity & wait"},
				Env: []corev1.EnvVar{
					{Name: "NFS_SERVER", Value: nfsServer},
					{Name: "NFS_EXPORT", Value: exportPath},
				},
				SecurityContext: &corev1.SecurityContext{Privileged: boolPtr(true)},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "tls-client", MountPath: "/run/zfs-csi-tls", ReadOnly: true},
					{Name: "run-zfs", MountPath: "/run/zfs-csi"},
					{Name: "dev", MountPath: "/dev"},
					{Name: "sys", MountPath: "/sys"},
				},
			}},
			Volumes: []corev1.Volume{
				{Name: "tls-client", VolumeSource: corev1.VolumeSource{Projected: projected}},
				{Name: "run-zfs", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/run/zfs-csi", Type: ptr.To(corev1.HostPathDirectoryOrCreate)}}},
				{Name: "dev", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/dev"}}},
				{Name: "sys", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/sys"}}},
			},
		},
	}
}

func waitForPodRunningReady(ctx context.Context, c client.Client, key client.ObjectKey, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		pod := &corev1.Pod{}
		if err := c.Get(ctx, key, pod); err == nil {
			if pod.Status.Phase == corev1.PodFailed {
				return fmt.Errorf("probe pod %s/%s failed: %s", key.Namespace, key.Name, pod.Status.Message)
			}
			for _, condition := range pod.Status.Conditions {
				if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
					return nil
				}
			}
		} else if !apierrors.IsNotFound(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("timed out waiting for probe pod %s/%s", key.Namespace, key.Name)
		case <-ticker.C:
		}
	}
}

func waitForPodDeleted(ctx context.Context, c client.Client, key client.ObjectKey, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := c.Get(ctx, key, &corev1.Pod{}); apierrors.IsNotFound(err) {
			return nil
		} else if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("timed out waiting for old probe pod %s/%s deletion", key.Namespace, key.Name)
		case <-ticker.C:
		}
	}
}

func kubectlExecInput(ctx context.Context, kubeconfig, namespace, pod, container, command string, input []byte) ([]byte, error) {
	args := []string{"--kubeconfig", kubeconfig, "-n", namespace, "exec", "-i", pod, "-c", container, "--", "sh", "-ceu", command}
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	cmd.Stdin = bytes.NewReader(input)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("kubectl exec %s/%s[%s]: %w\n%s", namespace, pod, container, err, out)
	}
	return out, nil
}

const nfsMTLSPeerProbeScript = `set -eu
expect_rejected() {
  name="$1"
  config="$2"
  rm -rf /tmp/probe-mount
  mkdir -p /tmp/probe-mount
  rm -f /tmp/tlshd.log
  /usr/sbin/tlshd -s -c "$config" >/tmp/tlshd.log 2>&1 &
  pid=$!
  sleep 1
  if timeout 30s mount -t nfs4 -o vers=4.2,xprtsec=mtls "$NFS_SERVER:$NFS_EXPORT" /tmp/probe-mount; then
    umount /tmp/probe-mount || true
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    echo "$name unexpectedly accepted" >&2
    cat /tmp/tlshd.log >&2 || true
    return 1
  fi
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  printf '%s=rejected\n' "$name"
}
run_shared() {
  rm -rf /tmp/probe-mount
  mkdir -p /tmp/probe-mount
  rm -f /tmp/tlshd.log
  /usr/sbin/tlshd -s -c /tmp/shared.conf >/tmp/tlshd.log 2>&1 &
  pid=$!
  sleep 1
  timeout 30s mount -t nfs4 -o vers=4.2,xprtsec=mtls "$NFS_SERVER:$NFS_EXPORT" /tmp/probe-mount
  test -d /tmp/probe-mount
  umount /tmp/probe-mount
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  printf 'shared_ca=accepted\n'
}
cat >/tmp/no-cert.conf <<'EOF'
[debug]
loglevel=2
tls=0
nl=0
[authenticate]
[authenticate.client]
x509.truststore= /run/zfs-csi-tls/ca.crt
EOF
cat >/tmp/foreign.conf <<'EOF'
[debug]
loglevel=2
tls=0
nl=0
[authenticate]
[authenticate.client]
x509.truststore= /run/zfs-csi-tls/ca.crt
x509.certificate= /tmp/foreign/tls.crt
x509.private_key= /tmp/foreign/tls.key
EOF
cat >/tmp/shared.conf <<'EOF'
[debug]
loglevel=2
tls=0
nl=0
[authenticate]
[authenticate.client]
x509.truststore= /run/zfs-csi-tls/ca.crt
x509.certificate= /run/zfs-csi-tls/tls.crt
x509.private_key= /run/zfs-csi-tls/tls.key
EOF
expect_rejected no_certificate /tmp/no-cert.conf
expect_rejected foreign_ca /tmp/foreign.conf
run_shared`

func assertCertificateMatchesProjectedChain(ctx context.Context, kubeconfig string, pod *corev1.Pod, issued *x509.Certificate) error {
	out, err := kubectlExecOutput(ctx, kubeconfig, pod.Namespace, pod.Name, "tlshd", "cat /run/zfs-csi-tls/tls.crt")
	if err != nil {
		return fmt.Errorf("read projected certificate chain: %w", err)
	}
	block, remainder := pem.Decode(out)
	if block == nil || block.Type != "CERTIFICATE" {
		return fmt.Errorf("projected certificate chain has no PEM leaf")
	}
	projected, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("parse projected leaf: %w", err)
	}
	if projected.SerialNumber.Cmp(issued.SerialNumber) != 0 {
		return fmt.Errorf("projected leaf serial %s does not match Issued PCR serial %s", projected.SerialNumber, issued.SerialNumber)
	}
	if next, _ := pem.Decode(remainder); next == nil || next.Type != "CERTIFICATE" {
		return fmt.Errorf("projected certificate chain lacks signer CA after leaf")
	}
	return nil
}

func assertNodeTLSHDMaterial(ctx context.Context, kubeconfig string, pod *corev1.Pod) (string, error) {
	command := `set -eu
grep -F 'x509.certificate= /run/zfs-csi-tls/tls.crt' /etc/tlshd/config >/dev/null
grep -F 'x509.private_key= /run/zfs-csi-tls/tls.key' /etc/tlshd/config >/dev/null
grep -F 'x509.truststore= /run/zfs-csi-tls/ca.crt' /etc/tlshd/config >/dev/null
test -s /run/zfs-csi-tls/tls.crt
test -s /run/zfs-csi-tls/tls.key
test -s /run/zfs-csi-tls/ca.crt
sha256sum /run/zfs-csi-tls/ca.crt | awk '{print $1}'`
	out, err := kubectlExecOutput(ctx, kubeconfig, pod.Namespace, pod.Name, "tlshd", command)
	if err != nil {
		return "", fmt.Errorf("prove node tlshd projected credential and trust paths: %w", err)
	}
	hash := strings.TrimSpace(string(out))
	if len(hash) != 64 {
		return "", fmt.Errorf("node CA sha256 output malformed: %q", hash)
	}
	return hash, nil
}

func assertServerRequiresMTLS(ctx context.Context, kubeconfig string, pod *corev1.Pod, exportPath string) error {
	command := `set -eu
found=false
for export in /proc/fs/nfsd/exports /var/lib/nfs/etab; do
  if test -r "$export" && grep -F "$NFS_EXPORT" "$export" | grep -F 'xprtsec=mtls' >/dev/null; then
    found=true
    break
  fi
done
test "$found" = true`
	command = "NFS_EXPORT=" + shellQuote(exportPath) + "\n" + command
	if _, err := kubectlExecOutput(ctx, kubeconfig, pod.Namespace, pod.Name, "storage", command); err != nil {
		return fmt.Errorf("prove no-certificate NFS peer is rejected by xprtsec=mtls: %w", err)
	}
	return nil
}

func nodePodForConsumer(ctx context.Context, c client.Client) (*corev1.Pod, error) {
	list := &corev1.PodList{}
	if err := c.List(ctx, list, client.InNamespace(zfsCSINamespace), client.MatchingLabels{
		"app.kubernetes.io/name":      "zfs-csi",
		"app.kubernetes.io/component": "node",
	}); err != nil {
		return nil, fmt.Errorf("list node pods: %w", err)
	}
	sort.Slice(list.Items, func(i, j int) bool { return list.Items[i].Name < list.Items[j].Name })
	for i := range list.Items {
		pod := &list.Items[i]
		if pod.DeletionTimestamp == nil && pod.Status.Phase == corev1.PodRunning && pod.Spec.NodeName != "" {
			return pod.DeepCopy(), nil
		}
	}
	return nil, fmt.Errorf("no running zfs-csi node pod found")
}

func assertPodCertificateProjection(pod *corev1.Pod) error {
	if pod.Spec.ServiceAccountName != podCertificateServiceAccount {
		return fmt.Errorf("node pod service account = %q, want %q", pod.Spec.ServiceAccountName, podCertificateServiceAccount)
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.Name != "tls-client" || volume.Projected == nil {
			continue
		}
		for _, source := range volume.Projected.Sources {
			if source.PodCertificate == nil {
				continue
			}
			projection := source.PodCertificate
			if projection.SignerName != podCertificateSignerName || projection.CertificateChainPath != "tls.crt" || projection.KeyPath != "tls.key" {
				return fmt.Errorf("node PodCertificate projection does not match signer/path contract: %#v", projection)
			}
			for _, container := range pod.Spec.Containers {
				if container.Name != "tlshd" {
					continue
				}
				for _, mount := range container.VolumeMounts {
					if mount.Name == "tls-client" && mount.MountPath == "/run/zfs-csi-tls" && mount.ReadOnly {
						return nil
					}
				}
				return fmt.Errorf("node tlshd does not mount tls-client read-only at /run/zfs-csi-tls")
			}
			return fmt.Errorf("node pod %s lacks tlshd container", pod.Name)
		}
	}
	return fmt.Errorf("node pod %s lacks tls-client PodCertificate projection", pod.Name)
}

func waitForIssuedPodCertificate(ctx context.Context, c client.Client, pod *corev1.Pod, previousSerial []byte, timeout time.Duration) (*issuedPodCertificate, error) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(podCertificatePollInterval)
	defer ticker.Stop()
	for {
		issued, err := issuedPodCertificateForPod(ctx, c, pod)
		if err == nil && issued != nil && !bytes.Equal(issued.Certificate.SerialNumber.Bytes(), previousSerial) {
			return issued, nil
		}
		if err != nil && !apierrors.IsNotFound(err) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, fmt.Errorf("no new Issued PCR for pod UID %s", pod.UID)
		case <-ticker.C:
		}
	}
}

func issuedPodCertificateForPod(ctx context.Context, c client.Client, pod *corev1.Pod) (*issuedPodCertificate, error) {
	list := &certificatesv1beta1.PodCertificateRequestList{}
	if err := c.List(ctx, list, client.InNamespace(pod.Namespace)); err != nil {
		return nil, fmt.Errorf("list PodCertificateRequests: %w", err)
	}
	var candidates []*certificatesv1beta1.PodCertificateRequest
	for i := range list.Items {
		pcr := &list.Items[i]
		if pcr.Spec.SignerName == podCertificateSignerName && pcr.Spec.PodUID == pod.UID && pcr.Spec.ServiceAccountName == podCertificateServiceAccount && string(pcr.Spec.NodeName) == pod.Spec.NodeName && pcrIssued(pcr) {
			candidates = append(candidates, pcr)
		}
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].CreationTimestamp.After(candidates[j].CreationTimestamp.Time)
	})
	pcr := candidates[0]
	block, _ := pem.Decode([]byte(pcr.Status.CertificateChain))
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("PCR %s has malformed certificateChain", pcr.Name)
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse PCR %s leaf: %w", pcr.Name, err)
	}
	if pcr.Status.BeginRefreshAt == nil {
		return nil, fmt.Errorf("PCR %s Issued without beginRefreshAt", pcr.Name)
	}
	return &issuedPodCertificate{Name: pcr.Name, ResourceVersion: pcr.ResourceVersion, Certificate: leaf, BeginRefreshAt: pcr.Status.BeginRefreshAt.Time}, nil
}

func pcrIssued(pcr *certificatesv1beta1.PodCertificateRequest) bool {
	for _, condition := range pcr.Status.Conditions {
		if condition.Type == certificatesv1beta1.PodCertificateRequestConditionTypeIssued && condition.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

func storageAgentPodForNode(ctx context.Context, c client.Client, nodeName string) (*corev1.Pod, error) {
	list := &corev1.PodList{}
	if err := c.List(ctx, list, client.InNamespace(zfsCSINamespace), client.MatchingLabels{"app.kubernetes.io/component": "storage"}); err != nil {
		return nil, fmt.Errorf("list storage pods: %w", err)
	}
	for i := range list.Items {
		pod := &list.Items[i]
		if pod.Spec.NodeName == nodeName && pod.Status.Phase == corev1.PodRunning {
			return pod.DeepCopy(), nil
		}
	}
	return nil, fmt.Errorf("no running storage pod found on %s", nodeName)
}

func assertServerTLSHDTrust(ctx context.Context, kubeconfig string, pod *corev1.Pod) (string, error) {
	command := `set -eu
grep -F 'x509.truststore= /run/zfs-csi-tls-ca/ca.crt' /etc/tlshd/config >/dev/null
test -s /run/zfs-csi-tls-ca/ca.crt
sha256sum /run/zfs-csi-tls-ca/ca.crt | awk '{print $1}'`
	out, err := kubectlExecOutput(ctx, kubeconfig, pod.Namespace, pod.Name, "tlshd", command)
	if err != nil {
		return "", fmt.Errorf("prove server tlshd public CA trust: %w", err)
	}
	hash := strings.TrimSpace(string(out))
	if len(hash) != 64 {
		return "", fmt.Errorf("server CA sha256 output malformed: %q", hash)
	}
	return hash, nil
}

func kubectlExecOutput(ctx context.Context, kubeconfig, namespace, pod, container, command string) ([]byte, error) {
	args := []string{"--kubeconfig", kubeconfig, "-n", namespace, "exec", pod, "-c", container, "--", "sh", "-ceu", command}
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("kubectl exec %s/%s[%s]: %w\n%s", namespace, pod, container, err, out)
	}
	return out, nil
}

func writePodCertificateEvidence(artifactDir string, evidence podCertificateEvidence) error {
	body, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal PodCertificate evidence: %w", err)
	}
	body = append(body, '\n')
	path := filepath.Join(artifactDir, "pod-certificate-nfs-mtls.json")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		return fmt.Errorf("write PodCertificate evidence %q: %w", path, err)
	}
	return nil
}
