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
	"strings"
	"testing"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestTLSNFSVolumeIdentityUsesDatasetMountpoint(t *testing.T) {
	volume := &zfscsiv1.Volume{
		Spec: zfscsiv1.VolumeSpec{NFSTLSEnabled: true},
		Status: zfscsiv1.VolumeStatus{
			NFSServer:   "192.0.2.10",
			DatasetPath: "tank/csi/fs/example",
		},
	}
	got, err := tlsNFSVolumeIdentityFromVolume(volume)
	if err != nil {
		t.Fatalf("TLS NFS identity: %v", err)
	}
	if got.NFSServer != "192.0.2.10" || got.ExportPath != "/tank/csi/fs/example" {
		t.Fatalf("TLS NFS identity = %#v", got)
	}
}

func TestPodCertificateAcceptancePVCNameIsDNSLabelAndRunScoped(t *testing.T) {
	name := podCertificateAcceptancePVCName("Run/2026_07_22 with unusual characters")
	if len(name) > 63 || name == "" {
		t.Fatalf("PVC name length = %d, name=%q", len(name), name)
	}
	for _, r := range name {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-') {
			t.Fatalf("PVC name contains invalid DNS label character %q: %q", r, name)
		}
	}
	if name != podCertificateAcceptancePVCName("Run/2026_07_22 with unusual characters") {
		t.Fatal("PVC name is not deterministic")
	}
	if name == podCertificateAcceptancePVCName("different-run") {
		t.Fatal("PVC names are not run-scoped")
	}
}

func TestPodCertificateAcceptanceProbeNameIsDeterministicDNSLabelAndRunScoped(t *testing.T) {
	runID := "Run/2026_07_22 with an exceptionally long identifier that must be bounded safely"
	name := podCertificateAcceptanceProbeName(runID)
	if len(name) > 63 || name == "" {
		t.Fatalf("probe name length = %d, name=%q", len(name), name)
	}
	if name != podCertificateAcceptanceProbeName(runID) {
		t.Fatal("probe name is not deterministic")
	}
	if name == podCertificateAcceptanceProbeName("different-run") {
		t.Fatal("probe names are not isolated by run ID")
	}
	for _, r := range name {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-') {
			t.Fatalf("probe name contains invalid DNS label character %q: %q", r, name)
		}
	}
}

func TestPodCertificateAcceptanceBootstrapProviderBranches(t *testing.T) {
	bootstrap, err := podCertificateAcceptanceBootstrap("aws", nil)
	if err != nil || !bootstrap {
		t.Fatalf("AWS empty-owner bootstrap = (%t, %v), want (true, nil)", bootstrap, err)
	}
	bootstrap, err = podCertificateAcceptanceBootstrap("aws", []storageOwner{{}})
	if err != nil || bootstrap {
		t.Fatalf("AWS initialized-owner bootstrap = (%t, %v), want (false, nil)", bootstrap, err)
	}
	bootstrap, err = podCertificateAcceptanceBootstrap("static", []storageOwner{{}})
	if err != nil || bootstrap {
		t.Fatalf("static initialized-owner bootstrap = (%t, %v), want (false, nil)", bootstrap, err)
	}
	if _, err := podCertificateAcceptanceBootstrap("static", nil); err == nil {
		t.Fatal("static empty-owner acceptance must reject standalone execution")
	}
}

func TestNFSMTLSPeerProbePodReusesDirectProjection(t *testing.T) {
	source := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "zfs-csi-node-source", Namespace: zfsCSINamespace},
		Spec: corev1.PodSpec{
			NodeName: "node-a",
			Containers: []corev1.Container{{
				Name:  "tlshd",
				Image: "registry.example/zfs-csi:test",
			}},
			Volumes: []corev1.Volume{{
				Name: "tls-client",
				VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
					DefaultMode: int32Ptr(0o400),
					Sources: []corev1.VolumeProjection{{PodCertificate: &corev1.PodCertificateProjection{
						SignerName:           podCertificateSignerName,
						KeyType:              "ECDSAP256",
						CertificateChainPath: "tls.crt",
						KeyPath:              "tls.key",
					}}},
				}},
			}},
		},
	}

	probe := nfsMTLSPeerProbePod(zfsCSINamespace, "probe", source, "192.0.2.10", "/tank/csi/fs/vol")
	if probe.Spec.ServiceAccountName != podCertificateServiceAccount {
		t.Fatalf("service account = %q", probe.Spec.ServiceAccountName)
	}
	if probe.Spec.NodeName != source.Spec.NodeName {
		t.Fatalf("node = %q, want %q", probe.Spec.NodeName, source.Spec.NodeName)
	}
	if probe.Spec.HostNetwork {
		t.Fatal("probe must not share host network namespace")
	}
	if probe.Spec.HostUsers == nil || !*probe.Spec.HostUsers {
		t.Fatal("probe must use host user namespace so mount privileges reach the node kernel")
	}
	if len(probe.Spec.Volumes) == 0 || probe.Spec.Volumes[0].Projected == nil {
		t.Fatal("probe lacks direct PodCertificate projection")
	}
	projection := probe.Spec.Volumes[0].Projected.Sources[0].PodCertificate
	if projection == nil || projection.SignerName != podCertificateSignerName {
		t.Fatalf("projection = %#v", projection)
	}
	if probe.Spec.Volumes[0].Projected == source.Spec.Volumes[0].Projected {
		t.Fatal("probe projection aliases source pod projection")
	}
	mount := probe.Spec.Containers[0].VolumeMounts[0]
	if mount.Name != "tls-client" || mount.MountPath != "/run/zfs-csi-tls" || !mount.ReadOnly {
		t.Fatalf("projected credential mount = %#v", mount)
	}
	for _, path := range []string{
		"x509.truststore= /run/zfs-csi-tls/ca.crt",
		"x509.certificate= /run/zfs-csi-tls/tls.crt",
		"x509.private_key= /run/zfs-csi-tls/tls.key",
	} {
		if !strings.Contains(nfsMTLSPeerProbeScript, path) {
			t.Fatalf("peer probe script lacks %q", path)
		}
	}
}

func int32Ptr(value int32) *int32 { return &value }
