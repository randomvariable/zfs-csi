// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

package stage

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/randomvariable/zfs-csi/internal/psk"
	stagepb "github.com/randomvariable/zfs-csi/internal/stagepb/stage"
	"github.com/randomvariable/zfs-csi/internal/transport"
)

type stageSecretReader struct {
	secret *corev1.Secret
	err    error
	keys   [][2]string
}

func (r *stageSecretReader) Get(_ context.Context, namespace, name string) (*corev1.Secret, error) {
	r.keys = append(r.keys, [2]string{namespace, name})
	if r.err != nil {
		return nil, r.err
	}
	return r.secret, nil
}

type stagePSKProvisioner struct {
	installs [][2]string
	revokes  [][2]string
	err      error
}

type recordingDetachClient struct {
	transport.Client
	detachErr error
	detached  bool
}

type recordingAttachClient struct {
	transport.Client
	attachErr error
}

func (c *recordingAttachClient) Attach(ctx context.Context, ref transport.TargetRef, initiatorID string) (string, error) {
	if c.attachErr != nil {
		return "", c.attachErr
	}
	return c.Client.Attach(ctx, ref, initiatorID)
}

func (c *recordingDetachClient) Detach(ctx context.Context, ref transport.TargetRef) error {
	c.detached = true
	if c.detachErr != nil {
		return c.detachErr
	}
	return c.Client.Detach(ctx, ref)
}

func (p *stagePSKProvisioner) Install(_ psk.Interchange, host, target string) error {
	p.installs = append(p.installs, [2]string{host, target})
	return p.err
}

func (p *stagePSKProvisioner) Revoke(_ psk.Interchange, host, target string) error {
	p.revokes = append(p.revokes, [2]string{host, target})
	return nil
}

func testTLSSecret(t *testing.T) *corev1.Secret {
	t.Helper()
	ic, err := psk.Generate(&testEntropy{}, psk.HMACSHA256)
	if err != nil {
		t.Fatal(err)
	}
	v, err := ic.Format()
	if err != nil {
		t.Fatal(err)
	}
	immutable := true
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "zfs-csi-nvme-psk-test"}, Type: corev1.SecretTypeOpaque, Immutable: &immutable, Data: map[string][]byte{"psk": []byte(v)}}
}

type testEntropy struct{ n byte }

func (r *testEntropy) Read(p []byte) (int, error) {
	for i := range p {
		r.n++
		p[i] = r.n
	}
	return len(p), nil
}

func tlsStageRequest() *stagepb.NodeStageRequest {
	return &stagepb.NodeStageRequest{StagingPath: "/staging/tls", FsType: "ext4", Source: &stagepb.NodeStageRequest_Nvme{Nvme: &stagepb.NVMeSource{TargetNqn: "nqn.test", Portal: "10.0.0.1:4421", NamespaceId: 1, DeviceGuid: "guid-tls", InitiatorId: "node-a", Tls: true, PskSecret: "zfs-csi-nvme-psk-test"}}}
}

func TestNVMeTLSInstallsPSKBeforeAttach(t *testing.T) {
	fake := transport.New()
	seedFakeTarget(t, fake, "nqn.test", "10.0.0.1:4421", "guid-tls", "node-a")
	reader := &stageSecretReader{secret: testTLSSecret(t)}
	provisioner := &stagePSKProvisioner{}
	p := &NVMeStagePlugin{Block: fake, Mount: newRecordingMount(), Log: logr.Discard(), NVMeTLSNamespace: "driver-ns", NVMeTLSSecrets: reader, NVMeTLSPSK: provisioner, BeforeAttach: func() {
		if len(provisioner.installs) != 1 {
			t.Fatal("attach before PSK install")
		}
	}}
	if _, err := p.NodeStage(t.Context(), tlsStageRequest()); err != nil {
		t.Fatalf("NodeStage: %v", err)
	}
	if got, want := reader.keys, [][2]string{{"driver-ns", "zfs-csi-nvme-psk-test"}}; len(got) != 1 || got[0] != want[0] {
		t.Fatalf("secret get = %#v", got)
	}
	if got, want := provisioner.installs[0], [2]string{transport.HostNQN("node-a"), "nqn.test"}; got != want {
		t.Fatalf("identity = %#v, want %#v", got, want)
	}
}

func TestNVMeTLSFailureDoesNotAttachOrLeakPSK(t *testing.T) {
	secret := testTLSSecret(t)
	raw := string(secret.Data["psk"])
	for _, tc := range []struct {
		name        string
		reader      *stageSecretReader
		provisioner *stagePSKProvisioner
	}{
		{"missing", &stageSecretReader{err: apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, "x")}, &stagePSKProvisioner{}},
		{"malformed", &stageSecretReader{secret: &corev1.Secret{Type: corev1.SecretTypeOpaque, Immutable: stageBoolPtr(true), Data: map[string][]byte{"psk": []byte("bad")}}}, &stagePSKProvisioner{}},
		{"install", &stageSecretReader{secret: secret}, &stagePSKProvisioner{err: errors.New("keyring failure")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := transport.New()
			p := &NVMeStagePlugin{Block: fake, Mount: newRecordingMount(), Log: logr.Discard(), NVMeTLSNamespace: "driver-ns", NVMeTLSSecrets: tc.reader, NVMeTLSPSK: tc.provisioner}
			_, err := p.NodeStage(t.Context(), tlsStageRequest())
			if err == nil {
				t.Fatal("NodeStage succeeded")
			}
			if strings.Contains(err.Error(), raw) {
				t.Fatal("PSK leaked in error")
			}
			if len(fake.Targets()) != 0 {
				t.Fatal("attach ran despite PSK failure")
			}
		})
	}
}

func stageBoolPtr(v bool) *bool { return &v }

func TestValidNVMeTLSPSKName(t *testing.T) {
	for _, tc := range []struct {
		name string
		want bool
	}{
		{"zfs-csi-nvme-psk-a", true},
		{"zfs-csi-nvme-psk-", false},
		{"zfs-csi-nvme-psk--a", false},
		{"zfs-csi-nvme-psk-a-", false},
		{"zfs-csi-nvme-psk-a space", false},
		{"zfs-csi-nvme-psk-a/b", false},
	} {
		if got := validNVMeTLSPSKName(tc.name); got != tc.want {
			t.Errorf("validNVMeTLSPSKName(%q) = %v, want %v", tc.name, got, tc.want)
		}
	}
	if validNVMeTLSPSKName("zfs-csi-nvme-psk-" + strings.Repeat("a", 237)) {
		t.Fatal("oversize Secret name accepted")
	}
}

func TestNVMeTLSAttachFailureRevokesInstalledPSK(t *testing.T) {
	secret := testTLSSecret(t)
	raw := string(secret.Data["psk"])
	provisioner := &stagePSKProvisioner{}
	attachErr := errors.New("connect timed out")
	p := &NVMeStagePlugin{
		Block:            &recordingAttachClient{Client: transport.New(), attachErr: attachErr},
		Mount:            newRecordingMount(),
		Log:              logr.Discard(),
		NVMeTLSNamespace: "driver-ns",
		NVMeTLSSecrets:   &stageSecretReader{secret: secret},
		NVMeTLSPSK:       provisioner,
	}
	_, err := p.NodeStage(t.Context(), tlsStageRequest())
	if err == nil || !strings.Contains(err.Error(), attachErr.Error()) {
		t.Fatalf("NodeStage error = %v, want original attach error", err)
	}
	if strings.Contains(err.Error(), raw) {
		t.Fatal("PSK leaked in attach error")
	}
	if len(provisioner.installs) != 1 || len(provisioner.revokes) != 1 {
		t.Fatalf("PSK lifecycle = installs=%d revokes=%d", len(provisioner.installs), len(provisioner.revokes))
	}
	if provisioner.installs[0] != provisioner.revokes[0] {
		t.Fatalf("revoke identity = %#v, install identity = %#v", provisioner.revokes[0], provisioner.installs[0])
	}
}

func TestNVMeNonTLSDoesNotUsePSK(t *testing.T) {
	fake := transport.New()
	seedFakeTarget(t, fake, "nqn.plain", "10.0.0.1:4420", "guid-plain", "node-a")
	reader := &stageSecretReader{}
	provisioner := &stagePSKProvisioner{}
	p := &NVMeStagePlugin{Block: fake, Mount: newRecordingMount(), Log: logr.Discard(), NVMeTLSSecrets: reader, NVMeTLSPSK: provisioner}
	req := tlsStageRequest()
	req.StagingPath = "/staging/plain"
	req.GetNvme().Tls = false
	req.GetNvme().PskSecret = ""
	req.GetNvme().TargetNqn = "nqn.plain"
	req.GetNvme().Portal = "10.0.0.1:4420"
	req.GetNvme().DeviceGuid = "guid-plain"
	if _, err := p.NodeStage(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if len(reader.keys) != 0 || len(provisioner.installs) != 0 {
		t.Fatal("non-TLS used PSK")
	}
}

func TestNVMeTLSUnstageRevokesOnlyAfterDetach(t *testing.T) {
	for _, tc := range []struct {
		name       string
		detachErr  error
		wantRevoke int
	}{
		{name: "success", wantRevoke: 1},
		{name: "detach failure", detachErr: errors.New("detach failed"), wantRevoke: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := transport.New()
			seedFakeTarget(t, fake, "nqn.test", "10.0.0.1:4421", "guid-tls", "node-a")
			block := &recordingDetachClient{Client: fake, detachErr: tc.detachErr}
			reader := &stageSecretReader{secret: testTLSSecret(t)}
			provisioner := &stagePSKProvisioner{}
			p := &NVMeStagePlugin{Block: block, Mount: newRecordingMount(), Log: logr.Discard(), NVMeTLSNamespace: "driver-ns", NVMeTLSSecrets: reader, NVMeTLSPSK: provisioner}
			req := &stagepb.NodeUnstageRequest{StagingPath: "/staging/tls", Source: &stagepb.NodeUnstageRequest_Nvme{Nvme: tlsStageRequest().GetNvme()}}
			_, err := p.NodeUnstage(t.Context(), req)
			if tc.detachErr != nil && err == nil {
				t.Fatal("NodeUnstage succeeded")
			}
			if tc.detachErr == nil && err != nil {
				t.Fatalf("NodeUnstage: %v", err)
			}
			if len(provisioner.revokes) != tc.wantRevoke {
				t.Fatalf("revoke calls = %d, want %d", len(provisioner.revokes), tc.wantRevoke)
			}
			if tc.wantRevoke == 1 {
				if !block.detached {
					t.Fatal("revoke before detach")
				}
				if got, want := provisioner.revokes[0], [2]string{transport.HostNQN("node-a"), "nqn.test"}; got != want {
					t.Fatalf("revoke identity = %#v", got)
				}
			}
		})
	}
}
