// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

package stage

import (
	"context"
	"errors"
	"regexp"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/randomvariable/zfs-csi/internal/psk"
	"github.com/randomvariable/zfs-csi/internal/transport"
)

const nvmeTLSPSKDataKey = "psk"

var nvmeTLSPSKNameRE = regexp.MustCompile(`^zfs-csi-nvme-psk-[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// NVMeTLSSecretReader can retrieve only one configured Secret by name.
type NVMeTLSSecretReader interface {
	Get(context.Context, string, string) (*corev1.Secret, error)
}

// NVMeTLSPSKProvisioner installs and revokes an initiator's derived key.
type NVMeTLSPSKProvisioner interface {
	Install(psk.Interchange, string, string) error
	Revoke(psk.Interchange, string, string) error
}

// LinuxNVMeTLSPSKProvisioner manages retained v0 PSKs in the host .nvme ring.
type LinuxNVMeTLSPSKProvisioner struct{}

func (LinuxNVMeTLSPSKProvisioner) Install(interchange psk.Interchange, hostNQN, targetNQN string) error {
	_, err := psk.Install(interchange, hostNQN, targetNQN, psk.DefaultVersion)
	return err
}

func (LinuxNVMeTLSPSKProvisioner) Revoke(interchange psk.Interchange, hostNQN, targetNQN string) error {
	material, err := psk.Derive(interchange, hostNQN, targetNQN, psk.DefaultVersion)
	if err != nil {
		return err
	}
	return psk.Remove(material.Identity)
}

func (p *NVMeStagePlugin) ensureTLSPSK(ctx context.Context, nvmeSecret, initiatorID, targetNQN string) (psk.Interchange, error) {
	if p.NVMeTLSSecrets == nil {
		return psk.Interchange{}, errors.New("NVMe TLS Secret reader is unavailable")
	}
	if p.NVMeTLSPSK == nil {
		return psk.Interchange{}, errors.New("NVMe TLS PSK provisioner is unavailable")
	}
	if !validNVMeTLSPSKName(nvmeSecret) {
		return psk.Interchange{}, errors.New("NVMe TLS Secret name is invalid")
	}
	if initiatorID == "" || targetNQN == "" {
		return psk.Interchange{}, errors.New("NVMe TLS identity is incomplete")
	}
	secret, err := p.NVMeTLSSecrets.Get(ctx, p.NVMeTLSNamespace, nvmeSecret)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return psk.Interchange{}, errors.New("NVMe TLS Secret is unavailable")
		}
		return psk.Interchange{}, errors.New("read NVMe TLS Secret failed")
	}
	interchange, err := validateNVMeTLSPSKSecret(secret)
	if err != nil {
		return psk.Interchange{}, err
	}
	if err := p.NVMeTLSPSK.Install(interchange, transport.HostNQN(initiatorID), targetNQN); err != nil {
		return psk.Interchange{}, errors.New("install NVMe TLS PSK failed")
	}
	return interchange, nil
}

func (p *NVMeStagePlugin) revokeTLSPSK(ctx context.Context, nvmeSecret, initiatorID, targetNQN string) {
	if p.NVMeTLSSecrets == nil || p.NVMeTLSPSK == nil || !validNVMeTLSPSKName(nvmeSecret) {
		return
	}
	secret, err := p.NVMeTLSSecrets.Get(ctx, p.NVMeTLSNamespace, nvmeSecret)
	if err != nil {
		return
	}
	interchange, err := validateNVMeTLSPSKSecret(secret)
	if err != nil {
		return
	}
	p.revokeInstalledTLSPSK(interchange, initiatorID, targetNQN)
}

func (p *NVMeStagePlugin) revokeInstalledTLSPSK(interchange psk.Interchange, initiatorID, targetNQN string) {
	if p.NVMeTLSPSK == nil {
		return
	}
	_ = p.NVMeTLSPSK.Revoke(interchange, transport.HostNQN(initiatorID), targetNQN)
}

func validateNVMeTLSPSKSecret(secret *corev1.Secret) (psk.Interchange, error) {
	if secret == nil || secret.Type != corev1.SecretTypeOpaque || secret.Immutable == nil || !*secret.Immutable {
		return psk.Interchange{}, errors.New("NVMe TLS Secret must be immutable and Opaque")
	}
	if len(secret.Data) != 1 || len(secret.Data[nvmeTLSPSKDataKey]) == 0 {
		return psk.Interchange{}, errors.New("NVMe TLS Secret must contain only non-empty data key psk")
	}
	interchange, err := parseNVMeTLSPSK(secret.Data[nvmeTLSPSKDataKey])
	if err != nil {
		return psk.Interchange{}, errors.New("NVMe TLS Secret data key psk is malformed")
	}
	return interchange, nil
}

// parseNVMeTLSPSK confines the parser's string-only API to this boundary.
// Raw Secret bytes never appear in errors, logs, or RPC objects.
func parseNVMeTLSPSK(raw []byte) (psk.Interchange, error) {
	return psk.Parse(string(raw))
}

func validNVMeTLSPSKName(name string) bool {
	return len(name) <= 253 && nvmeTLSPSKNameRE.MatchString(name)
}

// ExactSecretReader is deliberately incapable of listing or modifying Secrets.
type ExactSecretReader struct {
	GetSecret func(context.Context, string, string, metav1.GetOptions) (*corev1.Secret, error)
}

func (r ExactSecretReader) Get(ctx context.Context, namespace, name string) (*corev1.Secret, error) {
	if r.GetSecret == nil {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, name)
	}
	return r.GetSecret(ctx, namespace, name, metav1.GetOptions{})
}
