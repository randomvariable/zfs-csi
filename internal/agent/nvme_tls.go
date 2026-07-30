// Copyright 2026 Naadir Jeewa
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	eventsv1 "github.com/randomvariable/zfs-csi/internal/observability/events"
	"github.com/randomvariable/zfs-csi/internal/psk"
	"github.com/randomvariable/zfs-csi/internal/transport"
)

const nvmeTLSPSKSecretDataKey = "psk"

// NVMeTLSPSKProvisioner installs target-side PSK material. Implementations
// receive only parsed interchange material and public NVMe identities.
type NVMeTLSPSKProvisioner interface {
	InsertConfigured(interchange psk.Interchange, hostNQN, subsysNQN string) error
	RemoveConfigured(interchange psk.Interchange, hostNQN, subsysNQN string) error
}

// NVMeTLSPSKKeyringProvisioner installs the Linux 6.8-compatible v0 key.
// The host identity is the consumer's canonical Host NQN, matching the
// identity presented during its TLS handshake to the target subsystem.
type NVMeTLSPSKKeyringProvisioner struct{}

func (NVMeTLSPSKKeyringProvisioner) InsertConfigured(interchange psk.Interchange, hostNQN, subsysNQN string) error {
	_, err := psk.Install(interchange, hostNQN, subsysNQN, psk.DefaultVersion)
	return err
}

func (NVMeTLSPSKKeyringProvisioner) RemoveConfigured(interchange psk.Interchange, hostNQN, subsysNQN string) error {
	material, err := psk.Derive(interchange, hostNQN, subsysNQN, psk.DefaultVersion)
	if err != nil {
		return err
	}
	return psk.Remove(material.Identity)
}

// NVMeTLSSecretReader reads immutable target PSK Secrets by their configured
// namespace/name. Reconciliation never lists or watches Secret objects.
type NVMeTLSSecretReader interface {
	Get(context.Context, crclient.ObjectKey, crclient.Object, ...crclient.GetOption) error
}

func (r *VolumeReconciler) ensureNVMeTLSPSK(ctx context.Context, secretName, targetNQN string, initiatorIDs []string) error {
	if r.NVMeTLSPSK == nil {
		return errors.New("NVMe TLS PSK provisioner is unavailable")
	}
	if secretName == "" {
		return errors.New("NVMe TLS PSK Secret reference is missing")
	}
	initiatorIDs = uniqueInitiatorIDs(initiatorIDs)
	reader := r.NVMeTLSSecretReader
	if reader == nil {
		return errors.New("NVMe TLS direct Secret reader is unavailable")
	}

	secret := &corev1.Secret{}
	if err := reader.Get(ctx, crclient.ObjectKey{Namespace: r.Namespace, Name: secretName}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return errors.New("NVMe TLS PSK Secret is unavailable")
		}
		return errors.New("read NVMe TLS PSK Secret failed")
	}
	if secret.Type != corev1.SecretTypeOpaque || secret.Immutable == nil || !*secret.Immutable {
		return errors.New("NVMe TLS PSK Secret must be immutable and Opaque")
	}
	if len(secret.Data) != 1 {
		return errors.New("NVMe TLS PSK Secret must contain only data key psk")
	}
	configured, ok := secret.Data[nvmeTLSPSKSecretDataKey]
	if !ok || len(configured) == 0 {
		return errors.New("NVMe TLS PSK Secret data key psk is missing")
	}
	interchange, err := parseNVMeTLSPSK(configured)
	if err != nil {
		return errors.New("NVMe TLS PSK Secret data key psk is malformed")
	}
	for _, initiatorID := range initiatorIDs {
		if err := r.NVMeTLSPSK.InsertConfigured(interchange, transport.HostNQN(initiatorID), targetNQN); err != nil {
			return errors.New("install NVMe TLS PSK failed")
		}
	}
	return nil
}

// revokeNVMeTLSPSK runs only after the target has been torn down or the backend
// has been terminally destroyed. It never gates finalizer removal: keyrings are
// volatile, while deletion must remain safe when the Secret is already gone.
func (r *VolumeReconciler) revokeNVMeTLSPSK(ctx context.Context, secretName, targetNQN string, initiatorIDs []string) {
	if r.NVMeTLSPSK == nil || secretName == "" || r.NVMeTLSSecretReader == nil {
		return
	}
	secret := &corev1.Secret{}
	if err := r.NVMeTLSSecretReader.Get(ctx, crclient.ObjectKey{Namespace: r.Namespace, Name: secretName}, secret); err != nil {
		return
	}
	configured, ok := secret.Data[nvmeTLSPSKSecretDataKey]
	if !ok || len(configured) == 0 {
		return
	}
	interchange, err := parseNVMeTLSPSK(configured)
	if err != nil {
		return
	}
	for _, initiatorID := range uniqueInitiatorIDs(initiatorIDs) {
		_ = r.NVMeTLSPSK.RemoveConfigured(interchange, transport.HostNQN(initiatorID), targetNQN)
	}
}

// deleteNVMeTLSPSKSecret removes controller-created PSK material only after
// ZFS destroy and DEK crypto-shred completed. A missing Secret is success.
func (r *VolumeReconciler) deleteNVMeTLSPSKSecret(ctx context.Context, vol *zfscsiv1.Volume) (reconcile.Result, error) {
	secretName := vol.Spec.NVMeTLSPSKSecretName
	if secretName == "" {
		return reconcile.Result{}, nil
	}

	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Namespace: r.Namespace,
		Name:      secretName,
	}}
	if err := r.Delete(ctx, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		r.recordStatusWarning(
			ctx,
			vol,
			zfscsiv1.VolumeStateDeleting,
			"delete NVMe TLS PSK Secret: "+err.Error(),
			volumeWarningEvent{
				reason: eventsv1.ReasonVolumeDeleteFailed, action: eventsv1.ActionDeleting, publicNote: "volume deletion failed",
			},
		)
		return reconcile.Result{Priority: new(handler.LowPriority)}, fmt.Errorf(
			"delete NVMe TLS PSK Secret %s/%s: %w", r.Namespace, secretName, err,
		)
	}

	return reconcile.Result{}, nil
}

func uniqueInitiatorIDs(initiatorIDs []string) []string {
	seen := make(map[string]struct{}, len(initiatorIDs))
	unique := make([]string, 0, len(initiatorIDs))
	for _, initiatorID := range initiatorIDs {
		if initiatorID == "" {
			continue
		}
		if _, ok := seen[initiatorID]; ok {
			continue
		}
		seen[initiatorID] = struct{}{}
		unique = append(unique, initiatorID)
	}
	return unique
}

// parseNVMeTLSPSK confines the crypto package's required string conversion to
// this boundary; credential bytes are never formatted, logged, or persisted.
func parseNVMeTLSPSK(configured []byte) (psk.Interchange, error) {
	return psk.Parse(string(configured))
}
