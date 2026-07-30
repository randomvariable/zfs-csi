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

package agent

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/randomvariable/zfs-csi/internal/nfs"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/inventory"
	"github.com/randomvariable/zfs-csi/internal/naming"
	"github.com/randomvariable/zfs-csi/internal/zfs"
)

type FormatProbe func(context.Context, string) (string, error)

type VolumeImportReconciler struct {
	crclient.Client
	Scheme                  *runtime.Scheme
	Log                     logr.Logger
	ZFS                     zfs.Backend
	NodeName                string
	PoolResolver            inventory.Resolver
	ProbeFormat             FormatProbe
	MaxConcurrentReconciles int
}

func (r *VolumeImportReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	imp := &zfscsiv1.VolumeImport{}
	if err := r.Get(ctx, req.NamespacedName, imp); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("get VolumeImport: %w", err)
	}
	if !imp.DeletionTimestamp.IsZero() || imp.Spec.OwnerNode != r.NodeName {
		return reconcile.Result{}, nil
	}
	backendPath, err := validateImportPath(imp.Spec.Pool, imp.Spec.BackendPath)
	if err != nil {
		return r.fail(ctx, imp, "InvalidBackendPath", err.Error())
	}
	if imp.Spec.DeletionPolicy != "" && imp.Spec.DeletionPolicy != zfscsiv1.VolumeDeletionPolicyRetain {
		return r.fail(ctx, imp, "InvalidDeletionPolicy", "VolumeImport deletionPolicy must be Retain")
	}
	if imp.Spec.Type == zfscsiv1.VolumeTypeBlock && imp.Spec.Transport != "" && imp.Spec.Transport != zfscsiv1.TransportNVMeTCP {
		return r.fail(ctx, imp, "UnsupportedTransport", "block imports support only nvme-tcp")
	}
	if imp.Spec.Type == zfscsiv1.VolumeTypeFilesystem {
		cidrs, accessMode, err := nfs.NormalizeExportIntent(imp.Spec.NFSExportCIDRs, imp.Spec.NFSExportAccessMode)
		if err != nil {
			return r.fail(ctx, imp, "InvalidNFSExportIntent", "invalid NFS export intent: "+err.Error())
		}
		imp.Spec.NFSExportCIDRs = cidrs
		imp.Spec.NFSExportAccessMode = accessMode
	}
	pools, err := r.ZFS.PoolNames(ctx)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("list imported pools: %w", err)
	}
	if !slices.Contains(pools, imp.Spec.Pool) {
		return r.pending(ctx, imp, "PoolNotImported", "declared pool is not imported on the owner node", poolNotImportedRequeue)
	}
	resolver := r.PoolResolver
	if resolver.Client == nil {
		resolver.Client = r.Client
	}
	poolGUID, err := resolver.ResolvePoolGUID(ctx, imp.Spec.OwnerNode, imp.Spec.Pool)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("resolve import pool identity: %w", err)
	}
	if err := verifyPoolIdentity(ctx, r.ZFS, imp.Spec.Pool, poolGUID); err != nil {
		return reconcile.Result{}, err
	}
	storageNode := &zfscsiv1.StorageNode{}
	if err := r.Get(ctx, types.NamespacedName{Name: imp.Spec.OwnerNode}, storageNode); err != nil {
		return reconcile.Result{}, fmt.Errorf("get import owner reachability: %w", err)
	}
	info, err := r.ZFS.Get(ctx, backendPath)
	if err != nil {
		if errors.Is(err, zfs.ErrNotFound) {
			return r.fail(ctx, imp, "BackendNotFound", "backend object does not exist")
		}
		return reconcile.Result{}, fmt.Errorf("inspect backend %s: %w", backendPath, err)
	}
	wantKind := zfs.KindBlock
	if imp.Spec.Type == zfscsiv1.VolumeTypeFilesystem {
		wantKind = zfs.KindFilesystem
	}
	switch {
	case info.Kind != wantKind:
		return r.fail(ctx, imp, "WrongKind", fmt.Sprintf("backend kind %q does not match %q", info.Kind, wantKind))
	case info.Encrypted:
		return r.fail(ctx, imp, "EncryptedUnsupported", "encrypted backend objects cannot be imported")
	case info.Capacity <= 0 && wantKind == zfs.KindFilesystem:
		return r.fail(ctx, imp, "RefquotaRequired", "filesystem imports require a non-zero refquota")
	case info.Capacity < imp.Spec.Capacity:
		return r.fail(ctx, imp, "InsufficientCapacity", fmt.Sprintf("backend capacity %d is below requested %d", info.Capacity, imp.Spec.Capacity))
	case wantKind == zfs.KindFilesystem && info.ExportPath == "":
		return r.fail(ctx, imp, "ExportPathUnavailable", "filesystem export path cannot be observed")
	}
	if wantKind == zfs.KindFilesystem {
		exportPath, err := validateFilesystemExportPath(info.ExportPath)
		if err != nil {
			return r.fail(ctx, imp, "InvalidExportPath", err.Error())
		}
		info.ExportPath = exportPath
	}
	if wantKind == zfs.KindBlock {
		probe := r.ProbeFormat
		if probe == nil {
			probe = probeDiskFormat
		}
		format := info.Format
		var probeErr error
		if format == "" {
			format, probeErr = probe(ctx, info.DevPath)
		}
		if probeErr != nil {
			return r.fail(ctx, imp, "FormatProbeFailed", probeErr.Error())
		}
		if format != imp.Spec.FsType {
			return r.fail(ctx, imp, "FormatMismatch", fmt.Sprintf("backend format %q does not match requested %q", format, imp.Spec.FsType))
		}
	}
	if info.Name != "" && info.Name != backendPath {
		return r.fail(ctx, imp, "BackendIdentityMismatch", fmt.Sprintf("backend reported canonical name %q, expected %q", info.Name, backendPath))
	}

	id := naming.ImportID(backendPath)
	handle, err := naming.EncodeVolID(imp.Spec.Pool, wantKind, id)
	if err != nil {
		return r.fail(ctx, imp, "InvalidIdentity", err.Error())
	}
	otherImports := &zfscsiv1.VolumeImportList{}
	if err := r.List(ctx, otherImports); err != nil {
		return reconcile.Result{}, fmt.Errorf("list VolumeImports for backend conflict: %w", err)
	}
	for i := range otherImports.Items {
		other := &otherImports.Items[i]
		if volumeImportClaim(other) != volumeImportClaim(imp) && strings.TrimSuffix(strings.TrimSpace(other.Spec.BackendPath), "/") == backendPath && volumeImportPrecedes(other, imp) {
			return r.fail(ctx, imp, "BackendConflict", fmt.Sprintf("backend is already claimed by VolumeImport %q", volumeImportClaim(other)))
		}
	}
	volume := &zfscsiv1.Volume{}
	key := types.NamespacedName{Name: id}
	err = r.Get(ctx, key, volume)
	if apierrors.IsNotFound(err) {
		if imp.Status.VolumeRef != "" {
			return r.fail(ctx, imp, "MaterializedVolumeMissing", "materialized Volume no longer exists; refusing automatic re-adoption")
		}
		volume = &zfscsiv1.Volume{
			ObjectMeta: metav1.ObjectMeta{
				Name: id, Finalizers: []string{zfscsiv1.VolumeFinalizer},
				Annotations: map[string]string{volumeImportAnnotation: volumeImportClaim(imp)},
			},
			Spec: materializedImportIntent(imp, backendPath, handle, poolGUID, storageNode.Spec.NetworkDomain),
		}
		if err := r.Create(ctx, volume); err != nil {
			return reconcile.Result{}, fmt.Errorf("create imported Volume: %w", err)
		}
	} else if err != nil {
		return reconcile.Result{}, fmt.Errorf("get imported Volume: %w", err)
	} else if !volume.DeletionTimestamp.IsZero() {
		return r.fail(ctx, imp, "MaterializedVolumeDeleting", "materialized Volume is being deleted; refusing automatic re-adoption")
	} else if mismatches := importedVolumeIntentMismatches(volume, imp, backendPath, handle, poolGUID, storageNode.Spec.NetworkDomain); len(mismatches) != 0 {
		return r.fail(ctx, imp, "VolumeConflict", "materialized Volume has incompatible import intent: "+strings.Join(mismatches, ", "))
	}
	latest := &zfscsiv1.Volume{}
	if err := r.Get(ctx, key, latest); err != nil {
		return reconcile.Result{}, err
	}
	ready := latest.Status.CurrentState() == zfscsiv1.VolumeStateReady && latest.Status.ObservedGeneration == latest.Generation
	before := imp.DeepCopy()
	imp.Status.ObservedGeneration = imp.Generation
	imp.Status.VolumeHandle = handle
	imp.Status.VolumeRef = id
	imp.Status.ActualCapacity = info.Capacity
	imp.Status.ExportPath = info.ExportPath
	if ready {
		imp.Status.State = zfscsiv1.VolumeImportStateReady
		imp.Status.Conditions = setCondition(imp.Status.Conditions, imp.Generation, "Ready", metav1.ConditionTrue, "ImportedVolumeReady", "backend validated and Volume is ready")
	} else if latest.Status.CurrentState() == zfscsiv1.VolumeStateError {
		imp.Status.State = zfscsiv1.VolumeImportStateFailed
		imp.Status.Conditions = setCondition(imp.Status.Conditions, imp.Generation, "Ready", metav1.ConditionFalse, "ImportedVolumeFailed", "materialized Volume reconciliation failed")
	} else {
		imp.Status.State = zfscsiv1.VolumeImportStatePending
		imp.Status.Conditions = setCondition(imp.Status.Conditions, imp.Generation, "Ready", metav1.ConditionFalse, "WaitingForVolume", "backend validated; waiting for Volume readiness")
	}
	if err := r.Status().Patch(ctx, imp, crclient.MergeFrom(before)); err != nil {
		return reconcile.Result{}, fmt.Errorf("patch VolumeImport status: %w", err)
	}
	if !ready && imp.Status.State == zfscsiv1.VolumeImportStatePending {
		return reconcile.Result{RequeueAfter: time.Second}, nil
	}
	return reconcile.Result{}, nil
}

func (r *VolumeImportReconciler) pending(ctx context.Context, imp *zfscsiv1.VolumeImport, reason, message string, requeue time.Duration) (reconcile.Result, error) {
	before := imp.DeepCopy()
	imp.Status.State = zfscsiv1.VolumeImportStatePending
	imp.Status.ObservedGeneration = imp.Generation
	imp.Status.Conditions = setCondition(imp.Status.Conditions, imp.Generation, "Ready", metav1.ConditionFalse, reason, message)
	if err := r.Status().Patch(ctx, imp, crclient.MergeFrom(before)); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{RequeueAfter: requeue}, nil
}

func (r *VolumeImportReconciler) fail(ctx context.Context, imp *zfscsiv1.VolumeImport, reason, message string) (reconcile.Result, error) {
	before := imp.DeepCopy()
	imp.Status.State = zfscsiv1.VolumeImportStateFailed
	imp.Status.ObservedGeneration = imp.Generation
	imp.Status.Conditions = setCondition(imp.Status.Conditions, imp.Generation, "Ready", metav1.ConditionFalse, reason, message)
	if err := r.Status().Patch(ctx, imp, crclient.MergeFrom(before)); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

func validateImportPath(pool, backendPath string) (string, error) {
	path := strings.TrimSuffix(strings.TrimSpace(backendPath), "/")
	if path == "" || !strings.HasPrefix(path, pool+"/") || strings.Contains(path, "@") {
		return "", fmt.Errorf("backendPath must be a dataset in pool %q", pool)
	}
	reserved := pool + "/" + naming.CSIPathRoot
	if path == reserved || strings.HasPrefix(path, reserved+"/") {
		return "", fmt.Errorf("backendPath is inside the dynamically managed %s subtree", reserved)
	}
	return path, nil
}

func validateFilesystemExportPath(exportPath string) (string, error) {
	path := strings.TrimSpace(exportPath)
	if path == "" || path == "legacy" || path == "none" {
		return "", fmt.Errorf("filesystem backend requires an absolute mountpoint; got %q", path)
	}
	if !strings.HasPrefix(path, "/") || filepath.Clean(path) != path || path == "/" {
		return "", fmt.Errorf("filesystem backend mountpoint must be a clean absolute path; got %q", path)
	}
	return path, nil
}

func probeDiskFormat(ctx context.Context, device string) (string, error) {
	out, err := exec.CommandContext(ctx, "blkid", "-p", "-s", "TYPE", "-o", "value", device).Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 2 {
			return "", nil
		}
		return "", fmt.Errorf("probe %s: %w", device, err)
	}
	return strings.TrimSpace(string(out)), nil
}

func importedVolumeIntentMismatches(vol *zfscsiv1.Volume, imp *zfscsiv1.VolumeImport, path, handle, poolGUID, networkDomain string) []string {
	want := materializedImportIntent(imp, path, handle, poolGUID, networkDomain)
	checks := []struct {
		field string
		match bool
	}{
		{"provenance", vol.Spec.Provenance == want.Provenance},
		{"deletionPolicy", vol.Spec.DeletionPolicy == want.DeletionPolicy},
		{"backendPath", vol.Spec.BackendPath == want.BackendPath},
		{"importClaim", vol.Annotations[volumeImportAnnotation] == volumeImportClaim(imp)},
		{"pool", vol.Spec.Pool == want.Pool},
		{"poolGUID", vol.Spec.PoolGUID == want.PoolGUID},
		{"type", vol.Spec.Type == want.Type},
		{"capacity", vol.Spec.Capacity == want.Capacity},
		{"transport", vol.Spec.Transport == want.Transport},
		{"ownerNode", vol.Spec.OwnerNode == want.OwnerNode},
		{"volumeID", vol.Spec.VolumeID == want.VolumeID},
		{"volName", vol.Spec.VolName == want.VolName},
		{"importFsTypeDeclaration", vol.Spec.ImportFsTypeDeclaration == want.ImportFsTypeDeclaration},
		{"nfsExportCIDRs", cidrSetsEqual(vol.Spec.NFSExportCIDRs, want.NFSExportCIDRs)},
		{"nfsExportAccessMode", vol.Spec.NFSExportAccessMode == want.NFSExportAccessMode},
		{"sourceSnapshotID", vol.Spec.SourceSnapshotID == ""},
		{"sourceVolumeID", vol.Spec.SourceVolumeID == ""},
		{"encryptionKeyRef", vol.Spec.EncryptionKeyRef == ""},
	}
	mismatches := make([]string, 0)
	for _, check := range checks {
		if !check.match {
			mismatches = append(mismatches, check.field)
		}
	}
	return mismatches
}

func cidrSetsEqual(a, b []string) bool {
	if len(a) == 0 || len(b) == 0 {
		return len(a) == len(b)
	}
	aPrefixes, err := nfs.ParseExportCIDRs(a)
	if err != nil {
		return false
	}
	bPrefixes, err := nfs.ParseExportCIDRs(b)
	if err != nil {
		return false
	}
	return slices.Equal(aPrefixes, bPrefixes)
}

func materializedImportIntent(imp *zfscsiv1.VolumeImport, path, handle, poolGUID, networkDomain string) zfscsiv1.VolumeSpec {
	return zfscsiv1.VolumeSpec{
		Provenance: zfscsiv1.VolumeProvenanceImported, BackendPath: path,
		DeletionPolicy: zfscsiv1.VolumeDeletionPolicyRetain, Pool: imp.Spec.Pool, PoolGUID: poolGUID,
		NetworkDomain: networkDomain,
		Capacity:      imp.Spec.Capacity, Type: imp.Spec.Type,
		Transport: imp.Spec.Transport, OwnerNode: imp.Spec.OwnerNode,
		VolName: naming.ImportID(path), VolumeID: handle,
		NFSExportCIDRs: imp.Spec.NFSExportCIDRs, NFSExportAccessMode: imp.Spec.NFSExportAccessMode,
		ImportFsTypeDeclaration: imp.Spec.FsType,
	}
}

const volumeImportAnnotation = "zfs.csi.randomvariable.co.uk/volume-import"

func volumeImportPrecedes(a, b *zfscsiv1.VolumeImport) bool {
	if !a.CreationTimestamp.Time.Equal(b.CreationTimestamp.Time) {
		return a.CreationTimestamp.Before(&b.CreationTimestamp)
	}
	return volumeImportClaim(a) < volumeImportClaim(b)
}

func volumeImportClaim(imp *zfscsiv1.VolumeImport) string { return imp.Name }

func (r *VolumeImportReconciler) SetupWithManager(mgr ctrl.Manager) error {
	maxConcurrent := r.MaxConcurrentReconciles
	if maxConcurrent <= 0 {
		maxConcurrent = defaultMaxConcurrentReconciles
	}
	return ctrl.NewControllerManagedBy(mgr).Named("volume-import").WithOptions(controller.Options{MaxConcurrentReconciles: maxConcurrent}).For(&zfscsiv1.VolumeImport{}).Complete(r)
}

var _ reconcile.TypedReconciler[reconcile.Request] = (*VolumeImportReconciler)(nil)
