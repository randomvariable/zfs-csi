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
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientevents "k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	zfscsiv1 "github.com/randomvariable/zfs-csi/api/v1alpha1"
	"github.com/randomvariable/zfs-csi/internal/naming"
	"github.com/randomvariable/zfs-csi/internal/observability/events"
	"github.com/randomvariable/zfs-csi/internal/observability/logging"
	"github.com/randomvariable/zfs-csi/internal/observability/metrics"
	"github.com/randomvariable/zfs-csi/internal/zfs"
)

const snapshotRequeueAfterCreate = 30 * time.Second

// SnapshotReconciler materialises Snapshot CRs into ZFS snapshots.
type SnapshotReconciler struct {
	crclient.Client

	Scheme   *runtime.Scheme
	Log      logr.Logger
	ZFS      zfs.Backend
	NodeName string

	// Recorder emits Kubernetes Events. It remains nil-safe for direct construction.
	Recorder clientevents.EventRecorder

	locks sync.Map
}

func (r *SnapshotReconciler) lockFor(dataset string) *sync.Mutex {
	v, _ := r.locks.LoadOrStore(dataset, &sync.Mutex{})

	return v.(*sync.Mutex)
}

// Reconcile handles snapshot create/delete.
func (r *SnapshotReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := r.Log.WithValues(logging.KeySnapshot, req.String())

	snap := &zfscsiv1.Snapshot{}
	if err := r.Get(ctx, req.NamespacedName, snap); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}

		return reconcile.Result{}, fmt.Errorf("get snapshot %s: %w", req.NamespacedName, err)
	}
	if snap.Spec.OwnerNode != r.NodeName {
		return reconcile.Result{}, nil
	}
	pool, err := snapshotPool(snap)
	if err != nil {
		return reconcile.Result{}, err
	}
	if err := verifyPoolIdentity(ctx, r.ZFS, pool, snap.Spec.PoolGUID); err != nil {
		return reconcile.Result{}, err
	}
	// Finalization must not depend on the source Volume still existing or still
	// passing current admission rules. Use the persisted backend identity first,
	// then fall back to parseable spec handles for older Snapshot objects.
	if !snap.DeletionTimestamp.IsZero() || snap.Status.CurrentState() == zfscsiv1.SnapshotStateDeleting {
		dataset, snapName, err := snapshotBackendIdentity(snap)
		if err != nil {
			return r.removeMalformedSnapshotFinalizer(ctx, snap)
		}
		mu := r.lockFor(dataset + "@" + snapName)
		mu.Lock()
		defer mu.Unlock()
		return r.reconcileDelete(ctx, log, snap, dataset, snapName)
	}
	if snap.Spec.VolumeRef == "" {
		changed, _ := r.setStatus(ctx, snap, zfscsiv1.SnapshotStateError, metav1.ConditionFalse,
			events.ReasonSnapshotInvalidVolumeID, "source Volume reference is empty")
		if changed {
			r.recordEvent(snap, events.TypeWarning, events.ReasonSnapshotInvalidVolumeID, events.ActionProvisioning, "snapshot source Volume reference is empty")
		}
		return reconcile.Result{}, nil
	}

	// Resolve the parent Volume and reject imported provenance before mutation.
	p, err := naming.ParseVolID(snap.Spec.SourceVolumeID)
	if err != nil {
		changed, _ := r.setStatus(ctx, snap, zfscsiv1.SnapshotStateError, metav1.ConditionFalse,
			events.ReasonSnapshotInvalidVolumeID, "malformed source volume id")
		if changed {
			r.recordEvent(snap, events.TypeWarning, events.ReasonSnapshotInvalidVolumeID, events.ActionProvisioning, "snapshot source volume ID is invalid")
		}

		return reconcile.Result{}, nil
	}
	_, snapName, err := naming.ParseSnapID(snap.Spec.SnapshotID)
	if err != nil {
		changed, _ := r.setStatus(ctx, snap, zfscsiv1.SnapshotStateError, metav1.ConditionFalse,
			events.ReasonSnapshotInvalidSnapshotID, "malformed snapshot id")
		if changed {
			r.recordEvent(snap, events.TypeWarning, events.ReasonSnapshotInvalidSnapshotID, events.ActionProvisioning, "snapshot ID is invalid")
		}

		return reconcile.Result{}, nil
	}
	source := &zfscsiv1.Volume{}
	if err := r.Get(ctx, types.NamespacedName{Name: snap.Spec.VolumeRef}, source); err != nil {
		if apierrors.IsNotFound(err) {
			changed, _ := r.setStatus(ctx, snap, zfscsiv1.SnapshotStateError, metav1.ConditionFalse,
				events.ReasonSnapshotInvalidVolumeID, "source Volume is unavailable")
			if changed {
				r.recordEvent(snap, events.TypeWarning, events.ReasonSnapshotInvalidVolumeID, events.ActionProvisioning, "snapshot source Volume is unavailable")
			}
			return reconcile.Result{}, nil
		} else {
			return reconcile.Result{}, fmt.Errorf("get snapshot source Volume %s: %w", snap.Spec.VolumeRef, err)
		}
	}
	if source.Spec.OwnerNode != snap.Spec.OwnerNode {
		return reconcile.Result{}, fmt.Errorf("source Volume %q owner node %q does not match Snapshot owner node %q", source.Name, source.Spec.OwnerNode, snap.Spec.OwnerNode)
	}
	if source.Spec.PoolGUID != snap.Spec.PoolGUID {
		return reconcile.Result{}, fmt.Errorf("source Volume %q pool GUID %q does not match Snapshot pool GUID %q", source.Name, source.Spec.PoolGUID, snap.Spec.PoolGUID)
	}
	if source.Spec.Provenance == zfscsiv1.VolumeProvenanceImported {
		changed, _ := r.setStatus(ctx, snap, zfscsiv1.SnapshotStateError, metav1.ConditionFalse,
			events.ReasonSnapshotInvalidVolumeID, "snapshots of imported volumes are not supported")
		if changed {
			r.recordEvent(snap, events.TypeWarning, events.ReasonSnapshotInvalidVolumeID, events.ActionProvisioning, "snapshots of imported volumes are not supported")
		}
		return reconcile.Result{}, nil
	}

	dataset := p.DatasetPath()
	mu := r.lockFor(dataset + "@" + snapName)
	mu.Lock()
	defer mu.Unlock()

	return r.reconcileCreate(ctx, log, snap, dataset, snapName)
}

func snapshotPool(snap *zfscsiv1.Snapshot) (string, error) {
	if snap.Status.DatasetPath != "" {
		dataset := snap.Status.DatasetPath
		if at := strings.LastIndex(dataset, "@"); at >= 0 {
			dataset = dataset[:at]
		}
		if slash := strings.Index(dataset, "/"); slash > 0 {
			return dataset[:slash], nil
		}
	}
	p, err := naming.ParseVolID(snap.Spec.SourceVolumeID)
	if err != nil {
		return "", fmt.Errorf("resolve snapshot %s pool identity: %w", snap.Name, err)
	}
	return p.Pool, nil
}

func snapshotBackendIdentity(snap *zfscsiv1.Snapshot) (string, string, error) {
	if snap.Status.DatasetPath != "" {
		at := strings.LastIndex(snap.Status.DatasetPath, "@")
		if at > 0 && at < len(snap.Status.DatasetPath)-1 {
			return snap.Status.DatasetPath[:at], snap.Status.DatasetPath[at+1:], nil
		}
	}
	p, err := naming.ParseVolID(snap.Spec.SourceVolumeID)
	if err != nil {
		return "", "", err
	}
	_, snapName, err := naming.ParseSnapID(snap.Spec.SnapshotID)
	if err != nil {
		return "", "", err
	}
	return p.DatasetPath(), snapName, nil
}

func (r *SnapshotReconciler) removeMalformedSnapshotFinalizer(ctx context.Context, snap *zfscsiv1.Snapshot) (reconcile.Result, error) {
	if !hasFinalizer(snap.Finalizers, zfscsiv1.SnapshotFinalizer) {
		return reconcile.Result{}, nil
	}
	patch := crclient.MergeFrom(snap.DeepCopy())
	removeFinalizer(&snap.Finalizers, zfscsiv1.SnapshotFinalizer)
	if err := r.Patch(ctx, snap, patch); err != nil && !apierrors.IsNotFound(err) {
		return reconcile.Result{}, fmt.Errorf("remove malformed snapshot finalizer %s/%s: %w", snap.Namespace, snap.Name, err)
	}
	return reconcile.Result{}, nil
}

func (r *SnapshotReconciler) reconcileCreate(ctx context.Context, log logr.Logger, snap *zfscsiv1.Snapshot, dataset, snapName string) (reconcile.Result, error) {
	op := logging.LogWith(log, logging.OpZFSSnapshot, logging.KeyDataset, dataset, logging.KeySnapshot, snapName).
		Metric(metrics.ZFSOperationsTotal, "snapshot")
	if err := r.ZFS.Snapshot(ctx, dataset, snapName); err != nil {
		if errors.Is(err, zfs.ErrAlreadyExists) {
			op.OK()
		} else {
			op.Failed(err)
			changed, _ := r.setStatus(ctx, snap, zfscsiv1.SnapshotStateError, metav1.ConditionFalse,
				events.ReasonSnapshotCreateFailed, "snapshot: "+err.Error())
			if changed {
				r.recordEvent(snap, events.TypeWarning, events.ReasonSnapshotCreateFailed, events.ActionProvisioning, "snapshot creation failed")
			}

			return reconcile.Result{RequeueAfter: snapshotRequeueAfterCreate}, nil
		}
	} else {
		op.OK()
	}

	changed, err := r.setReadyStatus(ctx, snap, dataset+"@"+snapName, time.Now().Unix(),
		events.ReasonSnapshotReady, "snapshot taken")
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("%s ready %s/%s: %w", logging.OpPatchSnapshotStatus, snap.Namespace, snap.Name, err)
	}
	if changed {
		r.recordEvent(snap, events.TypeNormal, events.ReasonSnapshotReady, events.ActionProvisioning, "snapshot is ready")
	}

	return reconcile.Result{}, nil
}

func (r *SnapshotReconciler) reconcileDelete(ctx context.Context, log logr.Logger, snap *zfscsiv1.Snapshot, dataset, snapName string) (reconcile.Result, error) {
	op := logging.LogWith(log, logging.OpZFSDestroySnapshot, logging.KeyDataset, dataset, logging.KeySnapshot, snapName).
		Metric(metrics.ZFSOperationsTotal, "destroy_snapshot")
	if err := r.ZFS.DestroySnapshot(ctx, dataset, snapName); err != nil {
		if errors.Is(err, zfs.ErrNotFound) {
			op.OK()
		} else {
			op.Failed(err)
			changed, _ := r.setStatus(ctx, snap, zfscsiv1.SnapshotStateDeleting, metav1.ConditionFalse,
				events.ReasonSnapshotDestroyFailed, "destroy snapshot: "+err.Error())
			if changed {
				r.recordEvent(snap, events.TypeWarning, events.ReasonSnapshotDestroyFailed, events.ActionDeleting, "snapshot deletion failed")
			}

			return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
		}
	} else {
		op.OK()
	}

	changed, _ := r.setStatus(ctx, snap, zfscsiv1.SnapshotStateDeleting, metav1.ConditionFalse,
		events.ReasonSnapshotDeleting, "snapshot is deleting")
	if changed {
		// Record while the Snapshot remains a live Event reference. A finalizer
		// patch can delete it immediately or fail and require a retry.
		r.recordEvent(snap, events.TypeNormal, events.ReasonSnapshotDeleting, events.ActionDeleting, "snapshot is deleting")
	}

	if hasFinalizer(snap.Finalizers, zfscsiv1.SnapshotFinalizer) {
		patch := crclient.MergeFrom(snap.DeepCopy())
		removeFinalizer(&snap.Finalizers, zfscsiv1.SnapshotFinalizer)
		if err := r.Patch(ctx, snap, patch); err != nil {
			return reconcile.Result{}, fmt.Errorf("remove snapshot finalizer %s/%s: %w", snap.Namespace, snap.Name, err)
		}
	}

	return reconcile.Result{}, nil
}

// setStatus persists a lifecycle transition before reporting its Event. Status
// may retain dynamic backend detail; Event notes are intentionally public and static.
func (r *SnapshotReconciler) setStatus(
	ctx context.Context,
	snap *zfscsiv1.Snapshot,
	state zfscsiv1.SnapshotState,
	ready metav1.ConditionStatus,
	reason, msg string,
) (bool, error) {
	return r.setStatusWith(ctx, snap, state, ready, reason, msg, nil)
}

// setReadyStatus persists the backend snapshot identity with its Ready transition.
func (r *SnapshotReconciler) setReadyStatus(
	ctx context.Context,
	snap *zfscsiv1.Snapshot,
	datasetPath string,
	createdAt int64,
	reason, msg string,
) (bool, error) {
	return r.setStatusWith(ctx, snap, zfscsiv1.SnapshotStateReady, metav1.ConditionTrue, reason, msg, func(after *zfscsiv1.Snapshot) {
		after.Status.DatasetPath = datasetPath
		after.Status.CreatedAt = createdAt
	})
}

func (r *SnapshotReconciler) setStatusWith(
	ctx context.Context,
	snap *zfscsiv1.Snapshot,
	state zfscsiv1.SnapshotState,
	ready metav1.ConditionStatus,
	reason, msg string,
	update func(*zfscsiv1.Snapshot),
) (bool, error) {
	previousState := snap.Status.State
	previousStatus, previousReason := snapshotReadyCondition(snap.Status.Conditions)
	before := snap.DeepCopy()
	after := snap.DeepCopy()
	after.Status.State = state
	after.Status.ObservedGeneration = after.Generation
	after.Status.ReadyToUse = ready == metav1.ConditionTrue
	after.Status.Conditions = setCondition(after.Status.Conditions, after.Generation,
		string(zfscsiv1.SnapshotConditionReady), ready, reason, msg)
	if update != nil {
		update(after)
	}

	op := logging.LogWith(logr.FromContextOrDiscard(ctx), logging.OpPatchSnapshotStatus, logging.KeyNamespace, snap.Namespace, logging.KeyName, snap.Name, logging.KeyState, state)
	if err := patchStatusWithConditions(ctx, r.Client, before, after, string(zfscsiv1.SnapshotConditionReady)); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		op.Failed(err)

		return false, fmt.Errorf("%s %s/%s: %w", logging.OpPatchSnapshotStatus, snap.Namespace, snap.Name, err)
	}
	op.OK()
	if err := r.Get(ctx, types.NamespacedName{Name: snap.Name}, snap); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("get patched snapshot %s/%s: %w", snap.Namespace, snap.Name, err)
	}

	return snapshotLifecycleChanged(previousState, previousStatus, previousReason, snap.Status), nil
}

func (r *SnapshotReconciler) recordEvent(snap *zfscsiv1.Snapshot, eventType, reason, action, note string) {
	if r.Recorder != nil {
		r.Recorder.Eventf(snap, nil, eventType, reason, action, note)
	}
}

func snapshotReadyCondition(conditions []metav1.Condition) (metav1.ConditionStatus, string) {
	for _, condition := range conditions {
		if condition.Type == string(zfscsiv1.SnapshotConditionReady) {
			return condition.Status, condition.Reason
		}
	}

	return metav1.ConditionUnknown, ""
}

func snapshotLifecycleChanged(
	previousState zfscsiv1.SnapshotState,
	previousStatus metav1.ConditionStatus,
	previousReason string,
	status zfscsiv1.SnapshotStatus,
) bool {
	readyStatus, readyReason := snapshotReadyCondition(status.Conditions)

	return previousState != status.State || previousStatus != readyStatus || previousReason != readyReason
}

// SetupWithManager registers the snapshot reconciler.
func (r *SnapshotReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("zfs-csi-snapshot")
	}

	if err := ctrl.NewControllerManagedBy(mgr).
		Named("snapshot").
		WithOptions(controller.Options{UsePriorityQueue: new(true)}).
		For(&zfscsiv1.Snapshot{}).
		Complete(r); err != nil {
		return fmt.Errorf("complete snapshot controller: %w", err)
	}

	return nil
}

var _ reconcile.TypedReconciler[reconcile.Request] = (*SnapshotReconciler)(nil)
