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

// Package nvmet reconciles NVMeExport CRs into kernel nvmet configfs state.
package nvmet

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubeevents "k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nvmetv1 "github.com/randomvariable/zfs-csi/api/nvmet/v1alpha1"
	obsevents "github.com/randomvariable/zfs-csi/internal/observability/events"
	"github.com/randomvariable/zfs-csi/internal/observability/logging"
	"github.com/randomvariable/zfs-csi/internal/observability/metrics"
	"github.com/randomvariable/zfs-csi/internal/transport"
)

const (
	nvmeExportConditionReady = string(nvmetv1.NVMeExportConditionReady)
)

// ExportReconciler materialises NVMeExport CRs into transport exports. It is
// the sole writer of NVMeExport.status.
type ExportReconciler struct {
	crclient.Client

	Scheme *runtime.Scheme
	Log    logr.Logger
	Export transport.Server

	// Recorder emits Kubernetes Events. It remains nil-safe for direct construction.
	Recorder kubeevents.EventRecorder
}

// Reconcile applies the desired subsystem, namespace, and allowed-host set.
func (r *ExportReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := r.Log.WithValues(logging.KeyName, req.Name, logging.KeyNamespace, req.Namespace)

	export := &nvmetv1.NVMeExport{}
	if err := r.Get(ctx, req.NamespacedName, export); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}

		return reconcile.Result{}, fmt.Errorf("get NVMeExport %s: %w", req.NamespacedName, err)
	}
	server := r.Export
	if server == nil {
		server = transport.NewNVMET(transport.NewRealWriter())
	}

	ref := targetRef(export)
	if !export.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, log, server, export, ref)
	}

	if !controllerutil.ContainsFinalizer(export, nvmetv1.NVMeExportFinalizer) {
		patch := crclient.MergeFrom(export.DeepCopy())
		controllerutil.AddFinalizer(export, nvmetv1.NVMeExportFinalizer)
		if err := r.Patch(ctx, export, patch); err != nil {
			return reconcile.Result{}, fmt.Errorf("add NVMeExport finalizer %s/%s: %w", export.Namespace, export.Name, err)
		}
	}

	exportOp := logging.LogWith(log, logging.OpTransportExport,
		logging.KeyTargetNQN, export.Spec.TargetNQN,
		logging.KeyZvol, export.Spec.DevicePath,
		logging.KeyPortal, export.Spec.Portal,
		logging.KeyTransport, string(transport.KindNVMeTCP),
	).Metric(metrics.TransportOperationsTotal, string(transport.KindNVMeTCP), "export")
	ref, err := server.Export(ctx, transport.ExportOptions{
		ZvolPath:   export.Spec.DevicePath,
		DeviceGUID: export.Spec.DeviceGUID,
		TargetNQN:  export.Spec.TargetNQN,
		Portal:     export.Spec.Portal,
		Kind:       transport.KindNVMeTCP,
	})
	if err != nil && !errors.Is(err, transport.ErrAlreadyExported) {
		exportOp.Failed(err)
		transition, pErr := r.patchStatus(ctx, export, nvmetv1.NVMeExportStateError, false,
			nil, nvmeExportConditionReady, metav1.ConditionFalse, "ExportFailed", err.Error())
		if pErr != nil {
			return reconcile.Result{}, pErr
		}
		if transition.changed {
			r.recordEvent(export, obsevents.TypeWarning, obsevents.ReasonExportFailed, obsevents.ActionExporting,
				"NVMe export could not be reconciled")
		}

		return reconcile.Result{}, nil
	}
	exportOp.OK()

	// Liveness is no longer probed (F7): the previous HasActiveConnection read
	// targeted nvmet configfs attributes that do not exist on real kernels and so
	// always failed closed. Report ActiveConnection=false; deletion safety is now
	// enforced by desired-state gating, not a liveness probe (see reconcileDelete).
	const active = false

	mapped, err := server.MappedInitiators(ctx, ref)
	if err != nil {
		transition, pErr := r.patchStatus(ctx, export, nvmetv1.NVMeExportStateError, active,
			nil, nvmeExportConditionReady, metav1.ConditionFalse, "MappedInitiatorsFailed", err.Error())
		if pErr != nil {
			return reconcile.Result{}, pErr
		}
		if transition.changed {
			r.recordEvent(export, obsevents.TypeWarning, obsevents.ReasonMappedInitiatorsFailed, obsevents.ActionExporting,
				"NVMe export initiators could not be reconciled")
		}

		return reconcile.Result{}, nil
	}

	desired := stringSet(export.Spec.AllowedInitiators)
	for _, live := range mapped {
		if desired[live] {
			continue
		}

		op := logging.LogWith(log, logging.OpTransportUnmapInitiator,
			logging.KeyTargetNQN, ref.TargetNQN,
			logging.KeyInitiatorID, live,
			logging.KeyTransport, string(ref.Kind),
		).Metric(metrics.TransportOperationsTotal, string(ref.Kind), "unmap")
		if err := server.UnmapInitiator(ctx, ref, live); err != nil {
			op.Failed(err)
		} else {
			op.OK()
		}
	}

	admitted := make([]string, 0, len(export.Spec.AllowedInitiators))
	for _, initiator := range sortedKeys(desired) {
		op := logging.LogWith(log, logging.OpTransportMapInitiator,
			logging.KeyTargetNQN, ref.TargetNQN,
			logging.KeyInitiatorID, initiator,
			logging.KeyTransport, string(ref.Kind),
		).Metric(metrics.TransportOperationsTotal, string(ref.Kind), "map")
		if err := server.MapInitiator(ctx, ref, initiator); err != nil {
			op.Failed(err)
			continue
		}

		op.OK()
		admitted = append(admitted, initiator)
	}

	transition, err := r.patchStatus(ctx, export, nvmetv1.NVMeExportStateReady, active, admitted,
		nvmeExportConditionReady, metav1.ConditionTrue, "Reconciled", "export reconciled")
	if err != nil {
		return reconcile.Result{}, err
	}
	if transition.wasFalse && transition.changed {
		r.recordEvent(export, obsevents.TypeNormal, obsevents.ReasonExportReconciled, obsevents.ActionExporting,
			"NVMe export is ready")
	}

	return reconcile.Result{}, nil
}

func (r *ExportReconciler) reconcileDelete(ctx context.Context, log logr.Logger, server transport.Server,
	export *nvmetv1.NVMeExport, ref transport.TargetRef,
) (reconcile.Result, error) {
	// Desired-state gating (F7): the CR is being deleted, so the desired state is
	// "no export". The previous liveness-wait ("wait for active NVMe connection
	// to drain") relied on HasActiveConnection, which probed non-existent configfs
	// attributes and always failed closed — it would block deletion forever on a
	// real kernel. Unexport is the authoritative teardown: removing the port link
	// drops all controllers (the fence), and Unexport is idempotent. Proceed
	// directly to unexport.
	unexportOp := logging.LogWith(log, logging.OpTransportUnexport,
		logging.KeyTargetNQN, ref.TargetNQN,
		logging.KeyPortal, ref.Portal,
		logging.KeyTransport, string(ref.Kind),
	).Metric(metrics.TransportOperationsTotal, string(ref.Kind), "unexport")
	if err := server.Unexport(ctx, ref); err != nil {
		unexportOp.Failed(err)
		transition, pErr := r.patchStatus(ctx, export, nvmetv1.NVMeExportStateError, false,
			export.Status.AdmittedInitiators, nvmeExportConditionReady, metav1.ConditionFalse, "UnexportFailed", err.Error())
		if pErr != nil {
			return reconcile.Result{}, pErr
		}
		if transition.changed {
			r.recordEvent(export, obsevents.TypeWarning, obsevents.ReasonUnexportFailed, obsevents.ActionDeleting,
				"NVMe export could not be removed")
		}

		return reconcile.Result{}, nil
	}
	unexportOp.OK()

	patch := crclient.MergeFrom(export.DeepCopy())
	controllerutil.RemoveFinalizer(export, nvmetv1.NVMeExportFinalizer)
	if err := r.Patch(ctx, export, patch); err != nil {
		return reconcile.Result{}, fmt.Errorf("remove NVMeExport finalizer %s/%s: %w", export.Namespace, export.Name, err)
	}

	return reconcile.Result{}, nil
}

func (r *ExportReconciler) patchStatus(ctx context.Context, export *nvmetv1.NVMeExport,
	state nvmetv1.NVMeExportState, active bool, admitted []string,
	condType string, condStatus metav1.ConditionStatus, reason, msg string,
) (readyConditionTransition, error) {
	previous := meta.FindStatusCondition(export.Status.Conditions, condType)
	wasFalse := previous != nil && previous.Status == metav1.ConditionFalse
	changed := previous == nil || previous.Status != condStatus || previous.Reason != reason
	patch := crclient.MergeFrom(export.DeepCopy())
	export.Status.State = state
	export.Status.ActiveConnection = active
	export.Status.AdmittedInitiators = append([]string(nil), admitted...)
	sort.Strings(export.Status.AdmittedInitiators)
	export.Status.ObservedGeneration = export.Generation
	export.Status.Conditions = setExportCondition(export.Status.Conditions, export.Generation, condType, condStatus, reason, msg)

	if err := r.Status().Patch(ctx, export, patch); err != nil {
		return readyConditionTransition{}, fmt.Errorf("patch NVMeExport status %s/%s: %w", export.Namespace, export.Name, err)
	}

	return readyConditionTransition{
		changed:  changed,
		wasFalse: wasFalse,
	}, nil
}

// changed gates events when Ready status or reason changes; wasFalse identifies
// recovery from Ready=False, while the first Ready remains silent.
type readyConditionTransition struct {
	changed  bool
	wasFalse bool
}

func (r *ExportReconciler) recordEvent(export *nvmetv1.NVMeExport, eventType, reason, action, note string) {
	if r.Recorder != nil {
		r.Recorder.Eventf(export, nil, eventType, reason, action, note)
	}
}

// SetupWithManager registers the NVMeExport controller.
func (r *ExportReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("zfs-csi-nvmet-nvmeexport")
	}

	if err := ctrl.NewControllerManagedBy(mgr).
		Named("nvmeexport").
		WithOptions(controller.Options{UsePriorityQueue: new(bool)}).
		For(&nvmetv1.NVMeExport{}).
		Complete(r); err != nil {
		return fmt.Errorf("complete NVMeExport controller: %w", err)
	}

	return nil
}

func targetRef(export *nvmetv1.NVMeExport) transport.TargetRef {
	nsID := int(export.Spec.NamespaceID)
	if nsID == 0 {
		nsID = 1
	}

	return transport.TargetRef{
		Kind:        transport.KindNVMeTCP,
		TargetNQN:   export.Spec.TargetNQN,
		Portal:      export.Spec.Portal,
		NamespaceID: nsID,
		DeviceGUID:  export.Spec.DeviceGUID,
	}
}

func stringSet(in []string) map[string]bool {
	out := make(map[string]bool, len(in))
	for _, s := range in {
		if s == "" {
			continue
		}

		out[s] = true
	}

	return out
}

func sortedKeys(in map[string]bool) []string {
	out := make([]string, 0, len(in))
	for k := range in {
		out = append(out, k)
	}
	sort.Strings(out)

	return out
}

func setExportCondition(conds []metav1.Condition, generation int64, condType string, status metav1.ConditionStatus, reason, msg string) []metav1.Condition {
	meta.SetStatusCondition(&conds, metav1.Condition{
		Type:               condType,
		Status:             status,
		ObservedGeneration: generation,
		Reason:             reason,
		Message:            msg,
	})

	return conds
}

var _ reconcile.TypedReconciler[reconcile.Request] = (*ExportReconciler)(nil)
