/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package hardwaremanagement

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/openshift-kni/oran-hwmgr-plugin-test/internal/controller/utils"
	"github.com/openshift-kni/oran-hwmgr-plugin-test/internal/service"
	hwmgmtv1alpha1 "github.com/openshift-kni/oran-o2ims/api/hardwaremanagement/v1alpha1"
)

const pluginFinalizer = "oran-hwmgr-plugin-test.oran.openshift.io/nodepool-finalizer"

// NodePoolReconciler reconciles a NodePool object
type NodePoolReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Logger *slog.Logger
	hwmgr  *service.HwMgrService
}

func doNotRequeue() ctrl.Result { // nolint:unused
	return ctrl.Result{Requeue: false}
}

func requeueWithError(err error) (ctrl.Result, error) { // nolint:unused
	// can not be fixed by user during reconcile
	return ctrl.Result{}, err
}

func requeueWithLongInterval() ctrl.Result { // nolint:unused
	return requeueWithCustomInterval(5 * time.Minute)
}

func requeueWithMediumInterval() ctrl.Result { // nolint:unused
	return requeueWithCustomInterval(1 * time.Minute)
}

func requeueWithShortInterval() ctrl.Result { // nolint:unused
	return requeueWithCustomInterval(15 * time.Second)
}

func requeueWithCustomInterval(interval time.Duration) ctrl.Result { // nolint:unused
	return ctrl.Result{RequeueAfter: interval}
}

//+kubebuilder:rbac:groups=hardwaremanagement.oran.openshift.io,resources=nodepools,verbs=get;list;watch;update;patch
//+kubebuilder:rbac:groups=hardwaremanagement.oran.openshift.io,resources=nodepools/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=hardwaremanagement.oran.openshift.io,resources=nodepools/finalizers,verbs=update
//+kubebuilder:rbac:groups=hardwaremanagement.oran.openshift.io,resources=nodes,verbs=get;create;list;watch;update;patch;delete
//+kubebuilder:rbac:groups=hardwaremanagement.oran.openshift.io,resources=nodes/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=hardwaremanagement.oran.openshift.io,resources=nodes/finalizers,verbs=update
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;create;update;patch;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the NodePool object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.16.3/pkg/reconcile
func (r *NodePoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, err error) {
	_ = log.FromContext(ctx)
	result = doNotRequeue()

	// Fetch the nodepool:
	nodepool := &hwmgmtv1alpha1.NodePool{}
	if err = r.Client.Get(ctx, req.NamespacedName, nodepool); err != nil {
		if errors.IsNotFound(err) {
			// The NodePool could have been deleted
			r.Logger.ErrorContext(ctx, "NodePool not found... deleted? "+req.Name)
			err = nil
			return
		}
		r.Logger.ErrorContext(
			ctx,
			"Unable to fetch NodePool",
			slog.String("error", err.Error()),
		)
		return
	}

	r.Logger.InfoContext(ctx, "[NodePool] "+nodepool.Name)

	if nodepool.GetDeletionTimestamp() != nil {
		if controllerutil.ContainsFinalizer(nodepool, pluginFinalizer) {
			if err := r.finalizer(ctx, nodepool); err != nil {
				return requeueWithError(fmt.Errorf("finalizer failed: %w", err))
			}

			controllerutil.RemoveFinalizer(nodepool, pluginFinalizer)
			if err := r.Update(ctx, nodepool); err != nil {
				return requeueWithError(fmt.Errorf("failed to update nodepool CR after removing finalizer: %w", err))
			}

			return
		}
	}

	if !controllerutil.ContainsFinalizer(nodepool, pluginFinalizer) {
		controllerutil.AddFinalizer(nodepool, pluginFinalizer)
		if err := r.Update(ctx, nodepool); err != nil {
			return requeueWithError(fmt.Errorf("failed to update nodepool CR after adding finalizer: %w", err))
		}
	}

	return r.handleNodePoolObject(ctx, nodepool)
}

type NodePoolFSMAction int

const (
	NodePoolFSMCreate = iota
	NodePoolFSMProcessing
	NodePoolFSMNoop
)

func (r *NodePoolReconciler) determineAction(ctx context.Context, nodepool *hwmgmtv1alpha1.NodePool) NodePoolFSMAction {
	if len(nodepool.Status.Conditions) == 0 {
		r.Logger.InfoContext(ctx, "Handling Create NodePool request, name="+nodepool.Name)
		return NodePoolFSMCreate
	}

	provisionedCondition := meta.FindStatusCondition(
		nodepool.Status.Conditions,
		string(hwmgmtv1alpha1.Provisioned))
	if provisionedCondition != nil {
		if provisionedCondition.Status == metav1.ConditionTrue {
			r.Logger.InfoContext(ctx, "NodePool request in Provisioned state, name="+nodepool.Name)
			return NodePoolFSMNoop
		}

		return NodePoolFSMProcessing
	}

	return NodePoolFSMNoop
}

func (r *NodePoolReconciler) handleNodePoolCreate(
	ctx context.Context, nodepool *hwmgmtv1alpha1.NodePool) (ctrl.Result, error) {
	if err := r.hwmgr.CreateNodePool(ctx, nodepool); err != nil {
		r.Logger.Error("failed createNodePool", "err", err)
		utils.SetStatusCondition(&nodepool.Status.Conditions,
			hwmgmtv1alpha1.Provisioned,
			hwmgmtv1alpha1.Failed,
			metav1.ConditionFalse,
			"Creation request failed: "+err.Error())
	} else {
		// Update the condition
		utils.SetStatusCondition(&nodepool.Status.Conditions,
			hwmgmtv1alpha1.Provisioned,
			hwmgmtv1alpha1.InProgress,
			metav1.ConditionFalse,
			"Handling creation")
	}

	if updateErr := utils.UpdateK8sCRStatus(ctx, r.Client, nodepool); updateErr != nil {
		return requeueWithMediumInterval(),
			fmt.Errorf("failed to update status for NodePool %s: %w", nodepool.Name, updateErr)
	}

	return doNotRequeue(), nil
}

func (r *NodePoolReconciler) handleNodePoolProcessing(
	ctx context.Context, nodepool *hwmgmtv1alpha1.NodePool) (ctrl.Result, error) {
	full, err := r.hwmgr.CheckNodePoolProgress(ctx, nodepool)
	if err != nil {
		return requeueWithError(fmt.Errorf("failed CheckNodePoolProgress: %w", err))
	}

	allocatedNodes, err := r.hwmgr.GetAllocatedNodes(ctx, nodepool)
	if err != nil {
		return requeueWithError(fmt.Errorf("failed to get allocated nodes for %s: %w", nodepool.Name, err))
	}
	nodepool.Status.Properties.NodeNames = allocatedNodes

	var result ctrl.Result

	if full {
		r.Logger.InfoContext(ctx, "NodePool request is fully allocated, name="+nodepool.Name)

		utils.SetStatusCondition(&nodepool.Status.Conditions,
			hwmgmtv1alpha1.Provisioned,
			hwmgmtv1alpha1.Completed,
			metav1.ConditionTrue,
			"Created")

		result = doNotRequeue()
	} else {
		r.Logger.InfoContext(ctx, "NodePool request in progress, name="+nodepool.Name)
		result = requeueWithShortInterval()
	}

	if updateErr := utils.UpdateK8sCRStatus(ctx, r.Client, nodepool); updateErr != nil {
		return requeueWithError(fmt.Errorf("failed to update status for NodePool %s: %w", nodepool.Name, updateErr))
	}

	return result, nil
}

func (r *NodePoolReconciler) handleNodePoolObject(
	ctx context.Context, nodepool *hwmgmtv1alpha1.NodePool) (result ctrl.Result, err error) {
	result = doNotRequeue()

	switch r.determineAction(ctx, nodepool) {
	case NodePoolFSMCreate:
		return r.handleNodePoolCreate(ctx, nodepool)
	case NodePoolFSMProcessing:
		return r.handleNodePoolProcessing(ctx, nodepool)
	case NodePoolFSMNoop:
		// Nothing to do
		return
	}

	return
}

func (r *NodePoolReconciler) finalizer(ctx context.Context, nodepool *hwmgmtv1alpha1.NodePool) error {
	r.Logger.InfoContext(ctx, "Finalizing nodepool", "name", nodepool.Name)

	if err := r.hwmgr.ReleaseNodePool(ctx, nodepool); err != nil {
		return fmt.Errorf("failed to release nodepool %s: %w", nodepool.Name, err)
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *NodePoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	ctx := context.TODO()

	if hwmgr, err := service.NewHwMgrService().
		SetClient(mgr.GetClient()).
		SetLogger(r.Logger).
		Build(ctx); err != nil {
		return fmt.Errorf("failed to create HwMgrService: %w", err)
	} else {
		r.hwmgr = hwmgr
	}

	if err := ctrl.NewControllerManagedBy(mgr).
		For(&hwmgmtv1alpha1.NodePool{}).
		Complete(r); err != nil {
		return fmt.Errorf("failed to create controller: %w", err)
	}

	return nil
}
