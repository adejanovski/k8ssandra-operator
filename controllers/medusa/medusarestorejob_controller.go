/*
Copyright 2021.

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

package medusa

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/google/uuid"
	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	medusav1alpha1 "github.com/k8ssandra/k8ssandra-operator/apis/medusa/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/cassandra"
	"github.com/k8ssandra/k8ssandra-operator/pkg/config"
	"github.com/k8ssandra/k8ssandra-operator/pkg/medusa"
)

// MedusaRestoreJobReconciler reconciles a MedusaRestoreJob object
type MedusaRestoreJobReconciler struct {
	*config.ReconcilerConfig
	client.Client
	Scheme *runtime.Scheme
	medusa.ClientFactory
}

// +kubebuilder:rbac:groups=medusa.k8ssandra.io,namespace="k8ssandra",resources=medusarestores,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups=medusa.k8ssandra.io,namespace="k8ssandra",resources=medusarestores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=medusa.k8ssandra.io,namespace="k8ssandra",resources=medusabackups,verbs=get;list;watch
// +kubebuilder:rbac:groups=cassandra.datastax.com,namespace="k8ssandra",resources=cassandradatacenters,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,namespace="k8ssandra",resources=statefulsets,verbs=list;watch

func (r *MedusaRestoreJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("medusarestorejob", req.NamespacedName)
	factory := medusa.NewFactory(r.Client, logger)
	request, result, err := factory.NewRestoreRequest(ctx, req.NamespacedName)

	if result != nil {
		return *result, err
	}

	if request.Restore.Status.StartTime.IsZero() {
		request.SetRestoreStartTime(metav1.Now())
		request.SetRestoreKey(uuid.New().String())
		request.SetRestorePrepared(false)
	}

	cassdcKey := types.NamespacedName{Namespace: request.Restore.Namespace, Name: request.Datacenter.Name}
	cassdc := &cassdcapi.CassandraDatacenter{}
	err = r.Get(ctx, cassdcKey, cassdc)
	if err != nil {
		logger.Error(err, "failed to get cassandradatacenter", "CassandraDatacenter", cassdcKey)
		return ctrl.Result{RequeueAfter: r.DefaultDelay}, err
	}

	// Prepare the restore by placing a mapping file in the Cassandra data volume.
	if !request.Restore.Status.RestorePrepared {
		restorePrepared := false
		if requeue, err := r.prepareRestore(ctx, request); err != nil {
			logger.Error(err, "Failed to prepare restore")
			if requeue {
				return r.applyUpdatesAndRequeue(ctx, request)
			} else {
				return ctrl.Result{}, err
			}
		} else if requeue {
			// Operation is still in progress
			return r.applyUpdatesAndRequeue(ctx, request)
		} else {
			restorePrepared = true
			request.SetRestorePrepared(restorePrepared)
		}
		if !restorePrepared {
			logger.Error(fmt.Errorf("failed to prepare restore"), request.Restore.Status.RestoreKey)
			return r.applyUpdatesAndRequeue(ctx, request)
		}
	}

	if request.Restore.Spec.Shutdown && request.Restore.Status.DatacenterStopped.IsZero() {
		if stopped := stopDatacenter(request); !stopped {
			return r.applyUpdatesAndRequeue(ctx, request)
		}
	}

	if err := updateRestoreInitContainer(request); err != nil {
		request.Log.Error(err, "The datacenter is not properly configured for backup/restore")
		// No need to requeue here because the datacenter is not properly configured for
		// backup/restore with Medusa.
		return ctrl.Result{}, err
	}

	complete, err := r.podTemplateSpecUpdateComplete(ctx, request)

	if err != nil {
		request.Log.Error(err, "Failed to check if datacenter update is complete")
		// Not going to bother applying updates here since we hit an error.
		return ctrl.Result{RequeueAfter: r.DefaultDelay}, err
	}

	if !complete {
		request.Log.Info("Waiting for datacenter update to complete")
		return r.applyUpdatesAndRequeue(ctx, request)
	}

	request.Log.Info("The datacenter has been updated")

	if request.Datacenter.Spec.Stopped {
		request.Log.Info("Starting the datacenter")
		request.Datacenter.Spec.Stopped = false

		return r.applyUpdatesAndRequeue(ctx, request)
	}

	if !cassandra.DatacenterReady(request.Datacenter) {
		request.Log.Info("Waiting for datacenter to come back online")
		return r.applyUpdatesAndRequeue(ctx, request)
	}

	request.SetRestoreFinishTime(metav1.Now())
	if err := r.applyUpdates(ctx, request); err != nil {
		return ctrl.Result{RequeueAfter: r.DefaultDelay}, err
	}

	request.Log.Info("The restore operation is complete")
	return ctrl.Result{}, nil
}

// applyUpdates patches the CassandraDatacenter if its spec has been updated and patches
// the MedusaRestoreJob if its status has been updated.
func (r *MedusaRestoreJobReconciler) applyUpdates(ctx context.Context, req *medusa.RestoreRequest) error {
	if req.DatacenterModified() {
		if err := r.Patch(ctx, req.Datacenter, req.GetDatacenterPatch()); err != nil {
			if errors.IsResourceExpired(err) {
				req.Log.Info("CassandraDatacenter version expired!")
			}
			req.Log.Error(err, "Failed to patch the CassandraDatacenter")
			return err
		}
	}

	if req.RestoreModified() {
		if err := r.Status().Patch(ctx, req.Restore, req.GetRestorePatch()); err != nil {
			req.Log.Error(err, "Failed to patch the MedusaRestoreJob")
			return err
		}
	}

	return nil
}

func (r *MedusaRestoreJobReconciler) applyUpdatesAndRequeue(ctx context.Context, req *medusa.RestoreRequest) (ctrl.Result, error) {
	if err := r.applyUpdates(ctx, req); err != nil {
		req.Log.Error(err, "Failed to apply updates")
		return ctrl.Result{RequeueAfter: r.DefaultDelay}, err
	}
	return ctrl.Result{RequeueAfter: r.DefaultDelay}, nil
}

// podTemplateSpecUpdateComplete checks that the pod template spec changes, namely the ones
// with the restore container, have been pushed down to the StatefulSets. Return true if
// the changes have been applied.
func (r *MedusaRestoreJobReconciler) podTemplateSpecUpdateComplete(ctx context.Context, req *medusa.RestoreRequest) (bool, error) {
	if updated := cassandra.DatacenterUpdatedAfter(req.Restore.Status.DatacenterStopped.Time, req.Datacenter); !updated {
		return false, nil
	}

	// It may not be sufficient to check for the update only via status conditions. We will
	// check the template spec of the StatefulSets to be certain that the update has been
	// applied. We do this in order to avoid an extra rolling restart after the
	// StatefulSets are scaled back up.

	statefulsetList := &appsv1.StatefulSetList{}
	labels := client.MatchingLabels{cassdcapi.ClusterLabel: req.Datacenter.Spec.ClusterName, cassdcapi.DatacenterLabel: req.Datacenter.Name}

	if err := r.List(ctx, statefulsetList, labels); err != nil {
		req.Log.Error(err, "Failed to get StatefulSets")
		return false, err
	}

	for _, statefulset := range statefulsetList.Items {
		container := getRestoreInitContainerFromStatefulSet(&statefulset)

		if container == nil {
			return false, nil
		}

		if !containerHasEnvVar(container, backupNameEnvVar, req.Backup.Spec.Name) {
			return false, nil
		}

		if !containerHasEnvVar(container, restoreKeyEnvVar, req.Restore.Status.RestoreKey) {
			return false, nil
		}
	}

	return true, nil
}

func (r *MedusaRestoreJobReconciler) prepareRestore(ctx context.Context, request *medusa.RestoreRequest) (bool, error) {
	// Create a prepare_restore medusa task to create the mapping files in each pod.
	// Returns true if the reconcile needs to be requeued, false otherwise.
	prepare := &medusav1alpha1.MedusaTask{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: request.Restore.Status.RestoreKey, Namespace: request.Restore.Namespace}, prepare); err != nil {
		if errors.IsNotFound(err) {
			// Create the sync task
			prepare = &medusav1alpha1.MedusaTask{
				ObjectMeta: metav1.ObjectMeta{
					Name:      request.Restore.Status.RestoreKey,
					Namespace: request.Restore.Namespace,
				},
				Spec: medusav1alpha1.MedusaTaskSpec{
					Operation:           medusav1alpha1.OperationTypePrepareRestore,
					CassandraDatacenter: request.Restore.Spec.CassandraDatacenter.Name,
					BackupName:          request.Backup.Spec.Name,
					RestoreKey:          request.Restore.Status.RestoreKey,
				},
			}
			if err := r.Client.Create(context.Background(), prepare); err != nil {
				return true, err
			}
		} else {
			return true, err
		}
	} else {
		if !prepare.Status.FinishTime.IsZero() {
			// Prepare is finished
			return false, nil
		}
		if len(prepare.Status.InProgress) == 0 {
			// No more pods are running the task but finish time is not set.
			// This means the task failed.
			return false, fmt.Errorf("prepare restore task failed for restore %s", request.Restore.Name)
		}
	}
	// The operation is still in progress
	return true, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MedusaRestoreJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&medusav1alpha1.MedusaRestoreJob{}).
		Complete(r)
}
