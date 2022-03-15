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
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/go-logr/logr"
	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	medusav1alpha1 "github.com/k8ssandra/k8ssandra-operator/apis/medusa/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/config"
	"github.com/k8ssandra/k8ssandra-operator/pkg/medusa"
	"github.com/k8ssandra/k8ssandra-operator/pkg/shared"
	"github.com/k8ssandra/k8ssandra-operator/pkg/utils"
)

// MedusaBackupJobReconciler reconciles a MedusaBackupJob object
type MedusaBackupJobReconciler struct {
	*config.ReconcilerConfig
	client.Client
	Scheme *runtime.Scheme
	medusa.ClientFactory
}

// +kubebuilder:rbac:groups=medusa.k8ssandra.io,namespace="k8ssandra",resources=medusabackupjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=medusa.k8ssandra.io,namespace="k8ssandra",resources=medusabackupjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=medusa.k8ssandra.io,namespace="k8ssandra",resources=medusabackupjobs/finalizers,verbs=update
// +kubebuilder:rbac:groups="",namespace="k8ssandra",resources=pods;services,verbs=get;list;watch

func (r *MedusaBackupJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("medusabackupjob", req.NamespacedName)

	logger.Info("Starting reconciliation")

	instance := &medusav1alpha1.MedusaBackupJob{}
	err := r.Get(ctx, req.NamespacedName, instance)
	if err != nil {
		logger.Error(err, "Failed to get CassandraBackup")
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{RequeueAfter: r.DefaultDelay}, err
	}

	backup := instance.DeepCopy()

	cassdcKey := types.NamespacedName{Namespace: backup.Namespace, Name: backup.Spec.CassandraDatacenter}
	cassdc := &cassdcapi.CassandraDatacenter{}
	err = r.Get(ctx, cassdcKey, cassdc)
	if err != nil {
		logger.Error(err, "failed to get cassandradatacenter", "CassandraDatacenter", cassdcKey)
		return ctrl.Result{RequeueAfter: r.DefaultDelay}, err
	}

	pods, err := r.getCassandraDatacenterPods(ctx, cassdc, logger)
	if err != nil {
		logger.Error(err, "Failed to get datacenter pods")
		return ctrl.Result{RequeueAfter: r.DefaultDelay}, err
	}

	// If there is anything in progress, simply requeue the request
	if len(backup.Status.InProgress) > 0 {
		logger.Info("CassandraBackup is being processed already", "Backup", req.NamespacedName)
		return ctrl.Result{RequeueAfter: r.DefaultDelay}, nil
	}

	// If the backup is already finished, there is nothing to do.
	if medusaBackupFinished(backup) {
		logger.Info("Backup operation is already finished")
		return ctrl.Result{Requeue: false}, nil
	}

	// First check to see if the backup is already in progress
	if !backup.Status.StartTime.IsZero() {
		// If there is anything in progress, simply requeue the request
		if len(backup.Status.InProgress) > 0 {
			logger.Info("Backups already in progress")
			return ctrl.Result{RequeueAfter: r.DefaultDelay}, nil
		}

		logger.Info("backup complete")

		// The MedusaBackupJob is finished and we now need to perform a sync to create the corresponding MedusaBackup object.
		if !backup.Status.BackupSynced {
			backupSynced := false
			if requeue, err := r.syncBackups(ctx, backup); err != nil {
				logger.Error(err, "Failed to prepare restore")
				if requeue {
					return ctrl.Result{RequeueAfter: r.DefaultDelay}, err
				} else {
					return ctrl.Result{}, err
				}
			} else if requeue {
				// Operation is still in progress
				return ctrl.Result{RequeueAfter: r.DefaultDelay}, nil
			} else {
				backupSynced = true
				patch := client.MergeFrom(backup.DeepCopy())
				backup.Status.BackupSynced = true
				if err := r.Status().Patch(ctx, backup, patch); err != nil {
					logger.Error(err, "failed to patch status with backupSynced")
					return ctrl.Result{RequeueAfter: r.DefaultDelay}, err
				}
			}
			if !backupSynced {
				return ctrl.Result{RequeueAfter: r.DefaultDelay}, fmt.Errorf("failed to sync backups")
			}
		}

		// Set the finish time
		// Note that the time here is not accurate, but that is ok. For now we are just
		// using it as a completion marker.
		patch := client.MergeFrom(backup.DeepCopy())
		backup.Status.FinishTime = metav1.Now()
		if err := r.Status().Patch(ctx, backup, patch); err != nil {
			logger.Error(err, "failed to patch status with finish time")
			return ctrl.Result{RequeueAfter: r.DefaultDelay}, err
		}

		return ctrl.Result{Requeue: false}, nil
	}

	logger.Info("Backups have not been started yet")

	// Make sure that Medusa is deployed
	if !shared.IsMedusaDeployed(pods) {
		// TODO generate event and/or update status to indicate error condition
		logger.Error(medusa.BackupSidecarNotFound, "medusa is not deployed", "CassandraDatacenter", cassdcKey)
		return ctrl.Result{RequeueAfter: r.LongDelay}, medusa.BackupSidecarNotFound
	}

	patch := client.MergeFromWithOptions(backup.DeepCopy(), client.MergeFromWithOptimisticLock{})

	backup.Status.StartTime = metav1.Now()
	backup.Status.BackupSynced = false
	for _, pod := range pods {
		backup.Status.InProgress = append(backup.Status.InProgress, pod.Name)
	}

	if err := r.Status().Patch(ctx, backup, patch); err != nil {
		logger.Error(err, "Failed to patch status")
		// We received a stale object, requeue for next processing
		return ctrl.Result{RequeueAfter: r.DefaultDelay}, nil
	}

	logger.Info("Starting backups")
	// Do the actual backup in the background
	go func() {
		wg := sync.WaitGroup{}

		// Mutex to prevent concurrent updates to the backup.Status object
		backupMutex := sync.Mutex{}
		patch := client.MergeFrom(backup.DeepCopy())

		for _, p := range pods {
			pod := p
			wg.Add(1)
			go func() {
				logger.Info("starting backup", "CassandraPod", pod.Name)
				succeeded := false
				if err := doMedusaBackup(ctx, backup.ObjectMeta.Name, backup.Spec.Type, &pod, r.ClientFactory); err == nil {
					logger.Info("finished backup", "CassandraPod", pod.Name)
					succeeded = true
				} else {
					logger.Error(err, "backup failed", "CassandraPod", pod.Name)
				}
				backupMutex.Lock()
				defer backupMutex.Unlock()
				defer wg.Done()
				backup.Status.InProgress = utils.RemoveValue(backup.Status.InProgress, pod.Name)
				if succeeded {
					backup.Status.Finished = append(backup.Status.Finished, pod.Name)
				} else {
					backup.Status.Failed = append(backup.Status.Failed, pod.Name)
				}
			}()
		}
		wg.Wait()
		logger.Info("finished backup operations")
		if err := r.Status().Patch(context.Background(), backup, patch); err != nil {
			logger.Error(err, "failed to patch status", "Backup", fmt.Sprintf("%s/%s", backup.Name, backup.Namespace))
		}
	}()

	return ctrl.Result{RequeueAfter: r.DefaultDelay}, nil
}

func (r *MedusaBackupJobReconciler) getCassandraDatacenterPods(ctx context.Context, cassdc *cassdcapi.CassandraDatacenter, logger logr.Logger) ([]corev1.Pod, error) {
	podList := &corev1.PodList{}
	labels := client.MatchingLabels{cassdcapi.DatacenterLabel: cassdc.Name}
	if err := r.List(ctx, podList, labels); err != nil {
		logger.Error(err, "failed to get pods for cassandradatacenter", "CassandraDatacenter", cassdc.Name)
		return nil, err
	}

	pods := make([]corev1.Pod, 0)
	pods = append(pods, podList.Items...)

	return pods, nil
}

func (r *MedusaBackupJobReconciler) syncBackups(ctx context.Context, backup *medusav1alpha1.MedusaBackupJob) (bool, error) {
	// Create a prepare_restore medusa task to create the mapping files in each pod.
	// Returns true if the reconcile needs to be requeued, false otherwise.
	syncTaskName := backup.ObjectMeta.Name + "-sync"
	sync := &medusav1alpha1.MedusaTask{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: syncTaskName, Namespace: backup.ObjectMeta.Namespace}, sync); err != nil {
		if errors.IsNotFound(err) {
			// Create the sync task
			sync = &medusav1alpha1.MedusaTask{
				ObjectMeta: metav1.ObjectMeta{
					Name:      syncTaskName,
					Namespace: backup.ObjectMeta.Namespace,
				},
				Spec: medusav1alpha1.MedusaTaskSpec{
					Operation:           medusav1alpha1.OperationTypeSync,
					CassandraDatacenter: backup.Spec.CassandraDatacenter,
				},
			}
			if err := r.Client.Create(context.Background(), sync); err != nil {
				return true, err
			}
		} else {
			return true, err
		}
	} else {
		if !sync.Status.FinishTime.IsZero() {
			// Prepare is finished
			return false, nil
		}
		if len(sync.Status.InProgress) == 0 {
			// No more pods are running the task but finish time is not set.
			// This means the task failed.
			return false, fmt.Errorf("sync task failed for backup %s", backup.ObjectMeta.Name)
		}
	}
	// The operation is still in progress
	return true, nil
}

func doMedusaBackup(ctx context.Context, name string, backupType shared.BackupType, pod *corev1.Pod, clientFactory medusa.ClientFactory) error {
	addr := fmt.Sprintf("%s:%d", pod.Status.PodIP, shared.BackupSidecarPort)
	if medusaClient, err := clientFactory.NewClient(addr); err != nil {
		return err
	} else {
		defer medusaClient.Close()
		return medusaClient.CreateBackup(ctx, name, string(backupType))
	}
}

func medusaBackupFinished(backup *medusav1alpha1.MedusaBackupJob) bool {
	return !backup.Status.FinishTime.IsZero()
}

// SetupWithManager sets up the controller with the Manager.
func (r *MedusaBackupJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&medusav1alpha1.MedusaBackupJob{}).
		Complete(r)
}
