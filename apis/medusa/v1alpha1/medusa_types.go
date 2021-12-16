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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

type Storage struct {
	// The storage backend to use for the backups.
	// +kubebuilder:validation:Enum=local;google_storage;azure_blobs;s3;s3_compatible;s3_rgw;ibm_storage
	// +kubebuilder:validation:Required
	StorageProvider string `json:"storageProvider,omitempty"`

	// Kubernetes Secret that stores the key file for the storage provider's API.
	// If using 'local' storage, this value is ignored.
	// +optional
	StorageSecretRef string `json:"storageSecretRef,omitempty"`

	// The name of the bucket to use for the backups.
	// +kubebuilder:validation:Required
	BucketName string `json:"bucketName,omitempty"`

	// Name of the top level folder in the backup bucket.
	// If empty, the cluster name will be used.
	// +optional
	Prefix string `json:"prefix,omitempty"`

	// Maximum backup age that the purge process should observe.
	// +kubebuilder:default=0
	MaxBackupAge int `json:"maxBackupAge,omitempty"`

	// Maximum number of backups to keep (used by the purge process).
	// Default is unlimited.
	// +kubebuilder:default=0
	MaxBackupCount int `json:"maxBackupCount,omitempty"`

	// AWS Profile to use for authentication.
	// +optional
	ApiProfile string `json:"apiProfile,omitempty"`

	// Max upload bandwidth in MB/s.
	// Defaults to 50 MB/s.
	// +kubebuilder:default="50MB/s"
	// +optional
	TransferMaxBandwidth string `json:"transferMaxBandwidth,omitempty"`

	// Number of concurrent uploads.
	// Helps maximizing the speed of uploads but puts more pressure on the network.
	// Defaults to 1.
	// +kubebuilder:default=1
	// +optional
	ConcurrentTransfers int `json:"concurrentTransfers,omitempty"`

	// File size over which cloud specific cli tools are used for transfer.
	// Defaults to 100 MB.
	// +kubebuilder:default=104857600
	// +optional
	MultiPartUploadThreshold int `json:"multiPartUploadThreshold,omitempty"`

	// Host to connect to for the storage backend.
	// +optional
	Host string `json:"host,omitempty"`

	// Region of the storage bucket.
	// Defaults to "default".
	// +optional
	Region string `json:"region,omitempty"`

	// Port to connect to for the storage backend.
	// +optional
	Port int `json:"port,omitempty"`

	// Whether to use SSL for the storage backend.
	// +optional
	Secure bool `json:"secure,omitempty"`

	// Age after which orphan sstables can be deleted from the storage backend.
	// Protects from race conditions between purge and ongoing backups.
	// Defaults to 10 days.
	// +optional
	BackupGracePeriodInDays int `json:"backupGracePeriodInDays,omitempty"`
}

type ContainerImage struct {

	// The docker registry to use. Defaults to docker.io.
	// +kubebuilder:default="docker.io"
	// +optional
	Registry string `json:"registry,omitempty"`

	// The docker repository to use. Defaults to "stargateio".
	// +kubebuilder:default="k8ssandra"
	// +optional
	Repository string `json:"repository,omitempty"`

	// The image name to use.
	// version detected.
	// +kubebuilder:default="medusa"
	// +optional
	Name string `json:"name,omitempty"`

	// The image tag to use. Defaults to "latest" (but please note: "latest" is not a valid tag name for official
	// Stargate images from the stargateio Docker Hub repository).
	// +kubebuilder:default="latest"
	// +optional
	Tag string `json:"tag,omitempty"`

	// The image pull policy to use. Defaults to "Always" if the tag is "latest", otherwise to "IfNotPresent".
	// +optional
	// +kubebuilder:validation:Enum:=Always;IfNotPresent;Never
	PullPolicy corev1.PullPolicy `json:"pullPolicy,omitempty"`

	// The secret to use when pulling the image from private repositories.
	// +optional
	PullSecretRef *corev1.LocalObjectReference `json:"pullSecretRef,omitempty"`
}

func (in ContainerImage) GetRegistry() string {
	return in.Registry
}

func (in ContainerImage) GetRepository() string {
	return in.Repository
}

func (in ContainerImage) GetName() string {
	return in.Name
}

func (in ContainerImage) GetTag() string {
	return in.Tag
}

func (in ContainerImage) GetPullPolicy() corev1.PullPolicy {
	return in.PullPolicy
}

func (in ContainerImage) GetPullSecretRef() *corev1.LocalObjectReference {
	return in.PullSecretRef
}

type MedusaClusterTemplate struct {
	// MedusaContainerImage is the image characteristics to use for Medusa containers. Leave nil
	// to use a default image.
	// +optional
	ContainerImage *ContainerImage `json:"containerImage,omitempty"`

	// SecurityContext applied to the Medusa containers.
	// +optional
	SecurityContext *corev1.SecurityContext `json:"securityContext,omitempty"`

	// Defines the username and password that Medusa will use to authenticate CQL connections to Cassandra clusters.
	// These credentials will be automatically turned into CQL roles by cass-operator when bootstrapping the datacenter,
	// then passed to the Medusa instances, so that it can authenticate against nodes in the datacenter using CQL.
	// The secret must be in the same namespace as Cassandra and must contain two keys: "username" and "password".
	// +optional
	CassandraUserSecretRef string `json:"cassandraUserSecretRef,omitempty"`

	// Provides all storage backend related properties for backups.
	StorageProperties Storage `json:"storageProperties,omitempty"`
}

// MedusaSpec defines the desired state of Medusa.
type MedusaSpec struct {
	MedusaClusterTemplate `json:",inline"`
}
