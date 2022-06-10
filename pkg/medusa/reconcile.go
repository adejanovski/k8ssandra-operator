package medusa

import (
	"bytes"
	"fmt"
	"text/template"

	"github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	k8ss "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	api "github.com/k8ssandra/k8ssandra-operator/apis/medusa/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/images"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/pointer"

	"github.com/go-logr/logr"
	cassandra "github.com/k8ssandra/k8ssandra-operator/pkg/cassandra"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	DefaultMedusaImageRepository = "adejanovski"
	DefaultMedusaImageName       = "medusa"
	//TODO:Needs to be changed when the image is updated and prior to merging the changes
	DefaultMedusaVersion         = "0.13-202206081103-2"
	DefaultMedusaPort            = 50051
	DefaultProbeInitialDelay     = 10
	DefaultProbeTimeout          = 1
	DefaultProbePeriod           = 10
	DefaultProbeSuccessThreshold = 1
	DefaultProbeFailureThreshold = 10
	MedusaBackupsVolumeName      = "medusa-backups"
	MedusaBackupsMountPath       = "/mnt/backups"
)

var (
	defaultMedusaImage = images.Image{
		Registry:   images.DefaultRegistry,
		Repository: DefaultMedusaImageRepository,
		Name:       DefaultMedusaImageName,
		Tag:        DefaultMedusaVersion,
	}
)

func CreateMedusaIni(kc *k8ss.K8ssandraCluster) string {
	medusaIniTemplate := `
    [cassandra]
    use_sudo = false
    {{- if .Spec.Medusa.CertificatesSecretRef.Name }}
    certfile = /etc/certificates/rootca.crt
    usercert = /etc/certificates/client.crt_signed
    userkey = /etc/certificates/client.key
    {{- end}}

    [storage]
    use_sudo_for_restore = false
    storage_provider = {{ .Spec.Medusa.StorageProperties.StorageProvider }}
    {{- if eq .Spec.Medusa.StorageProperties.StorageProvider "local" }}
    bucket_name = {{ .Name }}
    base_path = /mnt/backups
    {{- else }}
    bucket_name = {{ .Spec.Medusa.StorageProperties.BucketName }}
    {{- end }}
    key_file = /etc/medusa-secrets/credentials
    {{- if .Spec.Medusa.StorageProperties.Prefix }}
    prefix = {{ .Spec.Medusa.StorageProperties.Prefix }}
    {{- else }}
    prefix = {{ .Name }}
    {{- end }}
    max_backup_age = {{ .Spec.Medusa.StorageProperties.MaxBackupAge }}
    max_backup_count = {{ .Spec.Medusa.StorageProperties.MaxBackupCount }}
    {{- if .Spec.Medusa.StorageProperties.Region }}
    region = {{ .Spec.Medusa.StorageProperties.Region }}
    {{- end }}
    {{- if .Spec.Medusa.StorageProperties.Host }}
    host = {{ .Spec.Medusa.StorageProperties.Host }}
    {{- end }}
    {{- if .Spec.Medusa.StorageProperties.Port }}
    port = {{ .Spec.Medusa.StorageProperties.Port }}
    {{- end }}
    {{- if not .Spec.Medusa.StorageProperties.Secure }}
    secure = False
    {{- else }}
    secure = True
    {{- end }}
    {{- if .Spec.Medusa.StorageProperties.BackupGracePeriodInDays }}
    backup_grace_period_in_days = {{ .Spec.Medusa.StorageProperties.BackupGracePeriodInDays }}
    {{- end }}
    {{- if .Spec.Medusa.StorageProperties.ApiProfile }}
    api_profile = {{ .Spec.Medusa.StorageProperties.ApiProfile }}
    {{- end }}
    {{- if .Spec.Medusa.StorageProperties.TransferMaxBandwidth }}
    transfer_max_bandwidth = {{ .Spec.Medusa.StorageProperties.TransferMaxBandwidth }}
    {{- end }}
    {{- if .Spec.Medusa.StorageProperties.ConcurrentTransfers }}
    concurrent_transfers = {{ .Spec.Medusa.StorageProperties.ConcurrentTransfers }}
    {{- end }}
    {{- if .Spec.Medusa.StorageProperties.MultiPartUploadThreshold }}
    multi_part_upload_threshold = {{ .Spec.Medusa.StorageProperties.MultiPartUploadThreshold }}
    {{- end }}

    [grpc]
    enabled = 1

    [kubernetes]
    cassandra_url = http://127.0.0.1:8080/api/v0/ops/node/snapshots
    use_mgmt_api = 1
    enabled = 1

    [logging]
    level = DEBUG`

	t, err := template.New("ini").Parse(medusaIniTemplate)
	if err != nil {
		panic(err)
	}
	medusaIni := new(bytes.Buffer)
	err = t.Execute(medusaIni, kc)
	if err != nil {
		panic(err)
	}

	return medusaIni.String()
}

func CreateMedusaConfigMap(namespace, clusterName, medusaIni string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-medusa", clusterName),
			Namespace: namespace,
		},
		Data: map[string]string{
			"medusa.ini": medusaIni,
		},
	}
}

// Get the cassandra user secret name from the medusa spec or generate one based on the cluster name
func CassandraUserSecretName(medusaSpec *api.MedusaClusterTemplate, clusterName string) string {
	if medusaSpec.CassandraUserSecretRef.Name == "" {
		return fmt.Sprintf("%s-medusa", clusterName)
	}
	return medusaSpec.CassandraUserSecretRef.Name
}

func UpdateMedusaInitContainer(dcConfig *cassandra.DatacenterConfig, medusaSpec *api.MedusaClusterTemplate, logger logr.Logger) {
	restoreContainerIndex, found := cassandra.FindInitContainer(dcConfig.PodTemplateSpec, "medusa-restore")
	restoreContainer := &corev1.Container{Name: "medusa-restore"}
	if found {
		logger.Info("Found medusa-restore init container")
		// medusa-restore init container already exists, we may need to update it
		restoreContainer = dcConfig.PodTemplateSpec.Spec.InitContainers[restoreContainerIndex].DeepCopy()
	}
	setImage(medusaSpec.ContainerImage, restoreContainer)
	restoreContainer.SecurityContext = medusaSpec.SecurityContext
	restoreContainer.Env = medusaEnvVars(medusaSpec, dcConfig, logger, "RESTORE")
	restoreContainer.VolumeMounts = medusaVolumeMounts(medusaSpec, dcConfig, logger)

	if !found {
		logger.Info("Couldn't find medusa-restore init container")
		// medusa-restore init container doesn't exist, we need to add it
		// We'll add the server-config-init init container in first position so it initializes cassandra.yaml for Medusa to use.
		// The definition of that container will be completed later by cass-operator.
		serverConfigContainer := &corev1.Container{Name: "server-config-init"}
		dcConfig.PodTemplateSpec.Spec.InitContainers = append(dcConfig.PodTemplateSpec.Spec.InitContainers, *serverConfigContainer)
		dcConfig.PodTemplateSpec.Spec.InitContainers = append(dcConfig.PodTemplateSpec.Spec.InitContainers, *restoreContainer)
	} else {
		// Overwrite existing medusa-restore init container
		dcConfig.PodTemplateSpec.Spec.InitContainers[restoreContainerIndex] = *restoreContainer
	}
}

func GenerateMedusaMainContainer(dcConfig *cassandra.DatacenterConfig, medusaSpec *api.MedusaClusterTemplate, logger logr.Logger) *corev1.Container {
	// medusa container already exists, we may need to update it
	medusaContainer := &corev1.Container{Name: "medusa"}
	setImage(medusaSpec.ContainerImage, medusaContainer)
	medusaContainer.SecurityContext = medusaSpec.SecurityContext
	medusaContainer.Env = medusaEnvVars(medusaSpec, dcConfig, logger, "GRPC")
	medusaContainer.VolumeMounts = medusaVolumeMounts(medusaSpec, dcConfig, logger)

	medusaContainer.Ports = []corev1.ContainerPort{
		{
			Name:          "grpc",
			ContainerPort: DefaultMedusaPort,
			Protocol:      "TCP",
		},
	}

	medusaContainer.ReadinessProbe = generateMedusaProbe(medusaSpec.ReadinessProbe)

	medusaContainer.LivenessProbe = generateMedusaProbe(medusaSpec.LivenessProbe)

	return medusaContainer
}

func UpdateMedusaMainContainer(dcConfig *cassandra.DatacenterConfig, medusaContainer *corev1.Container, logger logr.Logger) {
	medusaContainerIndex, found := cassandra.FindContainer(dcConfig.PodTemplateSpec, "medusa")

	if !found {
		logger.Info("Creating medusa container")
		// medusa container doesn't exist, we need to add it
		dcConfig.PodTemplateSpec.Spec.Containers = append(dcConfig.PodTemplateSpec.Spec.Containers, *medusaContainer)
	} else {
		// Overwrite existing medusa container
		dcConfig.PodTemplateSpec.Spec.Containers[medusaContainerIndex] = *medusaContainer
	}
}

func generateMedusaProbe(configuredProbe *corev1.Probe) *corev1.Probe {
	probe := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"/bin/grpc_health_probe", fmt.Sprintf("--addr=:%d", DefaultMedusaPort)},
			},
		},
		InitialDelaySeconds: DefaultProbeInitialDelay,
		TimeoutSeconds:      DefaultProbeTimeout,
		PeriodSeconds:       DefaultProbePeriod,
		SuccessThreshold:    DefaultProbeSuccessThreshold,
		FailureThreshold:    DefaultProbeFailureThreshold,
	}

	if configuredProbe != nil {
		if configuredProbe.InitialDelaySeconds > 0 {
			probe.InitialDelaySeconds = configuredProbe.InitialDelaySeconds
		}
		if configuredProbe.TimeoutSeconds > 0 {
			probe.TimeoutSeconds = configuredProbe.TimeoutSeconds
		}
		if configuredProbe.PeriodSeconds > 0 {
			probe.PeriodSeconds = configuredProbe.PeriodSeconds
		}
		if configuredProbe.SuccessThreshold > 0 {
			probe.SuccessThreshold = configuredProbe.SuccessThreshold
		}
		if configuredProbe.FailureThreshold > 0 {
			probe.FailureThreshold = configuredProbe.FailureThreshold
		}
		if configuredProbe.ProbeHandler.Exec != nil {
			probe.ProbeHandler.Exec = configuredProbe.ProbeHandler.Exec
		}
	}

	return probe
}

// Creates a deployment for a standalone Medusa pod which will be used by the operator to interact directly with the storage backend.
// It allows such interactions before any Cassandra pod is created, to enable performing a restore on the first startup.
func StandaloneMedusaDeployment(medusaContainer *corev1.Container, clusterName, namespace string, logger logr.Logger) *appsv1.Deployment {
	medusaStandaloneContainer := *medusaContainer.DeepCopy()
	// The standalone medusa pod won't be able to resolve its own IP address using DNS entries
	medusaStandaloneContainer.Env = append(medusaStandaloneContainer.Env, corev1.EnvVar{Name: "MEDUSA_RESOLVE_IP_ADDRESSES", Value: "False"})
	medusaDeployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-medusa-standalone", clusterName),
			Namespace: namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: pointer.Int32(1),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": fmt.Sprintf("%s-medusa-standalone", clusterName),
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": fmt.Sprintf("%s-medusa-standalone", clusterName),
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						*medusaStandaloneContainer.DeepCopy(),
					},
					Volumes: []corev1.Volume{},
				},
			},
		},
	}

	// Create dummy additional volumes
	for _, extraVolume := range [](string){"server-config", "server-data", MedusaBackupsVolumeName} {
		medusaDeployment.Spec.Template.Spec.Volumes = append(medusaDeployment.Spec.Template.Spec.Volumes, corev1.Volume{
			Name: extraVolume,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		})
	}

	return medusaDeployment
}

// Create a service for the Medusa standalone pod
func StandaloneMedusaService(dcConfig *cassandra.DatacenterConfig, medusaSpec *api.MedusaClusterTemplate, clusterName, namespace string, logger logr.Logger) *corev1.Service {
	medusaService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      MedusaServiceName(clusterName),
			Namespace: namespace,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name:       "grpc",
					Port:       DefaultMedusaPort,
					TargetPort: intstr.FromInt(DefaultMedusaPort),
				},
			},
			Selector: map[string]string{
				"app": fmt.Sprintf("%s-medusa-standalone", clusterName),
			},
		},
	}

	return medusaService
}

// Build the image name and pull policy and add it to a medusa container definition
func setImage(containerImage *images.Image, container *corev1.Container) {
	image := containerImage.ApplyDefaults(defaultMedusaImage)
	container.Image = image.String()
	container.ImagePullPolicy = image.PullPolicy
}

func medusaVolumeMounts(medusaSpec *api.MedusaClusterTemplate, dcConfig *cassandra.DatacenterConfig, logger logr.Logger) []corev1.VolumeMount {
	volumeMounts := []corev1.VolumeMount{
		{ // Cassandra config volume
			Name:      "server-config",
			MountPath: "/etc/cassandra",
		},
		{ // Cassandra data volume
			Name:      "server-data",
			MountPath: "/var/lib/cassandra",
		},
		{ // Medusa config volume
			Name:      fmt.Sprintf("%s-medusa", dcConfig.Cluster),
			MountPath: "/etc/medusa",
		},
		{ // Pod info volume
			Name:      "podinfo",
			MountPath: "/etc/podinfo",
		},
	}

	// Mount client encryption certificates if the secret ref is provided.
	if medusaSpec.CertificatesSecretRef.Name != "" {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "certificates",
			MountPath: "/etc/certificates",
		})
	}

	if medusaSpec.StorageProperties.StorageProvider == "local" {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			// Medusa local backup storage volume
			Name:      MedusaBackupsVolumeName,
			MountPath: MedusaBackupsMountPath,
		})
	} else {
		// We're not using local storage for backups, which requires a secret with backend credentials
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			// Medusa storage secret volume
			Name:      medusaSpec.StorageProperties.StorageSecretRef.Name,
			MountPath: "/etc/medusa-secrets",
		})
	}

	return volumeMounts
}

func medusaEnvVars(medusaSpec *api.MedusaClusterTemplate, dcConfig *cassandra.DatacenterConfig, logger logr.Logger, mode string) []corev1.EnvVar {
	return []corev1.EnvVar{
		{
			Name:  "MEDUSA_MODE",
			Value: mode,
		},
		{Name: "MEDUSA_CQL_USERNAME",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: CassandraUserSecretName(medusaSpec, dcConfig.Cluster),
					},
					Key: "username",
				},
			},
		},
		{Name: "MEDUSA_CQL_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: CassandraUserSecretName(medusaSpec, dcConfig.Cluster),
					},
					Key: "password",
				},
			},
		},
	}
}

type medusaVolume struct {
	Volume      *corev1.Volume
	VolumeIndex int
	Exists      bool
}

type medusaAdditionalVolume struct {
	Volume      *v1beta1.AdditionalVolumes
	VolumeIndex int
	Exists      bool
}

// Create or update volumes for medusa
func UpdateMedusaVolumes(podTemplateSpec *corev1.PodTemplateSpec, medusaSpec *api.MedusaClusterTemplate, clusterName string, logger logr.Logger) []medusaAdditionalVolume {
	var newVolumes []medusaVolume
	var additionalVolumes []medusaAdditionalVolume
	// Medusa config volume, containing medusa.ini
	configVolumeIndex, found := cassandra.FindVolume(podTemplateSpec, fmt.Sprintf("%s-medusa", clusterName))
	configVolume := &corev1.Volume{
		Name: fmt.Sprintf("%s-medusa", clusterName),
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: fmt.Sprintf("%s-medusa", clusterName),
				},
			},
		},
	}

	newVolumes = append(newVolumes, medusaVolume{
		Volume:      configVolume,
		VolumeIndex: configVolumeIndex,
		Exists:      found,
	})

	// Medusa credentials volume using the referenced secret
	if medusaSpec.StorageProperties.StorageProvider != "local" {
		// We're not using local storage for backups, which requires a secret with backend credentials
		secretVolumeIndex, found := cassandra.FindVolume(podTemplateSpec, medusaSpec.StorageProperties.StorageSecretRef.Name)
		secretVolume := &corev1.Volume{
			Name: medusaSpec.StorageProperties.StorageSecretRef.Name,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: medusaSpec.StorageProperties.StorageSecretRef.Name,
				},
			},
		}

		newVolumes = append(newVolumes, medusaVolume{
			Volume:      secretVolume,
			VolumeIndex: secretVolumeIndex,
			Exists:      found,
		})

	} else {
		// We're using local storage for backups, which requires a volume for the local backup storage
		backupVolumeIndex, found := cassandra.FindVolume(podTemplateSpec, MedusaBackupsVolumeName)
		accessModes := []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
		storageClassName := "standard"
		storageSize := resource.MustParse("10Gi")
		if medusaSpec.StorageProperties.PodStorage != nil {
			if medusaSpec.StorageProperties.PodStorage.AccessModes != nil {
				accessModes = medusaSpec.StorageProperties.PodStorage.AccessModes
			}
			if medusaSpec.StorageProperties.PodStorage.StorageClassName != "" {
				storageClassName = medusaSpec.StorageProperties.PodStorage.StorageClassName
			}
			if !medusaSpec.StorageProperties.PodStorage.Size.IsZero() {
				storageSize = medusaSpec.StorageProperties.PodStorage.Size
			}
		}

		backupVolume := &v1beta1.AdditionalVolumes{
			Name:      MedusaBackupsVolumeName,
			MountPath: MedusaBackupsMountPath,
			PVCSpec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &storageClassName,
				AccessModes:      accessModes,
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: storageSize,
					},
				},
			},
		}

		additionalVolumes = append(additionalVolumes, medusaAdditionalVolume{
			Volume:      backupVolume,
			VolumeIndex: backupVolumeIndex,
			Exists:      found,
		})

	}

	// Pod info volume
	podInfoVolumeIndex, found := cassandra.FindVolume(podTemplateSpec, "podinfo")
	podInfoVolume := &corev1.Volume{
		Name: "podinfo",
		VolumeSource: corev1.VolumeSource{
			DownwardAPI: &corev1.DownwardAPIVolumeSource{
				Items: []corev1.DownwardAPIVolumeFile{
					{
						Path: "labels",
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: "metadata.labels",
						},
					},
				},
			},
		},
	}
	newVolumes = append(newVolumes, medusaVolume{
		Volume:      podInfoVolume,
		VolumeIndex: podInfoVolumeIndex,
		Exists:      found,
	})

	for _, volume := range newVolumes {
		cassandra.AddOrUpdateVolume(podTemplateSpec, volume.Volume, volume.VolumeIndex, volume.Exists)
	}

	// Encryption client certificates
	if medusaSpec.CertificatesSecretRef.Name != "" {
		encryptionClientVolumeIndex, found := cassandra.FindVolume(podTemplateSpec, "certificates")
		encryptionClientVolume := &corev1.Volume{
			Name: "certificates",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: medusaSpec.CertificatesSecretRef.Name,
				},
			},
		}

		cassandra.AddOrUpdateVolume(podTemplateSpec, encryptionClientVolume, encryptionClientVolumeIndex, found)
	}

	return additionalVolumes
}

func MedusaServiceName(clusterName string) string {
	return fmt.Sprintf("%s-medusa-service", clusterName)
}
