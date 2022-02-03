package cassandra

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"k8s.io/utils/pointer"

	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
)

const (
	SystemReplicationDcNames = "-Dcassandra.system_distributed_replication_dc_names"
	SystemReplicationFactor  = "-Dcassandra.system_distributed_replication_per_dc"
	allowAlterRf             = "-Dcassandra.allow_alter_rf_during_range_movement=true"
)

// config is an internal type that is intended to be marshaled into JSON that is a valid
// value for CassandraDatacenter.Spec.Config.
type config struct {
	CassYamlIR
	cassandraVersion     string
	jvmOptions           jvmOptions
	additionalJvmOptions []string
}

// CassYamlIR is an internal representation of the cassandra.yaml. It is required because we want to make some options (esp. start_rpc) invisible to the user,
// but some of those options still need to be rendered out into the final cassandra.yaml.
type CassYamlIR struct {
	api.CassandraYaml `json:",inline,omitempty"`
	StartRpc          *bool `json:"start_rpc,omitempty"`
}

// jvmOptions is an internal type that is intended to be marshaled into JSON that is valid
// for the jvm options portion of the value supplied to CassandraDatacenter.Spec.Config.
type jvmOptions struct {
	InitialHeapSize *int64 `json:"initial_heap_size,omitempty"`
	MaxHeapSize     *int64 `json:"max_heap_size,omitempty"`
	HeapNewGenSize  *int64 `json:"heap_size_young_generation,omitempty"`
}

func IsCassandra3(version string) bool {
	return strings.HasPrefix(version, "3.")
}

func isCassandra4(version string) bool {
	return strings.HasPrefix(version, "4.")
}

func (c config) MarshalJSON() ([]byte, error) {
	config := make(map[string]interface{})

	// Even though we default to Cassandra's stock defaults for num_tokens, we need to
	// explicitly set it because the config builder defaults to num_tokens: 1
	if c.NumTokens == nil {
		if isCassandra4(c.cassandraVersion) {
			c.NumTokens = pointer.Int(16)
		} else {
			c.NumTokens = pointer.Int(256)
		}
	}

	config["cassandra-yaml"] = c.CassYamlIR

	if c.jvmOptions.InitialHeapSize != nil || c.jvmOptions.MaxHeapSize != nil || c.jvmOptions.HeapNewGenSize != nil {
		if isCassandra4(c.cassandraVersion) {
			config["jvm-server-options"] = c.jvmOptions
		} else {
			config["jvm-options"] = c.jvmOptions
		}
	}

	// Many config-builder templates accept a parameter called "additional-jvm-opts", e.g the jvm-options or the
	// cassandra-env-sh templates; it is safer to include this parameter in cassandra-env-sh, so that we can guarantee
	// that our options will be applied last and will override whatever options were specified by default.
	if c.additionalJvmOptions != nil {
		config["cassandra-env-sh"] = map[string]interface{}{
			"additional-jvm-opts": c.additionalJvmOptions,
		}
	}

	return json.Marshal(&config)
}

// NewIRYaml returns a new Cassandra Yaml with all internal fields set to their mandatory values. For example, StartRpc must always be set false,
// NewIRYaml ensures this happens.
func NewIRYaml(cassAPIYaml api.CassandraYaml) CassYamlIR {
	return CassYamlIR{
		CassandraYaml: cassAPIYaml,
		StartRpc:      pointer.Bool(false),
	}
}

func newConfig(apiConfig api.CassandraConfig, cassandraVersion string, encryptionStoresSecrets EncryptionStoresPasswords) (config, error) {
	// Filters out config element which do not exist in the Cassandra version in use
	apiConfig = addEncryptionOptions(&apiConfig, encryptionStoresSecrets, cassandraVersion)
	apiConfig = *apiConfig.DeepCopy()
	irCfgYaml := NewIRYaml(apiConfig.CassandraYaml)
	filterConfigForVersion(cassandraVersion, &irCfgYaml)
	cfg := config{CassYamlIR: irCfgYaml, cassandraVersion: cassandraVersion}
	err := validateConfig(&irCfgYaml)

	if err != nil {
		return cfg, err
	}

	if apiConfig.JvmOptions.HeapSize != nil {
		heapSize := apiConfig.JvmOptions.HeapSize.Value()
		cfg.jvmOptions.InitialHeapSize = &heapSize
		cfg.jvmOptions.MaxHeapSize = &heapSize
	}

	if apiConfig.JvmOptions.HeapNewGenSize != nil {
		newGenSize := apiConfig.JvmOptions.HeapNewGenSize.Value()
		cfg.jvmOptions.HeapNewGenSize = &newGenSize
	}

	cfg.additionalJvmOptions = apiConfig.JvmOptions.AdditionalOptions

	return cfg, nil
}

func addEncryptionOptions(apiConfig *api.CassandraConfig, encryptionStoresSecrets EncryptionStoresPasswords, cassandraVersion string) api.CassandraConfig {
	updatedConfig := apiConfig.DeepCopy()
	if updatedConfig.CassandraYaml.ClientEncryptionOptions == nil && updatedConfig.CassandraYaml.ServerEncryptionOptions == nil {
		return *updatedConfig
	}

	if updatedConfig.CassandraYaml.ClientEncryptionOptions != nil {
		if updatedConfig.CassandraYaml.ClientEncryptionOptions.Enabled {
			keystorePath := fmt.Sprintf("%s/%s", StoreMountFullPath("client", "keystore"), "keystore")
			truststorePath := fmt.Sprintf("%s/%s", StoreMountFullPath("client", "truststore"), "truststore")
			updatedConfig.CassandraYaml.ClientEncryptionOptions.Keystore = pointer.String(keystorePath)
			updatedConfig.CassandraYaml.ClientEncryptionOptions.Truststore = pointer.String(truststorePath)
			updatedConfig.CassandraYaml.ClientEncryptionOptions.KeystorePassword = &encryptionStoresSecrets.ClientKeystorePassword
			updatedConfig.CassandraYaml.ClientEncryptionOptions.TruststorePassword = &encryptionStoresSecrets.ClientTruststorePassword
		}
		// The encryption stores shouldn't end up in the cassandra yaml, they are specific to k8ssandra
		updatedConfig.CassandraYaml.ClientEncryptionOptions.EncryptionStores = nil
	}
	if updatedConfig.CassandraYaml.ServerEncryptionOptions != nil {
		if *updatedConfig.CassandraYaml.ServerEncryptionOptions.Enabled {
			keystorePath := fmt.Sprintf("%s/%s", StoreMountFullPath("server", "keystore"), "keystore")
			truststorePath := fmt.Sprintf("%s/%s", StoreMountFullPath("server", "truststore"), "truststore")
			updatedConfig.CassandraYaml.ServerEncryptionOptions.Keystore = pointer.String(keystorePath)
			updatedConfig.CassandraYaml.ServerEncryptionOptions.Truststore = pointer.String(truststorePath)
			updatedConfig.CassandraYaml.ServerEncryptionOptions.KeystorePassword = &encryptionStoresSecrets.ServerKeystorePassword
			updatedConfig.CassandraYaml.ServerEncryptionOptions.TruststorePassword = &encryptionStoresSecrets.ServerTruststorePassword
		}
		// The encryption stores shouldn't end up in the cassandra yaml, they are specific to k8ssandra
		updatedConfig.CassandraYaml.ServerEncryptionOptions.EncryptionStores = nil
		if IsCassandra3(cassandraVersion) {
			// Remove properties that don't exist in Cassandra 3.x
			updatedConfig.CassandraYaml.ServerEncryptionOptions.Enabled = nil
		}
	}
	return *updatedConfig
}

// Some settings in Cassandra are using a float type, which isn't supported for CRDs.
// They were changed to use a string type, and we validate here that if set they can parse correctly to float.
func validateConfig(config *CassYamlIR) error {
	if config.CassandraYaml.CommitlogSyncBatchWindowInMs != nil {
		if _, err := strconv.ParseFloat(*config.CassandraYaml.CommitlogSyncBatchWindowInMs, 64); err != nil {
			return fmt.Errorf("CommitlogSyncBatchWindowInMs must be a valid float: %v", err)
		}
	}

	if config.CassandraYaml.DiskOptimizationEstimatePercentile != nil {
		if _, err := strconv.ParseFloat(*config.CassandraYaml.DiskOptimizationEstimatePercentile, 64); err != nil {
			return fmt.Errorf("DiskOptimizationEstimatePercentile must be a valid float: %v", err)
		}
	}

	if config.CassandraYaml.DynamicSnitchBadnessThreshold != nil {
		if _, err := strconv.ParseFloat(*config.CassandraYaml.DynamicSnitchBadnessThreshold, 64); err != nil {
			return fmt.Errorf("DynamicSnitchBadnessThreshold must be a valid float: %v", err)
		}
	}

	if config.CassandraYaml.MemtableCleanupThreshold != nil {
		if _, err := strconv.ParseFloat(*config.CassandraYaml.MemtableCleanupThreshold, 64); err != nil {
			return fmt.Errorf("MemtableCleanupThreshold must be a valid float: %v", err)
		}
	}

	if config.CassandraYaml.PhiConvictThreshold != nil {
		if _, err := strconv.ParseFloat(*config.CassandraYaml.PhiConvictThreshold, 64); err != nil {
			return fmt.Errorf("PhiConvictThreshold must be a valid float: %v", err)
		}
	}

	if config.CassandraYaml.RangeTombstoneListGrowthFactor != nil {
		if _, err := strconv.ParseFloat(*config.CassandraYaml.RangeTombstoneListGrowthFactor, 64); err != nil {
			return fmt.Errorf("RangeTombstoneListGrowthFactor must be a valid float: %v", err)
		}
	}

	if config.CassandraYaml.CommitlogSyncPeriodInMs != nil && config.CassandraYaml.CommitlogSyncBatchWindowInMs != nil {
		return fmt.Errorf("CommitlogSyncPeriodInMs and CommitlogSyncBatchWindowInMs are mutually exclusive")
	}
	return nil
}

// Filters out config element which do not exist in the Cassandra version in use
// Generated using the filter columns in the first sheet of https://docs.google.com/spreadsheets/d/1P0bw5avkppBnoLXY00qVQmntgx6UJQbUidZtHfCRp_c/edit?usp=sharing
func filterConfigForVersion(cassandraVersion string, cassandraYamlIR *CassYamlIR) {
	if IsCassandra3(cassandraVersion) {
		cassandraYamlIR.AllocateTokensForLocalReplicationFactor = nil
		cassandraYamlIR.AuditLoggingOptions = nil
		cassandraYamlIR.AuthReadConsistencyLevel = nil
		cassandraYamlIR.AuthWriteConsistencyLevel = nil
		cassandraYamlIR.AutoHintsCleanupEnabled = nil
		cassandraYamlIR.AutoOptimiseFullRepairStreams = nil
		cassandraYamlIR.AutoOptimiseIncRepairStreams = nil
		cassandraYamlIR.AutoOptimisePreviewRepairStreams = nil
		cassandraYamlIR.AutocompactionOnStartupEnabled = nil
		cassandraYamlIR.AutomaticSstableUpgrade = nil
		cassandraYamlIR.AvailableProcessors = nil
		cassandraYamlIR.BlockForPeersInRemoteDcs = nil
		cassandraYamlIR.BlockForPeersTimeoutInSecs = nil
		cassandraYamlIR.ClientErrorReportingExclusions = nil
		cassandraYamlIR.CommitlogSyncGroupWindowInMs = nil
		cassandraYamlIR.CompactionTombstoneWarningThreshold = nil
		cassandraYamlIR.ConcurrentMaterializedViewBuilders = nil
		cassandraYamlIR.ConcurrentValidations = nil
		cassandraYamlIR.ConsecutiveMessageErrorsThreshold = nil
		cassandraYamlIR.CorruptedTombstoneStrategy = nil
		cassandraYamlIR.DefaultKeyspaceRf = nil
		cassandraYamlIR.DenylistConsistencyLevel = nil
		cassandraYamlIR.DenylistInitialLoadRetrySeconds = nil
		cassandraYamlIR.DenylistMaxKeysPerTable = nil
		cassandraYamlIR.DenylistMaxKeysTotal = nil
		cassandraYamlIR.DenylistRefreshSeconds = nil
		cassandraYamlIR.DiagnosticEventsEnabled = nil
		cassandraYamlIR.EnableDenylistRangeReads = nil
		cassandraYamlIR.EnableDenylistReads = nil
		cassandraYamlIR.EnableDenylistWrites = nil
		cassandraYamlIR.EnablePartitionDenylist = nil
		cassandraYamlIR.EnableTransientReplication = nil
		cassandraYamlIR.FailureDetector = nil
		cassandraYamlIR.FileCacheEnabled = nil
		cassandraYamlIR.FlushCompression = nil
		cassandraYamlIR.FullQueryLoggingOptions = nil
		cassandraYamlIR.HintWindowPersistentEnabled = nil
		cassandraYamlIR.IdealConsistencyLevel = nil
		cassandraYamlIR.InitialRangeTombstoneListAllocationSize = nil
		cassandraYamlIR.InternodeApplicationReceiveQueueCapacityInBytes = nil
		cassandraYamlIR.InternodeApplicationReceiveQueueReserveEndpointCapacityInBytes = nil
		cassandraYamlIR.InternodeApplicationReceiveQueueReserveGlobalCapacityInBytes = nil
		cassandraYamlIR.InternodeApplicationSendQueueCapacityInBytes = nil
		cassandraYamlIR.InternodeApplicationSendQueueReserveEndpointCapacityInBytes = nil
		cassandraYamlIR.InternodeApplicationSendQueueReserveGlobalCapacityInBytes = nil
		cassandraYamlIR.InternodeErrorReportingExclusions = nil
		cassandraYamlIR.InternodeMaxMessageSizeInBytes = nil
		cassandraYamlIR.InternodeSocketReceiveBufferSizeInBytes = nil
		cassandraYamlIR.InternodeSocketSendBufferSizeInBytes = nil
		cassandraYamlIR.InternodeStreamingTcpUserTimeoutInMs = nil
		cassandraYamlIR.InternodeTcpConnectTimeoutInMs = nil
		cassandraYamlIR.InternodeTcpUserTimeoutInMs = nil
		cassandraYamlIR.KeyCacheMigrateDuringCompaction = nil
		cassandraYamlIR.KeyspaceCountWarnThreshold = nil
		cassandraYamlIR.MaxConcurrentAutomaticSstableUpgrades = nil
		cassandraYamlIR.MinimumKeyspaceRf = nil
		cassandraYamlIR.NativeTransportAllowOlderProtocols = nil
		cassandraYamlIR.NativeTransportIdleTimeoutInMs = nil
		cassandraYamlIR.NativeTransportMaxRequestsPerSecond = nil
		cassandraYamlIR.NativeTransportRateLimitingEnabled = nil
		cassandraYamlIR.NativeTransportReceiveQueueCapacityInBytes = nil
		cassandraYamlIR.NetworkAuthorizer = nil
		cassandraYamlIR.NetworkingCacheSizeInMb = nil
		cassandraYamlIR.PaxosCacheSizeInMb = nil
		cassandraYamlIR.PeriodicCommitlogSyncLagBlockInMs = nil
		cassandraYamlIR.RangeTombstoneListGrowthFactor = nil
		cassandraYamlIR.RejectRepairCompactionThreshold = nil
		cassandraYamlIR.RepairCommandPoolFullStrategy = nil
		cassandraYamlIR.RepairCommandPoolSize = nil
		cassandraYamlIR.RepairSessionSpaceInMb = nil
		cassandraYamlIR.RepairedDataTrackingForPartitionReadsEnabled = nil
		cassandraYamlIR.RepairedDataTrackingForRangeReadsEnabled = nil
		cassandraYamlIR.ReportUnconfirmedRepairedDataMismatches = nil
		cassandraYamlIR.SnapshotLinksPerSecond = nil
		cassandraYamlIR.SnapshotOnRepairedDataMismatch = nil
		cassandraYamlIR.StreamEntireSstables = nil
		cassandraYamlIR.StreamingConnectionsPerHost = nil
		cassandraYamlIR.TableCountWarnThreshold = nil
		cassandraYamlIR.TrackWarnings = nil
		cassandraYamlIR.TraverseAuthFromRoot = nil
		cassandraYamlIR.UseDeterministicTableId = nil
		cassandraYamlIR.UseOffheapMerkleTrees = nil
		cassandraYamlIR.ValidationPreviewPurgeHeadStartInSec = nil
	}
	if isCassandra4(cassandraVersion) {
		cassandraYamlIR.AuthReadConsistencyLevel = nil
		cassandraYamlIR.AuthWriteConsistencyLevel = nil
		cassandraYamlIR.AutoHintsCleanupEnabled = nil
		cassandraYamlIR.AvailableProcessors = nil
		cassandraYamlIR.ClientErrorReportingExclusions = nil
		cassandraYamlIR.CompactionTombstoneWarningThreshold = nil
		cassandraYamlIR.DefaultKeyspaceRf = nil
		cassandraYamlIR.DenylistConsistencyLevel = nil
		cassandraYamlIR.DenylistInitialLoadRetrySeconds = nil
		cassandraYamlIR.DenylistMaxKeysPerTable = nil
		cassandraYamlIR.DenylistMaxKeysTotal = nil
		cassandraYamlIR.DenylistRefreshSeconds = nil
		cassandraYamlIR.EnableDenylistRangeReads = nil
		cassandraYamlIR.EnableDenylistReads = nil
		cassandraYamlIR.EnableDenylistWrites = nil
		cassandraYamlIR.EnablePartitionDenylist = nil
		cassandraYamlIR.FailureDetector = nil
		cassandraYamlIR.HintWindowPersistentEnabled = nil
		cassandraYamlIR.IndexInterval = nil
		cassandraYamlIR.InternodeErrorReportingExclusions = nil
		cassandraYamlIR.InternodeRecvBuffSizeInBytes = nil
		cassandraYamlIR.InternodeSendBuffSizeInBytes = nil
		cassandraYamlIR.MinimumKeyspaceRf = nil
		cassandraYamlIR.NativeTransportMaxRequestsPerSecond = nil
		cassandraYamlIR.NativeTransportRateLimitingEnabled = nil
		cassandraYamlIR.OtcBacklogExpirationIntervalMs = nil
		cassandraYamlIR.PaxosCacheSizeInMb = nil
		cassandraYamlIR.RequestScheduler = nil
		cassandraYamlIR.RequestSchedulerId = nil
		cassandraYamlIR.RequestSchedulerOptions = nil
		cassandraYamlIR.StartRpc = nil
		cassandraYamlIR.StreamingSocketTimeoutInMs = nil
		cassandraYamlIR.ThriftFramedTransportSizeInMb = nil
		cassandraYamlIR.ThriftMaxMessageLengthInMb = nil
		cassandraYamlIR.ThriftPreparedStatementsCacheSizeMb = nil
		cassandraYamlIR.TrackWarnings = nil
		cassandraYamlIR.TraverseAuthFromRoot = nil
		cassandraYamlIR.UseDeterministicTableId = nil
	}
}

// ApplySystemReplication adds system properties to configure replication of system
// keyspaces.
func ApplySystemReplication(dcConfig *DatacenterConfig, replication SystemReplication) {
	dcNames := SystemReplicationDcNames + "=" + strings.Join(replication.Datacenters, ",")
	replicationFactor := SystemReplicationFactor + "=" + strconv.Itoa(replication.ReplicationFactor)
	// prepend instead of append, so that user-specified options take precedence
	dcConfig.CassandraConfig.JvmOptions.AdditionalOptions = append(
		[]string{dcNames, replicationFactor},
		dcConfig.CassandraConfig.JvmOptions.AdditionalOptions...,
	)
}

func AllowAlterRfDuringRangeMovement(dcConfig *DatacenterConfig) {
	// prepend instead of append, so that user-specified options take precedence
	dcConfig.CassandraConfig.JvmOptions.AdditionalOptions = append(
		[]string{allowAlterRf},
		dcConfig.CassandraConfig.JvmOptions.AdditionalOptions...,
	)
}

// CreateJsonConfig parses dcConfig into a raw JSON base64-encoded string. If config is nil
// then nil, nil is returned
func CreateJsonConfig(config api.CassandraConfig, cassandraVersion string, encryptionStoresSecrets EncryptionStoresPasswords) ([]byte, error) {
	cfg, err := newConfig(config, cassandraVersion, encryptionStoresSecrets)
	if err != nil {
		return nil, err
	}
	return json.Marshal(cfg)
}
