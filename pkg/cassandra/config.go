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
	cassandraYaml        api.CassandraJsonMapper
	cassandraVersion     string
	jvmOptions           jvmOptions
	additionalJvmOptions []string
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
	if c.cassandraYaml.NumTokens == nil {
		if isCassandra4(c.cassandraVersion) {
			c.cassandraYaml.NumTokens = pointer.Int(16)
		} else {
			c.cassandraYaml.NumTokens = pointer.Int(256)
		}
	}

	config["cassandra-yaml"] = c.cassandraYaml

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

func newConfig(apiConfig api.CassandraConfig, cassandraVersion string, encryptionStoresSecrets EncryptionStoresPasswords) (config, error) {
	// Filters out config element which do not exist in the Cassandra version in use
	filteredCassandraConfig := addEncryptionOptions(&apiConfig, encryptionStoresSecrets, cassandraVersion)
	filterConfigForVersion(cassandraVersion, &filteredCassandraConfig)
	cfg := config{cassandraYaml: filteredCassandraConfig, cassandraVersion: cassandraVersion}
	err := validateConfig(&filteredCassandraConfig)
	if err != nil {
		return cfg, err
	}

	cfg.cassandraYaml = filteredCassandraConfig

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

func addEncryptionOptions(apiConfig *api.CassandraConfig, encryptionStoresSecrets EncryptionStoresPasswords, cassandraVersion string) api.CassandraJsonMapper {
	var updatedConfig api.CassandraJsonMapper
	updatedConfig.CassandraYaml = apiConfig.CassandraYaml
	if updatedConfig.ClientEncryptionOptions == nil && updatedConfig.ServerEncryptionOptions == nil {
		return updatedConfig
	}

	if apiConfig.CassandraYaml.ClientEncryptionOptions != nil {
		if apiConfig.CassandraYaml.ClientEncryptionOptions.Enabled {
			keystorePath := fmt.Sprintf("%s/%s", StoreMountFullPath("client", "keystore"), "keystore")
			truststorePath := fmt.Sprintf("%s/%s", StoreMountFullPath("client", "truststore"), "truststore")
			updatedConfig.ClientEncryptionOptions.Keystore = &keystorePath
			updatedConfig.ClientEncryptionOptions.Truststore = &truststorePath
			updatedConfig.ClientEncryptionOptions.KeystorePassword = &encryptionStoresSecrets.ClientKeystorePassword
			updatedConfig.ClientEncryptionOptions.TruststorePassword = &encryptionStoresSecrets.ClientTruststorePassword
		}
	}
	if apiConfig.CassandraYaml.ServerEncryptionOptions != nil {
		if *apiConfig.CassandraYaml.ServerEncryptionOptions.Enabled {
			keystorePath := fmt.Sprintf("%s/%s", StoreMountFullPath("server", "keystore"), "keystore")
			truststorePath := fmt.Sprintf("%s/%s", StoreMountFullPath("server", "truststore"), "truststore")
			updatedConfig.ServerEncryptionOptions.Keystore = &keystorePath
			updatedConfig.ServerEncryptionOptions.Truststore = &truststorePath
			updatedConfig.ServerEncryptionOptions.KeystorePassword = &encryptionStoresSecrets.ServerKeystorePassword
			updatedConfig.ServerEncryptionOptions.TruststorePassword = &encryptionStoresSecrets.ServerTruststorePassword
		}
		// The encryption stores shouldn't end up in the cassandra yaml, they are specific to k8ssandra
		if IsCassandra3(cassandraVersion) {
			// Remove properties that don't exist in Cassandra 3.x
			updatedConfig.CassandraYaml.ServerEncryptionOptions.Enabled = nil
		}
	}
	return updatedConfig
}

// Some settings in Cassandra are using a float type, which isn't supported for CRDs.
// They were changed to use a string type, and we validate here that if set they can parse correctly to float.
func validateConfig(config *api.CassandraJsonMapper) error {
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
func filterConfigForVersion(cassandraVersion string, filteredConfig *api.CassandraJsonMapper) {
	if IsCassandra3(cassandraVersion) {
		filteredConfig.AllocateTokensForLocalReplicationFactor = nil
		filteredConfig.AuditLoggingOptions = nil
		filteredConfig.AuthReadConsistencyLevel = nil
		filteredConfig.AuthWriteConsistencyLevel = nil
		filteredConfig.AutoHintsCleanupEnabled = nil
		filteredConfig.AutoOptimiseFullRepairStreams = nil
		filteredConfig.AutoOptimiseIncRepairStreams = nil
		filteredConfig.AutoOptimisePreviewRepairStreams = nil
		filteredConfig.AutocompactionOnStartupEnabled = nil
		filteredConfig.AutomaticSstableUpgrade = nil
		filteredConfig.AvailableProcessors = nil
		filteredConfig.BlockForPeersInRemoteDcs = nil
		filteredConfig.BlockForPeersTimeoutInSecs = nil
		filteredConfig.ClientErrorReportingExclusions = nil
		filteredConfig.CommitlogSyncGroupWindowInMs = nil
		filteredConfig.CompactionTombstoneWarningThreshold = nil
		filteredConfig.ConcurrentMaterializedViewBuilders = nil
		filteredConfig.ConcurrentValidations = nil
		filteredConfig.ConsecutiveMessageErrorsThreshold = nil
		filteredConfig.CorruptedTombstoneStrategy = nil
		filteredConfig.DefaultKeyspaceRf = nil
		filteredConfig.DenylistConsistencyLevel = nil
		filteredConfig.DenylistInitialLoadRetrySeconds = nil
		filteredConfig.DenylistMaxKeysPerTable = nil
		filteredConfig.DenylistMaxKeysTotal = nil
		filteredConfig.DenylistRefreshSeconds = nil
		filteredConfig.DiagnosticEventsEnabled = nil
		filteredConfig.EnableDenylistRangeReads = nil
		filteredConfig.EnableDenylistReads = nil
		filteredConfig.EnableDenylistWrites = nil
		filteredConfig.EnablePartitionDenylist = nil
		filteredConfig.EnableTransientReplication = nil
		filteredConfig.FailureDetector = nil
		filteredConfig.FileCacheEnabled = nil
		filteredConfig.FlushCompression = nil
		filteredConfig.FullQueryLoggingOptions = nil
		filteredConfig.HintWindowPersistentEnabled = nil
		filteredConfig.IdealConsistencyLevel = nil
		filteredConfig.InitialRangeTombstoneListAllocationSize = nil
		filteredConfig.InternodeApplicationReceiveQueueCapacityInBytes = nil
		filteredConfig.InternodeApplicationReceiveQueueReserveEndpointCapacityInBytes = nil
		filteredConfig.InternodeApplicationReceiveQueueReserveGlobalCapacityInBytes = nil
		filteredConfig.InternodeApplicationSendQueueCapacityInBytes = nil
		filteredConfig.InternodeApplicationSendQueueReserveEndpointCapacityInBytes = nil
		filteredConfig.InternodeApplicationSendQueueReserveGlobalCapacityInBytes = nil
		filteredConfig.InternodeErrorReportingExclusions = nil
		filteredConfig.InternodeMaxMessageSizeInBytes = nil
		filteredConfig.InternodeSocketReceiveBufferSizeInBytes = nil
		filteredConfig.InternodeSocketSendBufferSizeInBytes = nil
		filteredConfig.InternodeStreamingTcpUserTimeoutInMs = nil
		filteredConfig.InternodeTcpConnectTimeoutInMs = nil
		filteredConfig.InternodeTcpUserTimeoutInMs = nil
		filteredConfig.KeyCacheMigrateDuringCompaction = nil
		filteredConfig.KeyspaceCountWarnThreshold = nil
		filteredConfig.MaxConcurrentAutomaticSstableUpgrades = nil
		filteredConfig.MinimumKeyspaceRf = nil
		filteredConfig.NativeTransportAllowOlderProtocols = nil
		filteredConfig.NativeTransportIdleTimeoutInMs = nil
		filteredConfig.NativeTransportMaxRequestsPerSecond = nil
		filteredConfig.NativeTransportRateLimitingEnabled = nil
		filteredConfig.NativeTransportReceiveQueueCapacityInBytes = nil
		filteredConfig.NetworkAuthorizer = nil
		filteredConfig.NetworkingCacheSizeInMb = nil
		filteredConfig.PaxosCacheSizeInMb = nil
		filteredConfig.PeriodicCommitlogSyncLagBlockInMs = nil
		filteredConfig.RangeTombstoneListGrowthFactor = nil
		filteredConfig.RejectRepairCompactionThreshold = nil
		filteredConfig.RepairCommandPoolFullStrategy = nil
		filteredConfig.RepairCommandPoolSize = nil
		filteredConfig.RepairSessionSpaceInMb = nil
		filteredConfig.RepairedDataTrackingForPartitionReadsEnabled = nil
		filteredConfig.RepairedDataTrackingForRangeReadsEnabled = nil
		filteredConfig.ReportUnconfirmedRepairedDataMismatches = nil
		filteredConfig.SnapshotLinksPerSecond = nil
		filteredConfig.SnapshotOnRepairedDataMismatch = nil
		filteredConfig.StreamEntireSstables = nil
		filteredConfig.StreamingConnectionsPerHost = nil
		filteredConfig.TableCountWarnThreshold = nil
		filteredConfig.TrackWarnings = nil
		filteredConfig.TraverseAuthFromRoot = nil
		filteredConfig.UseDeterministicTableId = nil
		filteredConfig.UseOffheapMerkleTrees = nil
		filteredConfig.ValidationPreviewPurgeHeadStartInSec = nil
	}
	if isCassandra4(cassandraVersion) {
		filteredConfig.AuthReadConsistencyLevel = nil
		filteredConfig.AuthWriteConsistencyLevel = nil
		filteredConfig.AutoHintsCleanupEnabled = nil
		filteredConfig.AvailableProcessors = nil
		filteredConfig.ClientErrorReportingExclusions = nil
		filteredConfig.CompactionTombstoneWarningThreshold = nil
		filteredConfig.DefaultKeyspaceRf = nil
		filteredConfig.DenylistConsistencyLevel = nil
		filteredConfig.DenylistInitialLoadRetrySeconds = nil
		filteredConfig.DenylistMaxKeysPerTable = nil
		filteredConfig.DenylistMaxKeysTotal = nil
		filteredConfig.DenylistRefreshSeconds = nil
		filteredConfig.EnableDenylistRangeReads = nil
		filteredConfig.EnableDenylistReads = nil
		filteredConfig.EnableDenylistWrites = nil
		filteredConfig.EnablePartitionDenylist = nil
		filteredConfig.FailureDetector = nil
		filteredConfig.HintWindowPersistentEnabled = nil
		filteredConfig.IndexInterval = nil
		filteredConfig.InternodeErrorReportingExclusions = nil
		filteredConfig.InternodeRecvBuffSizeInBytes = nil
		filteredConfig.InternodeSendBuffSizeInBytes = nil
		filteredConfig.MinimumKeyspaceRf = nil
		filteredConfig.NativeTransportMaxRequestsPerSecond = nil
		filteredConfig.NativeTransportRateLimitingEnabled = nil
		filteredConfig.OtcBacklogExpirationIntervalMs = nil
		filteredConfig.PaxosCacheSizeInMb = nil
		filteredConfig.RequestScheduler = nil
		filteredConfig.RequestSchedulerId = nil
		filteredConfig.RequestSchedulerOptions = nil
		filteredConfig.StreamingSocketTimeoutInMs = nil
		filteredConfig.ThriftFramedTransportSizeInMb = nil
		filteredConfig.ThriftMaxMessageLengthInMb = nil
		filteredConfig.ThriftPreparedStatementsCacheSizeMb = nil
		filteredConfig.TrackWarnings = nil
		filteredConfig.TraverseAuthFromRoot = nil
		filteredConfig.UseDeterministicTableId = nil
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
