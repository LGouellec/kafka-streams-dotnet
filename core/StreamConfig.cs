using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net
{
    /// <summary>
    /// Processing Guarantee enumeration used in <see cref="IStreamConfig.Guarantee"/>
    /// </summary>
    public enum ProcessingGuarantee
    {
        /// <summary>
        /// Config value for parameter <see cref="IStreamConfig.Guarantee"/> for at-least-once processing guarantees.
        /// </summary>
        AT_LEAST_ONCE,
        /// <summary>
        /// Config value for parameter <see cref="IStreamConfig.Guarantee"/> for exactly-once processing guarantees.
        /// </summary>
        EXACTLY_ONCE
    }

    /// <summary>
    /// 
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    internal class StreamConfigPropertyAttribute : Attribute
    {
        internal StreamConfigPropertyAttribute(string keyName)
            : this(keyName, false)
        {

        }
        
        internal StreamConfigPropertyAttribute(string keyName, bool readOnly)
        {
            KeyName = keyName;
            ReadOnly = readOnly;
        }
        
        public string KeyName { get; set; }
        public bool ReadOnly { get; set; }
    }
    
    /// <summary>
    /// Interface stream configuration for a <see cref="KafkaStream"/> instance.
    /// See <see cref="StreamConfig"/> to obtain implementation about this interface.
    /// You could develop your own implementation and get it in your <see cref="KafkaStream"/> instance.
    /// </summary>
    public interface IStreamConfig : ICloneable<IStreamConfig>
    {
        #region Methods 

        /// <summary>
        /// Add a new key/value configuration for a <see cref="KafkaStream"/> instance.
        /// The consumer/producer prefix can also be used to distinguish these custom config values passed to different clients with the same config name.
        /// <para>
        /// Example :
        /// StreamConfig config = new StreamConfig();
        /// config.Add(StreamConfig.MainConsumerPrefix("fetch.min.bytes"), 1000);
        /// config.Add(StreamConfig.RestoreConsumerPrefix("fetch.max.bytes"), 1000000);
        /// config.Add(StreamConfig.ProducerPrefix("acks"), Acks.All);
        /// </para>
        /// </summary>
        /// <param name="key">New key</param>
        /// <param name="value">New value</param>
        void AddConfig(string key, dynamic value);
        
        /// <summary>
        /// Get the configs to the <see cref="IProducer{TKey, TValue}"/>
        /// </summary>
        /// <returns>Return <see cref="ProducerConfig"/> for building <see cref="IProducer{TKey, TValue}"/> instance.</returns>
        ProducerConfig ToProducerConfig();

        /// <summary>
        /// Get the configs to the <see cref="IProducer{TKey, TValue}"/> with specific <paramref name="clientId"/>
        /// </summary>
        /// <param name="clientId">Producer client ID</param>
        /// <returns>Return <see cref="ProducerConfig"/> for building <see cref="IProducer{TKey, TValue}"/> instance.</returns>
        ProducerConfig ToProducerConfig(string clientId);

        /// <summary>
        /// Get the configs to the external <see cref="IProducer{TKey, TValue}"/> with specific <paramref name="clientId"/>
        /// </summary>
        /// <param name="clientId">Producer client ID</param>
        /// <returns>Return <see cref="ProducerConfig"/> for building <see cref="IProducer{TKey, TValue}"/> instance.</returns>
        ProducerConfig ToExternalProducerConfig(string clientId);

        /// <summary>
        /// Get the configs to the <see cref="IConsumer{TKey, TValue}"/>
        /// </summary>
        /// <returns>Return <see cref="ConsumerConfig"/> for building <see cref="IConsumer{TKey, TValue}"/> instance.</returns>
        ConsumerConfig ToConsumerConfig();

        /// <summary>
        /// Get the configs to the <see cref="IConsumer{TumerKey, TValue}"/> with specific <paramref name="clientId"/>
        /// </summary>
        /// <param name="clientId">Consumer client ID</param>
        /// <param name="override">Override the configuration or not</param>
        /// <returns>Return <see cref="ConsumerConfig"/> for building <see cref="IConsumer{TKey, TValue}"/> instance.</returns>
        ConsumerConfig ToConsumerConfig(string clientId, bool @override = true);
        
        /// <summary>
        /// Get the configs to the restore <see cref="IConsumer{TKey, TValue}"/> with specific <paramref name="clientId"/>.
        /// Restore consumer is using to restore persistent state store.
        /// </summary>
        /// <param name="clientId">Consumer client ID</param>
        /// <returns>Return <see cref="ConsumerConfig"/> for building <see cref="IConsumer{TKey, TValue}"/> instance.</returns>
        ConsumerConfig ToRestoreConsumerConfig(string clientId);

        /// <summary>
        /// Get the configs to the restore <see cref="IConsumer{TKey, TValue}"/> with specific <paramref name="clientId"/>.
        /// Global consumer is using to update the global state stores.
        /// </summary>
        /// <param name="clientId">Consumer client ID</param>
        /// <returns>Return <see cref="ConsumerConfig"/> for building <see cref="IConsumer{TKey, TValue}"/> instance.</returns>
        ConsumerConfig ToGlobalConsumerConfig(string clientId);

        /// <summary>
        /// Get the configs to the external <see cref="IConsumer{TumerKey, TValue}"/> with specific <paramref name="clientId"/>
        /// </summary>
        /// <param name="clientId">Consumer client ID</param>
        /// <returns>Return <see cref="ConsumerConfig"/> for building <see cref="IConsumer{TKey, TValue}"/> instance.</returns>
        ConsumerConfig ToExternalConsumerConfig(string clientId);

        /// <summary>
        /// Get the configs to the <see cref="IAdminClient"/> with specific <paramref name="clientId"/>
        /// </summary>
        /// <param name="clientId">Admin client ID</param>
        /// <returns>Return <see cref="AdminClientConfig"/> for building <see cref="IAdminClient"/> instance.</returns>
        AdminClientConfig ToAdminConfig(string clientId);

        /// <summary>
        /// Get the config value of the key. Null if any key found
        /// </summary>
        /// <param name="key">Key searched</param>
        /// <returns>Return the config value of the key, null otherwise</returns>
        dynamic Get(string key);

        #endregion

        #region Stream Config Property

        /// <summary>
        /// A Rocks DB config handler function
        /// </summary>
        Action<string, RocksDbOptions> RocksDbConfigHandler { get; set; }

        /// <summary>
        /// Inner exception handling function called during processing.
        /// </summary>
        Func<Exception, ExceptionHandlerResponse> InnerExceptionHandler { get; set; }

        /// <summary>
        /// Deserialization exception handling function called when deserialization exception during kafka consumption is raise.
        /// </summary>
        Func<ProcessorContext, ConsumeResult<byte[], byte[]>, Exception, ExceptionHandlerResponse> DeserializationExceptionHandler { get; set; }

        /// <summary>
        /// Production exception handling function called when kafka produce exception is raise.
        /// </summary>
        Func<DeliveryReport<byte[], byte[]>, ProductionExceptionHandlerResponse> ProductionExceptionHandler { get; set; }

        /// <summary>
        /// Maximum allowed time between calls to consume messages (e.g., rd_kafka_consumer_poll())
        /// for high-level consumers. If this interval is exceeded the consumer is considered
        /// failed and the group will rebalance in order to reassign the partitions to another
        /// consumer group member. Warning: Offset commits may be not possible at this point.
        /// Note: It is recommended to set `enable.auto.offset.store=false` for long-time
        /// processing applications and then explicitly store offsets (using offsets_store())
        /// *after* message processing, to make sure offsets are not auto-committed prior
        /// to processing has finished. The interval is checked two times per second. See
        /// KIP-62 for more information. default: 300000 importance: high
        /// </summary>
        int? MaxPollIntervalMs { get; set; }

        /// <summary>
        /// The maximum number of records returned in a polling phase. (Default: 500)
        /// </summary>
        long MaxPollRecords { get; set; }

        /// <summary>
        /// The maximum number of records returned in a polling restore phase. (Default: 1000)
        /// </summary>
        long MaxPollRestoringRecords { get; set; }

        /// <summary>
        /// The amount of time in milliseconds to block waiting for input.
        /// </summary>
        long PollMs { get; set; }

        /// <summary>
        /// The frequency with which to save the position of the processor. (Note, if <see cref="IStreamConfig.Guarantee"/> is set to <see cref="ProcessingGuarantee.EXACTLY_ONCE"/>, the default value is <see cref="StreamConfig.EOS_DEFAULT_COMMIT_INTERVAL_MS"/>,
        /// otherwise the default value is <see cref="StreamConfig.DEFAULT_COMMIT_INTERVAL_MS"/>)
        /// </summary>
        long CommitIntervalMs { get; set; }

        /// <summary>
        /// Timeout used for transaction related operations. (Default : 10 seconds).
        /// </summary>
        TimeSpan TransactionTimeout { get; set; }

        /// <summary>
        /// Enables the transactional producer. The transactional.id is used to identify the same transactional producer instance across process restarts.
        /// </summary>
        string TransactionalId { get; set; }

        /// <summary>
        /// An identifier for the stream processing application. Must be unique within the Kafka cluster. It is used as 1) the default client-id prefix, 2) the group-id for membership management, 3) the changelog topic prefix.
        /// </summary>
        string ApplicationId { get; set; }

        /// <summary>
        /// An ID prefix string used for the client IDs of internal consumer, producer and restore-consumer, with pattern '&lt;client.id&gt;-StreamThread-&lt;threadSequenceNumber&gt;-&lt;consumer|producer|restore-consumer&gt;'.
        /// </summary>
        string ClientId { get; set; }

        /// <summary>
        /// The number of threads to execute stream processing.
        /// </summary>
        int NumStreamThreads { get; set; }

        /// <summary>
        /// Default key serdes for consumer and materialized state store
        /// </summary>
        ISerDes DefaultKeySerDes { get; set; }

        /// <summary>
        /// Default value serdes for consumer and materialized state store
        /// </summary>
        ISerDes DefaultValueSerDes { get; set; }

        /// <summary>
        /// Default timestamp extractor class that implements the <see cref="ITimestampExtractor"/> interface.
        /// </summary>
        ITimestampExtractor DefaultTimestampExtractor { get; set; }

        /// <summary>
        /// The processing guarantee that should be used. Possible values are <see cref="ProcessingGuarantee.AT_LEAST_ONCE"/> (default) and <see cref="ProcessingGuarantee.EXACTLY_ONCE"/>. Note that exactly-once processing requires a cluster of at least three brokers by default what is the recommended setting for production; for development you can change this, by adjusting broker setting 'transaction.state.log.replication.factor' and 'transaction.state.log.min.isr'.
        /// </summary>
        ProcessingGuarantee Guarantee { get; set; }

        /// <summary>
        /// Initial list of brokers as a CSV list of broker host or host:port.
        /// </summary>
        string BootstrapServers { get; set; }

        /// <summary>
        /// Maximum amount of time a stream task will stay idle when not all of its partition buffers contain records, to avoid potential out-of-order record processing across multiple input streams. (Default: 0)
        /// </summary>
        long MaxTaskIdleMs { get; set; }

        /// <summary>
        /// Maximum number of records to buffer per partition. (Default: 1000)
        /// </summary>
        long BufferedRecordsPerPartition { get; set; }

        /// <summary>
        /// Authorize your streams application to follow metadata (timestamp, topic, partition, offset and headers) during processing record.
        /// You can use <see cref="StreamizMetadata"/> to get these metadatas. (Default : false)
        /// </summary>
        bool FollowMetadata { get; set; }

        /// <summary>
        /// Directory location for state store. This path must be unique for each streams instance sharing the same underlying filesystem.
        /// </summary>
        string StateDir { get; set; }

        /// <summary>
        /// The replication factor for change log topics topics created by the stream processing application. Default is -1, meaning default broker replication factor setting.
        /// </summary>
        int ReplicationFactor { get; set; }

        /// <summary>
        /// Added to a windows maintainMs to ensure data is not deleted from the log prematurely. Allows for clock drift. Default is 1 day.
        /// </summary>
        long WindowStoreChangelogAdditionalRetentionMs { get; set; }

        /// <summary>
        /// Manager which track offset saved in local state store
        /// </summary>
        IOffsetCheckpointManager OffsetCheckpointManager { get; set; }
        
        /// <summary>
        /// Logger factory which will be used for logging 
        /// </summary>
        ILoggerFactory Logger { get; set; }
        
        /// <summary>
        /// Delay between two invocations of MetricsReporter().
        /// Minimum and default value : 30 seconds
        /// </summary>
        long MetricsIntervalMs { get; set; }
        
        /// <summary>
        /// The reporter expose a list of sensors throw by a stream thread every <see cref="MetricsIntervalMs"/>.
        /// This reporter has the responsibility to export sensors and metrics into another platform.
        /// Streamiz package provide one reporter for Prometheus (see Streamiz.Kafka.Net.Metrics.Prometheus package).
        /// </summary>
        Action<IEnumerable<Sensor>> MetricsReporter { get; set; }
        
        /// <summary>
        /// Boolean which indicate if librdkafka handle statistics should be exposed ot not. (default: false)
        /// Only mainConsumer and producer will be concerned.
        /// </summary>
        bool ExposeLibrdKafkaStats { get; set; }
        
        /// <summary>
        /// The highest recording level for metrics.
        /// </summary>
        MetricsRecordingLevel MetricsRecording { get; set; }
        
        /// <summary>
        /// Time wait before completing the start task of <see cref="KafkaStream"/>. (default: 2000)
        /// </summary>
        long StartTaskDelayMs { get; set; }
        
        /// <summary>
        /// Enables parallel processing for messages (default: false)
        /// </summary>
        bool ParallelProcessing { get; set; }
        
        /// <summary>
        /// The max number of concurrent messages processing by thread. (default: 8)
        /// Only valid if ParallelProcessing is true
        /// </summary>
        int MaxDegreeOfParallelism { get; set; }
        
        /// <summary>
        /// Aims to log (info level) processing summary records per thread every X minutes. (default: 1 minute)
        /// </summary>
        TimeSpan LogProcessingSummary { get; set; }
        
        /// <summary>
        /// Maximum number of memory bytes to be used for state stores cache for a single store. (default: 5Mb)
        /// </summary>
        long StateStoreCacheMaxBytes { get; set; }
        
        #endregion
        
        #region Middlewares
        
        /// <summary>
        /// Add middleware
        /// </summary>
        /// <param name="item">New middleware</param>
        void AddMiddleware(IStreamMiddleware item);

        /// <summary>
        /// Clear all middlewares
        /// </summary>
        void ClearMiddleware();
        
        /// <summary>
        /// Remove a specific middleware
        /// </summary>
        /// <param name="item">middleware to remove</param>
        /// <returns></returns>
        bool RemoveMiddleware(IStreamMiddleware item);
        
        /// <summary>
        /// Get all middlewares
        /// </summary>
        IEnumerable<IStreamMiddleware> Middlewares { get; }
        
        #endregion
    }

    /// <summary>
    /// Implementation of <see cref="IStreamConfig"/>. Contains all configuration for your stream.
    /// By default, Kafka Streams does not allow users to overwrite the following properties (Streams setting shown in parentheses)
    ///    - EnableAutoCommit = (false) - Streams client will always disable/turn off auto committing
    ///    - EnableAutoOffsetStore = (false)
    /// If <see cref="IStreamConfig.Guarantee"/> is set to <see cref="ProcessingGuarantee.EXACTLY_ONCE"/>, Kafka Streams does not allow users to overwrite the following properties (Streams setting shown in parentheses):
    ///    - <see cref="IsolationLevel"/> (<see cref="IsolationLevel.ReadCommitted"/>) - Consumers will always read committed data only
    ///    - <see cref="EnableIdempotence"/> (true) - Producer will always have idempotency enabled
    ///    - <see cref="MaxInFlight"/> (5) - Producer will always have one in-flight request per connection
    /// If <see cref="IStreamConfig.Guarantee"/> is set to <see cref="ProcessingGuarantee.EXACTLY_ONCE"/>, Kafka Streams initialize the following properties :
    ///    - <see cref="CommitIntervalMs"/> (<see cref="EOS_DEFAULT_COMMIT_INTERVAL_MS"/>
    /// <exemple>
    /// <code>
    /// var config = new StreamConfig();
    /// config.ApplicationId = "test-app";
    /// config.BootstrapServers = "localhost:9092";
    /// </code>
    /// </exemple>
    /// </summary>
    public class StreamConfig : IStreamConfig, ISchemaRegistryConfig
    {
        private class KeyValueComparer : IEqualityComparer<KeyValuePair<string, string>>
        {
            public bool Equals(KeyValuePair<string, string> x, KeyValuePair<string, string> y)
                => x.Key.Equals(y.Key);

            public int GetHashCode(KeyValuePair<string, string> obj)
                => obj.Key.GetHashCode();
        }
        
        #region Not used for moment

        /// private string applicationServerCst = "application.server";
        /// private string topologyOptimizationCst = "topology.optimization";
        /// private string cacheMaxBytesBufferingCst = "cache.max.bytes.buffering";
        /// private string rocksdbConfigSetterCst = "rocksdb.config.setter";
        /// private string stateCleanupDelayMsCst = "state.cleanup.delay.ms";
        /// private string pollMsCst = "poll.ms";
        /// private string processingGuaranteeCst = "processing.guarantee";

        /// public static readonly string AT_LEAST_ONCE = "at_least_once";
        /// public static readonly string EXACTLY_ONCE = "exactly_once";

        /// private string Optimize
        /// {
        ///     get => configProperties[topologyOptimizationCst];
        ///     set => configProperties.AddOrUpdate(topologyOptimizationCst, value);
        /// }

        /// private string ApplicationServer
        /// {
        ///     get => configProperties[applicationServerCst];
        ///     set => configProperties.AddOrUpdate(applicationServerCst, value);
        /// }

        /// private string ProcessingGuaranteeConfig
        /// {
        ///     get => configProperties[processingGuaranteeCst];
        ///     set
        ///     {
        ///         if (value.Equals(AT_LEAST_ONCE) || value.Equals(EXACTLY_ONCE))
        ///             configProperties.AddOrUpdate(processingGuaranteeCst, value);
        ///         else
        ///             throw new InvalidOperationException($"ProcessingGuaranteeConfig value must equal to {AT_LEAST_ONCE} or {EXACTLY_ONCE}");
        ///     }
        /// }

        /// private long PollMsConfig
        /// {
        ///     get => Convert.ToInt64(configProperties[pollMsCst]);
        ///     set => configProperties.AddOrUpdate(pollMsCst, value.ToString());
        /// }

        /// private long StateCleanupDelayMs
        /// {
        ///     get => Convert.ToInt64(configProperties[stateCleanupDelayMsCst]);
        ///     set => configProperties.AddOrUpdate(stateCleanupDelayMsCst, value.ToString());
        /// }

        /// private long CacheMaxBytesBuffering
        /// {
        ///     get => Convert.ToInt64(configProperties[cacheMaxBytesBufferingCst]);
        ///     set => configProperties.AddOrUpdate(cacheMaxBytesBufferingCst, value.ToString());
        /// }

        #endregion

        #region Config constants

        private const string schemaRegistryUrlCst = "schema.registry.url";
        private const string schemaRegistryBasicAuthUserInfoCst = "schema.registry.basic.auth.user.info";
        private const string schemaRegistryBasicAuthCredentialSourceCst = "schema.registry.basic.auth.credentials.source";
        private const string schemaRegistryRequestTimeoutMsCst = "schema.registry.request.timeout.ms";
        private const string schemaRegistryMaxCachedSchemasCst = "schema.registry.max.cached.schemas";
        private const string avroSerializerAutoRegisterSchemasCst = "avro.serializer.auto.register.schemas";
        private const string avroSerializerSubjectNameStrategyCst = "avro.serializer.subject.name.strategy";
        private const string avroSerializerUseLatestVersionCst = "avro.serializer.use.latest.version";
        private const string avroSerializerBufferBytesCst = "avro.serializer.buffer.bytes";
        private const string protobufAutoRegisterSchemasCst = "protobuf.serializer.auto.register.schemas";
        private const string protobufSerializerBufferBytesCst = "protobuf.serializer.buffer.bytes";
        private const string protobufSerializerUseLatestVersionCst = "protobuf.serializer.use.latest.version";
        private const string protobufSerializerSkipKnownTypesCst = "protobuf.serializer.skip.known.types";
        private const string protobufSerializerUseDeprecatedFormatCst = "protobuf.serializer.use.deprecated.format";
        private const string protobufSerializerSubjectNameStrategyCst = "protobuf.serializer.subject.name.strategy";
        private const string protobufSerializerReferenceSubjectNameStrategyCst = "protobuf.serializer.reference.subject.name.strategy";
        private const string applicatonIdCst = "application.id";
        private const string clientIdCst = "client.id";
        private const string numStreamThreadsCst = "num.stream.threads";
        private const string defaultKeySerDesCst = "default.key.serdes";
        private const string defaultValueSerDesCst = "default.value.serdes";
        private const string defaultTimestampExtractorCst = "default.timestamp.extractor";
        private const string processingGuaranteeCst = "processing.guarantee";
        private const string transactionTimeoutCst = "transaction.timeout";
        private const string commitIntervalMsCst = "commit.interval.ms";
        private const string pollMsCst = "poll.ms";
        private const string maxPollRecordsCst = "max.poll.records";
        private const string maxPollRestoringRecordsCst = "max.poll.restoring.records";
        private const string maxTaskIdleCst = "max.task.idle.ms";
        private const string bufferedRecordsPerPartitionCst = "buffered.records.per.partition";
        private const string followMetadataCst = "follow.metadata";
        private const string stateDirCst = "state.dir";
        private const string replicationFactorCst = "replication.factor";
        private const string windowstoreChangelogAdditionalRetentionMsCst = "windowstore.changelog.additional.retention.ms";
        private const string offsetCheckpointManagerCst = "offset.checkpoint.manager";
        private const string metricsReportCst = "metrics.reporter";
        private const string metricsIntervalMsCst = "metrics.interval.ms";
        private const string exposeLibrdKafkaCst = "expose.librdkafka.stats";
        private const string metricsRecordingLevelCst = "metrics.recording.level";
        private const string startTaskDelayMsCst = "start.task.delay.ms";
        private const string parallelProcessingCst = "parallel.processing";
        private const string maxDegreeOfParallelismCst = "max.degree.of.parallelism";
        private const string rocksDbConfigSetterCst = "rocksdb.config.setter";
        private const string innerExceptionHandlerCst = "inner.exception.handler";
        private const string deserializationExceptionHandlerCst = "deserialization.exception.handler";
        private const string productionExceptionHandlerCst = "production.exception.handler";
        private const string logProcessingSummaryCst = "log.processing.summary";
        private const string stateStoreCacheMaxBytesCst = "statestore.cache.max.bytes";
        
        /// <summary>
        /// Default commit interval in milliseconds when exactly once is not enabled
        /// </summary>
        public static readonly long DEFAULT_COMMIT_INTERVAL_MS = 30000L;

        /// <summary>
        /// Default commit interval in milliseconds when exactly once is enabled
        /// </summary>
        public static readonly long EOS_DEFAULT_COMMIT_INTERVAL_MS = 100L;

        #endregion
        
        private const string mainConsumerPrefix = "main.consumer.";
        private const string globalConsumerPrefix = "global.consumer.";
        private const string restoreConsumerPrefix = "restore.consumer.";
        private const string producerPrefix = "producer.";
        private const string externalConsumerPrefix = "external.consumer.";
        private const string externalProducerPrefix = "external.producer.";

        private ConsumerConfig _consumerConfig = null;
        private ProducerConfig _producerConfig = null;
        private AdminClientConfig _adminClientConfig = null;
        private ClientConfig _config = null;

        private readonly ConsumerConfig _overrideMainConsumerConfig = new();
        private readonly ConsumerConfig _overrideGlobalConsumerConfig = new();
        private readonly ConsumerConfig _overrideRestoreConsumerConfig = new();
        private readonly ConsumerConfig _overrideExternalConsumerConfig = new();
        private readonly ProducerConfig _overrideProducerConfig = new();
        private readonly ProducerConfig _overrideExternalProducerConfig = new();
        
        private readonly List<IStreamMiddleware> middlewares = new();

        private bool changeGuarantee = false;
        
        private readonly IDictionary<string, PropertyInfo> cacheProperties 
            = new Dictionary<string, PropertyInfo>();

        private readonly Dictionary<string, dynamic> configProperties = new();

        internal long MetricsMinIntervalMs { get; set; } = 30000;

        #region Middlewares

        /// <summary>
        /// Add middleware
        /// </summary>
        /// <param name="item">New middleware</param>
        public void AddMiddleware(IStreamMiddleware item) => middlewares.Add(item);

        /// <summary>
        /// Clear all middlewares
        /// </summary>
        public void ClearMiddleware() => middlewares.Clear();

        /// <summary>
        /// Remove a specific middleware
        /// </summary>
        /// <param name="item">middleware to remove</param>
        /// <returns></returns>
        public bool RemoveMiddleware(IStreamMiddleware item) => middlewares.Remove(item);
        
        /// <summary>
        /// Get all middlewares
        /// </summary>
        public IEnumerable<IStreamMiddleware> Middlewares => middlewares.AsReadOnly();
        
        #endregion

        #region ClientConfig

        /// <summary>
        /// Timeout for broker API version requests. default: 10000 importance: low
        /// </summary>
        [StreamConfigProperty("api.version.request.timeout.ms")]
        public int? ApiVersionRequestTimeoutMs
        {
            get => _config.ApiVersionRequestTimeoutMs;
            set
            {
                _config.ApiVersionRequestTimeoutMs = value;
                _consumerConfig.ApiVersionRequestTimeoutMs = value;
                _producerConfig.ApiVersionRequestTimeoutMs = value;
                _adminClientConfig.ApiVersionRequestTimeoutMs = value;
            }
        }

        /// <summary>
        /// Dictates how long the `broker.version.fallback` fallback is used in the case
        /// the ApiVersionRequest fails. **NOTE**: The ApiVersionRequest is only issued when
        /// a new connection to the broker is made (such as after an upgrade). default: 0
        /// importance: medium
        /// </summary>
        [StreamConfigProperty("api.version.fallback.ms")]
        public int? ApiVersionFallbackMs
        {
            get => _config.ApiVersionFallbackMs;
            set
            {
                _config.ApiVersionFallbackMs = value;
                _consumerConfig.ApiVersionFallbackMs = value;
                _producerConfig.ApiVersionFallbackMs = value;
                _adminClientConfig.ApiVersionFallbackMs = value;
            }
        }

        /// <summary>
        /// Older broker versions (before 0.10.0) provide no way for a client to query for
        /// supported protocol features (ApiVersionRequest, see `api.version.request`) making
        /// it impossible for the client to know what features it may use. As a workaround
        /// a user may set this property to the expected broker version and the client will
        /// automatically adjust its feature set accordingly if the ApiVersionRequest fails
        /// (or is disabled). The fallback broker version will be used for `api.version.fallback.ms`.
        /// Valid values are: 0.9.0, 0.8.2, 0.8.1, 0.8.0. Any other value >= 0.10, such as
        /// 0.10.2.1, enables ApiVersionRequests. default: 0.10.0 importance: medium
        /// </summary>
        [StreamConfigProperty("broker.version.fallback")]
        public string BrokerVersionFallback
        {
            get => _config.BrokerVersionFallback;
            set
            {
                _config.BrokerVersionFallback = value;
                _consumerConfig.BrokerVersionFallback = value;
                _producerConfig.BrokerVersionFallback = value;
                _adminClientConfig.BrokerVersionFallback = value;
            }
        }

        /// <summary>
        /// Protocol used to communicate with brokers. default: plaintext importance: high
        /// </summary>
        [StreamConfigProperty("security.protocol")]
        public SecurityProtocol? SecurityProtocol
        {
            get => _config.SecurityProtocol;
            set
            {
                _config.SecurityProtocol = value;
                _consumerConfig.SecurityProtocol = value;
                _producerConfig.SecurityProtocol = value;
                _adminClientConfig.SecurityProtocol = value;
            }
        }

        /// <summary>
        /// A cipher suite is a named combination of authentication, encryption, MAC and
        /// key exchange algorithm used to negotiate the security settings for a network
        /// connection using TLS or SSL network protocol. See manual page for `ciphers(1)`
        /// and `SSL_CTX_set_cipher_list(3). default: '' importance: low
        /// </summary>
        [StreamConfigProperty("ssl.cipher.suites")]
        public string SslCipherSuites
        {
            get => _config.SslCipherSuites;
            set
            {
                _config.SslCipherSuites = value;
                _consumerConfig.SslCipherSuites = value;
                _producerConfig.SslCipherSuites = value;
                _adminClientConfig.SslCipherSuites = value;
            }
        }

        /// <summary>
        /// The supported-curves extension in the TLS ClientHello message specifies the curves
        /// (standard/named, or 'explicit' GF(2^k) or GF(p)) the client is willing to have
        /// the server use. See manual page for `SSL_CTX_set1_curves_list(3)`. OpenSSL >=
        /// 1.0.2 required. default: '' importance: low
        /// </summary>
        [StreamConfigProperty("ssl.curves.list")]
        public string SslCurvesList
        {
            get => _config.SslCurvesList;
            set
            {
                _config.SslCurvesList = value;
                _consumerConfig.SslCurvesList = value;
                _producerConfig.SslCurvesList = value;
                _adminClientConfig.SslCurvesList = value;
            }
        }

        /// <summary>
        /// The client uses the TLS ClientHello signature_algorithms extension to indicate
        /// to the server which signature/hash algorithm pairs may be used in digital signatures.
        /// See manual page for `SSL_CTX_set1_sigalgs_list(3)`. OpenSSL >= 1.0.2 required.
        /// default: '' importance: low
        /// </summary>
        [StreamConfigProperty("ssl.sigalgs.list")]
        public string SslSigalgsList
        {
            get => _config.SslSigalgsList;
            set
            {
                _config.SslSigalgsList = value;
                _consumerConfig.SslSigalgsList = value;
                _producerConfig.SslSigalgsList = value;
                _adminClientConfig.SslSigalgsList = value;
            }
        }

        /// <summary>
        /// Path to client's private key (PEM) used for authentication. default: '' importance:
        /// low
        /// </summary>
        [StreamConfigProperty("ssl.key.location")]
        public string SslKeyLocation
        {
            get => _config.SslKeyLocation;
            set
            {
                _config.SslKeyLocation = value;
                _consumerConfig.SslKeyLocation = value;
                _producerConfig.SslKeyLocation = value;
                _adminClientConfig.SslKeyLocation = value;
            }
        }

        /// <summary>
        /// Private key passphrase (for use with `ssl.key.location` and `set_ssl_cert()`)
        /// default: '' importance: low
        /// </summary>
        [StreamConfigProperty("ssl.key.password")]
        public string SslKeyPassword
        {
            get => _config.SslKeyPassword;
            set
            {
                _config.SslKeyPassword = value;
                _consumerConfig.SslKeyPassword = value;
                _producerConfig.SslKeyPassword = value;
                _adminClientConfig.SslKeyPassword = value;
            }
        }

        /// <summary>
        /// Client's private key string (PEM format) used for authentication. default: ''
        /// importance: low
        /// </summary>
        [StreamConfigProperty("ssl.key.pem")]
        public string SslKeyPem
        {
            get => _config.SslKeyPem;
            set
            {
                _config.SslKeyPem = value;
                _consumerConfig.SslKeyPem = value;
                _producerConfig.SslKeyPem = value;
                _adminClientConfig.SslKeyPem = value;
            }
        }

        /// <summary>
        /// Path to client's public key (PEM) used for authentication. default: '' importance:
        /// low
        /// </summary>
        [StreamConfigProperty("ssl.certificate.location")]
        public string SslCertificateLocation
        {
            get => _config.SslCertificateLocation;
            set
            {
                _config.SslCertificateLocation = value;
                _consumerConfig.SslCertificateLocation = value;
                _producerConfig.SslCertificateLocation = value;
                _adminClientConfig.SslCertificateLocation = value;
            }
        }

        /// <summary>
        /// Client's public key string (PEM format) used for authentication. default: ''
        /// importance: low
        /// </summary>
        [StreamConfigProperty("ssl.certificate.pem")]
        public string SslCertificatePem
        {
            get => _config.SslCertificatePem;
            set
            {
                _config.SslCertificatePem = value;
                _consumerConfig.SslCertificatePem = value;
                _producerConfig.SslCertificatePem = value;
                _adminClientConfig.SslCertificatePem = value;
            }
        }

        /// <summary>
        /// File or directory path to CA certificate(s) for verifying the broker's key. default:
        /// '' importance: low
        /// </summary>
        [StreamConfigProperty("ssl.ca.location")]
        public string SslCaLocation
        {
            get => _config.SslCaLocation;
            set
            {
                _config.SslCaLocation = value;
                _consumerConfig.SslCaLocation = value;
                _producerConfig.SslCaLocation = value;
                _adminClientConfig.SslCaLocation = value;
            }
        }

        /// <summary>
        /// Path to client's keystore (PKCS#12) used for authentication. default: '' importance:
        /// low
        /// </summary>
        [StreamConfigProperty("ssl.keystore.location")]
        public string SslKeystoreLocation
        {
            get => _config.SslKeystoreLocation;
            set
            {
                _config.SslKeystoreLocation = value;
                _consumerConfig.SslKeystoreLocation = value;
                _producerConfig.SslKeystoreLocation = value;
                _adminClientConfig.SslKeystoreLocation = value;
            }
        }

        /// <summary>
        /// Request broker's supported API versions to adjust functionality to available
        /// protocol features. If set to false, or the ApiVersionRequest fails, the fallback
        /// version `broker.version.fallback` will be used. **NOTE**: Depends on broker version
        /// >=0.10.0. If the request is not supported by (an older) broker the `broker.version.fallback`
        /// fallback is used. default: true importance: high
        /// </summary>
        [StreamConfigProperty("api.version.request")]
        public bool? ApiVersionRequest
        {
            get => _config.ApiVersionRequest;
            set
            {
                _config.ApiVersionRequest = value;
                _consumerConfig.ApiVersionRequest = value;
                _producerConfig.ApiVersionRequest = value;
                _adminClientConfig.ApiVersionRequest = value;
            }
        }

        /// <summary>
        /// Client's keystore (PKCS#12) password. default: '' importance: low
        /// </summary>
        [StreamConfigProperty("ssl.keystore.password")]
        public string SslKeystorePassword
        {
            get => _config.SslKeystorePassword;
            set
            {
                _config.SslKeystorePassword = value;
                _consumerConfig.SslKeystorePassword = value;
                _producerConfig.SslKeystorePassword = value;
                _adminClientConfig.SslKeystorePassword = value;
            }
        }

        /// <summary>
        /// Enable OpenSSL's builtin broker (server) certificate verification. default:
        /// true importance: low
        /// </summary>
        [StreamConfigProperty("enable.ssl.certificate.verification")]
        public bool? EnableSslCertificateVerification
        {
            get => _config.EnableSslCertificateVerification;
            set
            {
                _config.EnableSslCertificateVerification = value;
                _consumerConfig.EnableSslCertificateVerification = value;
                _producerConfig.EnableSslCertificateVerification = value;
                _adminClientConfig.EnableSslCertificateVerification = value;
            }
        }

        /// <summary>
        /// Endpoint identification algorithm to validate broker hostname using broker certificate.
        /// https - Server (broker) hostname verification as specified in RFC2818. none -
        /// No endpoint verification. OpenSSL >= 1.0.2 required. default: none importance:
        /// low
        /// </summary>
        [StreamConfigProperty("ssl.endpoint.identification.algorithm")]
        public SslEndpointIdentificationAlgorithm? SslEndpointIdentificationAlgorithm
        {
            get => _config.SslEndpointIdentificationAlgorithm;
            set
            {
                _config.SslEndpointIdentificationAlgorithm = value;
                _consumerConfig.SslEndpointIdentificationAlgorithm = value;
                _producerConfig.SslEndpointIdentificationAlgorithm = value;
                _adminClientConfig.SslEndpointIdentificationAlgorithm = value;
            }
        }

        /// <summary>
        /// Kerberos principal name that Kafka runs as, not including /hostname@REALM default:
        /// kafka importance: low
        /// </summary>
        [StreamConfigProperty("sasl.kerberos.service.name")]
        public string SaslKerberosServiceName
        {
            get => _config.SaslKerberosServiceName;
            set
            {
                _config.SaslKerberosServiceName = value;
                _consumerConfig.SaslKerberosServiceName = value;
                _producerConfig.SaslKerberosServiceName = value;
                _adminClientConfig.SaslKerberosServiceName = value;
            }
        }

        /// <summary>
        /// This client's Kerberos principal name. (Not supported on Windows, will use the
        /// logon user's principal). default: kafkaclient importance: low
        /// </summary>
        [StreamConfigProperty("sasl.kerberos.principal")]
        public string SaslKerberosPrincipal
        {
            get => _config.SaslKerberosPrincipal;
            set
            {
                _config.SaslKerberosPrincipal = value;
                _consumerConfig.SaslKerberosPrincipal = value;
                _producerConfig.SaslKerberosPrincipal = value;
                _adminClientConfig.SaslKerberosPrincipal = value;
            }
        }

        /// <summary>
        /// Shell command to refresh or acquire the client's Kerberos ticket. This command
        /// is executed on client creation and every sasl.kerberos.min.time.before.relogin
        /// (0=disable). %{config.prop.name} is replaced by corresponding config object value.
        /// default: kinit -R -t "%{sasl.kerberos.keytab}" -k %{sasl.kerberos.principal}
        /// || kinit -t "%{sasl.kerberos.keytab}" -k %{sasl.kerberos.principal} importance:
        /// low
        /// </summary>
        [StreamConfigProperty("sasl.kerberos.kinit.cmd")]
        public string SaslKerberosKinitCmd
        {
            get => _config.SaslKerberosKinitCmd;
            set
            {
                _config.SaslKerberosKinitCmd = value;
                _consumerConfig.SaslKerberosKinitCmd = value;
                _producerConfig.SaslKerberosKinitCmd = value;
                _adminClientConfig.SaslKerberosKinitCmd = value;
            }
        }

        /// <summary>
        /// Path to Kerberos keytab file. This configuration property is only used as a variable
        /// in `sasl.kerberos.kinit.cmd` as ` ... -t "%{sasl.kerberos.keytab}"`. default:
        /// '' importance: low
        /// </summary>
        [StreamConfigProperty("sasl.kerberos.keytab")]
        public string SaslKerberosKeytab
        {
            get => _config.SaslKerberosKeytab;
            set
            {
                _config.SaslKerberosKeytab = value;
                _consumerConfig.SaslKerberosKeytab = value;
                _producerConfig.SaslKerberosKeytab = value;
                _adminClientConfig.SaslKerberosKeytab = value;
            }
        }

        /// <summary>
        /// Minimum time in milliseconds between key refresh attempts. Disable automatic
        /// key refresh by setting this property to 0. default: 60000 importance: low
        /// </summary>
        [StreamConfigProperty("sasl.kerberos.min.time.before.relogin")]
        public int? SaslKerberosMinTimeBeforeRelogin
        {
            get => _config.SaslKerberosMinTimeBeforeRelogin;
            set
            {
                _config.SaslKerberosMinTimeBeforeRelogin = value;
                _consumerConfig.SaslKerberosMinTimeBeforeRelogin = value;
                _producerConfig.SaslKerberosMinTimeBeforeRelogin = value;
                _adminClientConfig.SaslKerberosMinTimeBeforeRelogin = value;
            }
        }

        /// <summary>
        /// SASL username for use with the PLAIN and SASL-SCRAM-.. mechanisms default: ''
        /// importance: high
        /// </summary>
        [StreamConfigProperty("sasl.username")]
        public string SaslUsername
        {
            get => _config.SaslUsername;
            set
            {
                _config.SaslUsername = value;
                _consumerConfig.SaslUsername = value;
                _producerConfig.SaslUsername = value;
                _adminClientConfig.SaslUsername = value;
            }
        }

        /// <summary>
        /// SASL password for use with the PLAIN and SASL-SCRAM-.. mechanism default: ''
        /// importance: high
        /// </summary>
        [StreamConfigProperty("sasl.password")]
        public string SaslPassword
        {
            get => _config.SaslPassword;
            set
            {
                _config.SaslPassword = value;
                _consumerConfig.SaslPassword = value;
                _producerConfig.SaslPassword = value;
                _adminClientConfig.SaslPassword = value;
            }
        }

        /// <summary>
        /// SASL/OAUTHBEARER configuration. The format is implementation-dependent and must
        /// be parsed accordingly. The default unsecured token implementation (see https://tools.ietf.org/html/rfc7515#appendix-A.5)
        /// recognizes space-separated name=value pairs with valid names including principalClaimName,
        /// principal, scopeClaimName, scope, and lifeSeconds. The default value for principalClaimName
        /// is "sub", the default value for scopeClaimName is "scope", and the default value
        /// for lifeSeconds is 3600. The scope value is CSV format with the default value
        /// being no/empty scope. For example: `principalClaimName=azp principal=admin scopeClaimName=roles
        /// scope=role1,role2 lifeSeconds=600`. In addition, SASL extensions can be communicated
        /// to the broker via `extension_NAME=value`. For example: `principal=admin extension_traceId=123`
        /// default: '' importance: low
        /// </summary>
        [StreamConfigProperty("sasl.oauthbearer.config")]
        public string SaslOauthbearerConfig
        {
            get => _config.SaslOauthbearerConfig;
            set
            {
                _config.SaslOauthbearerConfig = value;
                _consumerConfig.SaslOauthbearerConfig = value;
                _producerConfig.SaslOauthbearerConfig = value;
                _adminClientConfig.SaslOauthbearerConfig = value;
            }
        }

        /// <summary>
        /// Enable the builtin unsecure JWT OAUTHBEARER token handler if no oauthbearer_refresh_cb
        /// has been set. This builtin handler should only be used for development or testing,
        /// and not in production. default: false importance: low
        /// </summary>
        [StreamConfigProperty("sasl.oauthbearer.unsecure.jwt")]
        public bool? EnableSaslOauthbearerUnsecureJwt
        {
            get => _config.EnableSaslOauthbearerUnsecureJwt;
            set
            {
                _config.EnableSaslOauthbearerUnsecureJwt = value;
                _consumerConfig.EnableSaslOauthbearerUnsecureJwt = value;
                _producerConfig.EnableSaslOauthbearerUnsecureJwt = value;
                _adminClientConfig.EnableSaslOauthbearerUnsecureJwt = value;
            }
        }

        /// <summary>
        /// Set to "default" or "oidc" to control which login method to be used. If set to "oidc", the following properties must also be be specified: `sasl.oauthbearer.client.id`, `sasl.oauthbearer.client.secret`, and `sasl.oauthbearer.token.endpoint.url`.
        /// default: default
        /// importance: low
        /// </summary>
        [StreamConfigProperty("sasl.oauthbearer.method")]
        public Confluent.Kafka.SaslOauthbearerMethod? SaslOauthbearerMethod
        {
            get => _config.SaslOauthbearerMethod;
            set
            {
                _config.SaslOauthbearerMethod = value;
                _consumerConfig.SaslOauthbearerMethod = value;
                _producerConfig.SaslOauthbearerMethod = value;
                _adminClientConfig.SaslOauthbearerMethod = value;
            }
        }

        /// <summary>
        /// Public identifier for the application. Must be unique across all clients that the authorization server handles. Only used when `sasl.oauthbearer.method` is set to "oidc".
        /// default: ''
        /// importance: low
        /// </summary>
        [StreamConfigProperty("sasl.oauthbearer.client.id")]
        public string SaslOauthbearerClientId
        {
            get => _config.SaslOauthbearerClientId;
            set
            {
                _config.SaslOauthbearerClientId = value;
                _consumerConfig.SaslOauthbearerClientId = value;
                _producerConfig.SaslOauthbearerClientId = value;
                _adminClientConfig.SaslOauthbearerClientId = value;
            }
        }

        /// <summary>
        /// Client secret only known to the application and the authorization server. This should be a sufficiently random string that is not guessable. Only used when `sasl.oauthbearer.method` is set to "oidc".
        /// default: ''
        /// importance: low
        /// </summary>
        [StreamConfigProperty("sasl.oauthbearer.client.secret")]
        public string SaslOauthbearerClientSecret
        {
            get => _config.SaslOauthbearerClientSecret;
            set
            {
                _config.SaslOauthbearerClientSecret = value;
                _consumerConfig.SaslOauthbearerClientSecret = value;
                _producerConfig.SaslOauthbearerClientSecret = value;
                _adminClientConfig.SaslOauthbearerClientSecret = value;
            }
        }

        /// <summary>
        /// Client use this to specify the scope of the access request to the broker. Only used when `sasl.oauthbearer.method` is set to "oidc".
        /// default: ''
        /// importance: low
        /// </summary>
        [StreamConfigProperty("sasl.oauthbearer.scope")]
        public string SaslOauthbearerScope
        {
            get => _config.SaslOauthbearerScope;
            set
            {
                _config.SaslOauthbearerScope = value;
                _consumerConfig.SaslOauthbearerScope = value;
                _producerConfig.SaslOauthbearerScope = value;
                _adminClientConfig.SaslOauthbearerScope = value;
            }
        }

        /// <summary>
        /// Allow additional information to be provided to the broker. Comma-separated list of key=value pairs. E.g., "supportFeatureX=true,organizationId=sales-emea".Only used when `sasl.oauthbearer.method` is set to "oidc".
        /// default: ''
        /// importance: low
        /// </summary>
        [StreamConfigProperty("sasl.oauthbearer.extensions")]
        public string SaslOauthbearerExtensions
        {
            get => _config.SaslOauthbearerExtensions;
            set
            {
                _config.SaslOauthbearerExtensions = value;
                _consumerConfig.SaslOauthbearerExtensions = value;
                _producerConfig.SaslOauthbearerExtensions = value;
                _adminClientConfig.SaslOauthbearerExtensions = value;
            }
        }

        /// <summary>
        /// OAuth/OIDC issuer token endpoint HTTP(S) URI used to retrieve token. Only used when `sasl.oauthbearer.method` is set to "oidc".
        /// default: ''
        /// importance: low
        /// </summary>
        [StreamConfigProperty("sasl.oauthbearer.token.endpoint.url")]
        public string SaslOauthbearerTokenEndpointUrl
        {
            get => _config.SaslOauthbearerTokenEndpointUrl;
            set
            {
                _config.SaslOauthbearerTokenEndpointUrl = value;
                _consumerConfig.SaslOauthbearerTokenEndpointUrl = value;
                _producerConfig.SaslOauthbearerTokenEndpointUrl = value;
                _adminClientConfig.SaslOauthbearerTokenEndpointUrl = value;
            }
        }
        
        /// <summary>
        ///  Path to CRL for verifying broker's certificate validity. default: '' importance:
        ///  low
        ///  </summary>
        [StreamConfigProperty("ssl.crl.location")]
        public string SslCrlLocation
        {
            get => _config.SslCrlLocation;
            set
            {
                _config.SslCrlLocation = value;
                _consumerConfig.SslCrlLocation = value;
                _producerConfig.SslCrlLocation = value;
                _adminClientConfig.SslCrlLocation = value;
            }
        }

        /// <summary>
        /// Signal that librdkafka will use to quickly terminate on rd_kafka_destroy(). If
        /// this signal is not set then there will be a delay before rd_kafka_wait_destroyed()
        /// returns true as internal threads are timing out their system calls. If this signal
        /// is set however the delay will be minimal. The application should mask this signal
        /// as an internal signal handler is installed. default: 0 importance: low
        /// </summary>
        [StreamConfigProperty("internal.termination.signal")]
        public int? InternalTerminationSignal
        {
            get => _config.InternalTerminationSignal;
            set
            {
                _config.InternalTerminationSignal = value;
                _consumerConfig.InternalTerminationSignal = value;
                _producerConfig.InternalTerminationSignal = value;
                _adminClientConfig.InternalTerminationSignal = value;
            }
        }

        /// <summary>
        /// Log broker disconnects. It might be useful to turn this off when interacting
        /// with 0.9 brokers with an aggressive `connection.max.idle.ms` value. default:
        /// true importance: low
        /// </summary>
        [StreamConfigProperty("log.connection.close")]
        public bool? LogConnectionClose
        {
            get => _config.LogConnectionClose;
            set
            {
                _config.LogConnectionClose = value;
                _consumerConfig.LogConnectionClose = value;
                _producerConfig.LogConnectionClose = value;
                _adminClientConfig.LogConnectionClose = value;
            }
        }

        /// <summary>
        /// Print internal thread name in log messages (useful for debugging librdkafka internals)
        /// default: true importance: low
        /// </summary>
        [StreamConfigProperty("log.thread.name")]
        public bool? LogThreadName
        {
            get => _config.LogThreadName;
            set
            {
                _config.LogThreadName = value;
                _consumerConfig.LogThreadName = value;
                _producerConfig.LogThreadName = value;
                _adminClientConfig.LogThreadName = value;
            }
        }

        /// <summary>
        /// SASL mechanism to use for authentication. Supported: GSSAPI, PLAIN, SCRAM-SHA-256,
        /// SCRAM-SHA-512. **NOTE**: Despite the name, you may not configure more than one
        /// mechanism.
        /// </summary>
        [StreamConfigProperty("sasl.mechanism")]
        public SaslMechanism? SaslMechanism
        {
            get => _config.SaslMechanism;
            set
            {
                _config.SaslMechanism = value;
                _consumerConfig.SaslMechanism = value;
                _producerConfig.SaslMechanism = value;
                _adminClientConfig.SaslMechanism = value;
            }
        }

        /// <summary>
        /// This field indicates the number of acknowledgements the leader broker must receive
        /// from ISR brokers before responding to the request: Zero=Broker does not send
        /// any response/ack to client, One=The leader will write the record to its local
        /// log but will respond without awaiting full acknowledgement from all followers.
        /// All=Broker will block until message is committed by all in sync replicas (ISRs).
        /// If there are less than min.insync.replicas (broker configuration) in the ISR
        /// set the produce request will fail.
        /// </summary>
        [StreamConfigProperty("acks")]
        public Acks? Acks
        {
            get => _config.Acks;
            set
            {
                _config.Acks = value;
                _consumerConfig.Acks = value;
                _producerConfig.Acks = value;
                _adminClientConfig.Acks = value;
            }
        }

        /// <summary>
        /// Maximum Kafka protocol request message size. Due to differing framing overhead
        /// between protocol versions the producer is unable to reliably enforce a strict
        /// max message limit at produce time and may exceed the maximum size by one message
        /// in protocol ProduceRequests, the broker will enforce the the topic's `max.message.bytes`
        /// limit (see Apache Kafka documentation). default: 1000000 importance: medium
        /// </summary>
        [StreamConfigProperty("message.max.bytes")]
        public int? MessageMaxBytes
        {
            get => _config.MessageMaxBytes;
            set
            {
                _config.MessageMaxBytes = value;
                _consumerConfig.MessageMaxBytes = value;
                _producerConfig.MessageMaxBytes = value;
                _adminClientConfig.MessageMaxBytes = value;
            }
        }

        /// <summary>
        /// Maximum size for message to be copied to buffer. Messages larger than this will
        /// be passed by reference (zero-copy) at the expense of larger iovecs. default:
        /// 65535 importance: low
        /// </summary>
        [StreamConfigProperty("message.copy.max.bytes")]
        public int? MessageCopyMaxBytes
        {
            get => _config.MessageCopyMaxBytes;
            set
            {
                _config.MessageCopyMaxBytes = value;
                _consumerConfig.MessageCopyMaxBytes = value;
                _producerConfig.MessageCopyMaxBytes = value;
                _adminClientConfig.MessageCopyMaxBytes = value;
            }
        }

        /// <summary>
        /// Maximum Kafka protocol response message size. This serves as a safety precaution
        /// to avoid memory exhaustion in case of protocol hickups. This value must be at
        /// least `fetch.max.bytes` + 512 to allow for protocol overhead; the value is adjusted
        /// automatically unless the configuration property is explicitly set. default: 100000000
        /// importance: medium
        /// </summary>
        [StreamConfigProperty("receive.message.max.bytes")]
        public int? ReceiveMessageMaxBytes
        {
            get => _config.ReceiveMessageMaxBytes;
            set
            {
                _config.ReceiveMessageMaxBytes = value;
                _consumerConfig.ReceiveMessageMaxBytes = value;
                _producerConfig.ReceiveMessageMaxBytes = value;
                _adminClientConfig.ReceiveMessageMaxBytes = value;
            }
        }

        /// <summary>
        /// Maximum number of in-flight requests per broker connection. This is a generic
        /// property applied to all broker communication, however it is primarily relevant
        /// to produce requests. In particular, note that other mechanisms limit the number
        /// of outstanding consumer fetch request per broker to one. default: 1000000 importance:
        /// low
        /// </summary>
        [StreamConfigProperty("max.in.flight")]
        public int? MaxInFlight
        {
            get => _config.MaxInFlight;
            set
            {
                if (Guarantee != ProcessingGuarantee.EXACTLY_ONCE)
                {
                    _config.MaxInFlight = value;
                    _consumerConfig.MaxInFlight = value;
                    _producerConfig.MaxInFlight = value;
                    _adminClientConfig.MaxInFlight = value;
                }
                else if (!changeGuarantee)
                    throw new StreamsException($"You can't update MaxInFlight because your processing guarantee is exactly-once");
            }
        }

        /// <summary>
        /// Period of time in milliseconds at which topic and broker metadata is refreshed
        /// in order to proactively discover any new brokers, topics, partitions or partition
        /// leader changes. Use -1 to disable the intervalled refresh (not recommended).
        /// If there are no locally referenced topics (no topic objects created, no messages
        /// produced, no subscription or no assignment) then only the broker list will be
        /// refreshed every interval but no more often than every 10s. default: 300000 importance:
        /// low
        /// </summary>
        [StreamConfigProperty("topic.metadata.refresh.interval.ms")]
        public int? TopicMetadataRefreshIntervalMs
        {
            get => _config.TopicMetadataRefreshIntervalMs;
            set
            {
                _config.TopicMetadataRefreshIntervalMs = value;
                _consumerConfig.TopicMetadataRefreshIntervalMs = value;
                _producerConfig.TopicMetadataRefreshIntervalMs = value;
                _adminClientConfig.TopicMetadataRefreshIntervalMs = value;
            }
        }

        /// <summary>
        /// Metadata cache max age. Defaults to topic.metadata.refresh.interval.ms * 3 default:
        /// 900000 importance: low
        /// </summary>
        [StreamConfigProperty("metadata.max.age.ms")]
        public int? MetadataMaxAgeMs
        {
            get => _config.MetadataMaxAgeMs;
            set
            {
                _config.MetadataMaxAgeMs = value;
                _consumerConfig.MetadataMaxAgeMs = value;
                _producerConfig.MetadataMaxAgeMs = value;
                _adminClientConfig.MetadataMaxAgeMs = value;
            }
        }

        /// <summary>
        /// When a topic loses its leader a new metadata request will be enqueued with this
        /// initial interval, exponentially increasing until the topic metadata has been
        /// refreshed. This is used to recover quickly from transitioning leader brokers.
        /// default: 250 importance: low
        /// </summary>
        [StreamConfigProperty("topic.metadata.refresh.fast.interval.ms")]
        public int? TopicMetadataRefreshFastIntervalMs
        {
            get => _config.TopicMetadataRefreshFastIntervalMs;
            set
            {
                _config.TopicMetadataRefreshFastIntervalMs = value;
                _consumerConfig.TopicMetadataRefreshFastIntervalMs = value;
                _producerConfig.TopicMetadataRefreshFastIntervalMs = value;
                _adminClientConfig.TopicMetadataRefreshFastIntervalMs = value;
            }
        }

        /// <summary>
        /// Sparse metadata requests (consumes less network bandwidth) default: true importance:
        /// low
        /// </summary>
        [StreamConfigProperty("topic.metadata.refresh.sparse")]
        public bool? TopicMetadataRefreshSparse
        {
            get => _config.TopicMetadataRefreshSparse;
            set
            {
                _config.TopicMetadataRefreshSparse = value;
                _consumerConfig.TopicMetadataRefreshSparse = value;
                _producerConfig.TopicMetadataRefreshSparse = value;
                _adminClientConfig.TopicMetadataRefreshSparse = value;
            }
        }

        /// <summary>
        /// Topic blacklist, a comma-separated list of regular expressions for matching topic
        /// names that should be ignored in broker metadata information as if the topics
        /// did not exist. default: '' importance: low
        /// </summary>
        [StreamConfigProperty("topic.blacklist")]
        public string TopicBlacklist
        {
            get => _config.TopicBlacklist;
            set
            {
                _config.TopicBlacklist = value;
                _consumerConfig.TopicBlacklist = value;
                _producerConfig.TopicBlacklist = value;
                _adminClientConfig.TopicBlacklist = value;
            }
        }

        /// <summary>
        /// A comma-separated list of debug contexts to enable. Detailed Producer debugging:
        /// broker,topic,msg. Consumer: consumer,cgrp,topic,fetch default: '' importance:
        /// medium
        /// </summary>
        [StreamConfigProperty("debug")]
        public string Debug
        {
            get => _config.Debug;
            set
            {
                _config.Debug = value;
                _consumerConfig.Debug = value;
                _producerConfig.Debug = value;
                _adminClientConfig.Debug = value;
            }
        }

        /// <summary>
        /// Default timeout for network requests. Producer: ProduceRequests will use the
        /// lesser value of `socket.timeout.ms` and remaining `message.timeout.ms` for the
        /// first message in the batch. Consumer: FetchRequests will use `fetch.wait.max.ms`
        /// + `socket.timeout.ms`. Admin: Admin requests will use `socket.timeout.ms` or
        /// explicitly set `rd_kafka_AdminOptions_set_operation_timeout()` value. default:
        /// 60000 importance: low
        /// </summary>
        [StreamConfigProperty("socket.timeout.ms")]
        public int? SocketTimeoutMs
        {
            get => _config.SocketTimeoutMs;
            set
            {
                _config.SocketTimeoutMs = value;
                _consumerConfig.SocketTimeoutMs = value;
                _producerConfig.SocketTimeoutMs = value;
                _adminClientConfig.SocketTimeoutMs = value;
            }
        }

        /// <summary>
        /// Broker socket send buffer size. System default is used if 0. default: 0 importance:
        /// low
        /// </summary>
        [StreamConfigProperty("socket.send.buffer.bytes")]
        public int? SocketSendBufferBytes
        {
            get => _config.SocketSendBufferBytes;
            set
            {
                _config.SocketSendBufferBytes = value;
                _consumerConfig.SocketSendBufferBytes = value;
                _producerConfig.SocketSendBufferBytes = value;
                _adminClientConfig.SocketSendBufferBytes = value;
            }
        }

        /// <summary>
        /// Broker socket receive buffer size. System default is used if 0. default: 0 importance:
        /// low
        /// </summary>
        [StreamConfigProperty("socket.receive.buffer.bytes")]
        public int? SocketReceiveBufferBytes
        {
            get => _config.SocketReceiveBufferBytes;
            set
            {
                _config.SocketReceiveBufferBytes = value;
                _consumerConfig.SocketReceiveBufferBytes = value;
                _producerConfig.SocketReceiveBufferBytes = value;
                _adminClientConfig.SocketReceiveBufferBytes = value;
            }
        }

        /// <summary>
        /// Enable TCP keep-alives (SO_KEEPALIVE) on broker sockets default: false importance:
        /// low
        /// </summary>
        [StreamConfigProperty("socket.keepalive.enable")]
        public bool? SocketKeepaliveEnable
        {
            get => _config.SocketKeepaliveEnable;
            set
            {
                _config.SocketKeepaliveEnable = value;
                _consumerConfig.SocketKeepaliveEnable = value;
                _producerConfig.SocketKeepaliveEnable = value;
                _adminClientConfig.SocketKeepaliveEnable = value;
            }
        }

        /// <summary>
        /// Disable the Nagle algorithm (TCP_NODELAY) on broker sockets. default: false importance:
        /// low
        /// </summary>
        [StreamConfigProperty("socket.nagle.disable")]
        public bool? SocketNagleDisable
        {
            get => _config.SocketNagleDisable;
            set
            {
                _config.SocketNagleDisable = value;
                _consumerConfig.SocketNagleDisable = value;
                _producerConfig.SocketNagleDisable = value;
                _adminClientConfig.SocketNagleDisable = value;
            }
        }

        /// <summary>
        /// Disconnect from broker when this number of send failures (e.g., timed out requests)
        /// is reached. Disable with 0. WARNING: It is highly recommended to leave this setting
        /// at its default value of 1 to avoid the client and broker to become desynchronized
        /// in case of request timeouts. NOTE: The connection is automatically re-established.
        /// default: 1 importance: low
        /// </summary>
        [StreamConfigProperty("socket.max.fails")]
        public int? SocketMaxFails
        {
            get => _config.SocketMaxFails;
            set
            {
                _config.SocketMaxFails = value;
                _consumerConfig.SocketMaxFails = value;
                _producerConfig.SocketMaxFails = value;
                _adminClientConfig.SocketMaxFails = value;
            }
        }

        /// <summary>
        /// How long to cache the broker address resolving results (milliseconds). default:
        /// 1000 importance: low
        /// </summary>
        [StreamConfigProperty("broker.address.ttl")]
        public int? BrokerAddressTtl
        {
            get => _config.BrokerAddressTtl;
            set
            {
                _config.BrokerAddressTtl = value;
                _consumerConfig.BrokerAddressTtl = value;
                _producerConfig.BrokerAddressTtl = value;
                _adminClientConfig.BrokerAddressTtl = value;
            }
        }

        /// <summary>
        /// Allowed broker IP address families: any, v4, v6 default: any importance: low
        /// </summary>
        [StreamConfigProperty("broker.address.family")]
        public BrokerAddressFamily? BrokerAddressFamily
        {
            get => _config.BrokerAddressFamily;
            set
            {
                _config.BrokerAddressFamily = value;
                _consumerConfig.BrokerAddressFamily = value;
                _producerConfig.BrokerAddressFamily = value;
                _adminClientConfig.BrokerAddressFamily = value;
            }
        }

        /// <summary>
        /// The initial time to wait before reconnecting to a broker after the connection
        /// has been closed. The time is increased exponentially until `reconnect.backoff.max.ms`
        /// is reached. -25% to +50% jitter is applied to each reconnect backoff. A value
        /// of 0 disables the backoff and reconnects immediately. default: 100 importance:
        /// medium
        /// </summary>
        [StreamConfigProperty("reconnect.backoff.ms")]
        public int? ReconnectBackoffMs
        {
            get => _config.ReconnectBackoffMs;
            set
            {
                _config.ReconnectBackoffMs = value;
                _consumerConfig.ReconnectBackoffMs = value;
                _producerConfig.ReconnectBackoffMs = value;
                _adminClientConfig.ReconnectBackoffMs = value;
            }
        }

        /// <summary>
        /// The maximum time to wait before reconnecting to a broker after the connection
        /// has been closed. default: 10000 importance: medium
        /// </summary>
        [StreamConfigProperty("reconnect.backoff.max.ms")]
        public int? ReconnectBackoffMaxMs
        {
            get => _config.ReconnectBackoffMaxMs;
            set
            {
                _config.ReconnectBackoffMaxMs = value;
                _consumerConfig.ReconnectBackoffMaxMs = value;
                _producerConfig.ReconnectBackoffMaxMs = value;
                _adminClientConfig.ReconnectBackoffMaxMs = value;
            }
        }

        /// <summary>
        /// librdkafka statistics emit interval. The granularity is 1000ms.
        /// A value of 0 disables statistics. default: 0 importance: high
        /// </summary>
        [StreamConfigProperty("statistics.interval.ms")]
        public int? StatisticsIntervalMs
        {
            get => _config.StatisticsIntervalMs;
            set
            {
                _config.StatisticsIntervalMs = value;
                _consumerConfig.StatisticsIntervalMs = value;
                _producerConfig.StatisticsIntervalMs = value;
                _adminClientConfig.StatisticsIntervalMs = value;
            }
        }

        /// <summary>
        /// Disable spontaneous log_cb from internal librdkafka threads, instead enqueue
        /// log messages on queue set with `rd_kafka_set_log_queue()` and serve log callbacks
        /// or events through the standard poll APIs. **NOTE**: Log messages will linger
        /// in a temporary queue until the log queue has been set. default: false importance:
        /// low
        /// </summary>
        [StreamConfigProperty("log.queue")]
        public bool? LogQueue
        {
            get => _config.LogQueue;
            set
            {
                _config.LogQueue = value;
                _consumerConfig.LogQueue = value;
                _producerConfig.LogQueue = value;
                _adminClientConfig.LogQueue = value;
            }
        }

        /// <summary>
        /// List of plugin libraries to load (; separated). The library search path is platform
        /// dependent (see dlopen(3) for Unix and LoadLibrary() for Windows). If no filename
        /// extension is specified the platform-specific extension (such as .dll or .so)
        /// will be appended automatically. default: '' importance: low
        /// </summary>
        [StreamConfigProperty("plugin.library.paths")]
        public string PluginLibraryPaths
        {
            get => _config.PluginLibraryPaths;
            set
            {
                _config.PluginLibraryPaths = value;
                _consumerConfig.PluginLibraryPaths = value;
                _producerConfig.PluginLibraryPaths = value;
                _adminClientConfig.PluginLibraryPaths = value;
            }
        }

        /// <summary>
        /// A rack identifier for this client. This can be any string value which indicates
        /// where this client is physically located. It corresponds with the broker config
        /// `broker.rack`. default: '' importance: low
        /// </summary>
        [StreamConfigProperty("rack")]
        public string ClientRack
        {
            get => _config.ClientRack;
            set
            {
                _config.ClientRack = value;
                _consumerConfig.ClientRack = value;
                _producerConfig.ClientRack = value;
                _adminClientConfig.ClientRack = value;
            }
        }

        /// <summary>
        /// Comma-separated list of Windows Certificate stores to load CA certificates from.
        /// Certificates will be loaded in the same order as stores are specified. If no
        /// certificates can be loaded from any of the specified stores an error is logged
        /// and the OpenSSL library's default CA location is used instead. Store names are
        /// typically one or more of: MY, Root, Trust, CA. default: Root importance: low
        /// </summary>
        [StreamConfigProperty("ssl.ca.certificate.stores")]
        public string SslCaCertificateStores
        {
            get => _config.SslCaCertificateStores;
            set
            {
                _config.SslCaCertificateStores = value;
                _consumerConfig.SslCaCertificateStores = value;
                _producerConfig.SslCaCertificateStores = value;
                _adminClientConfig.SslCaCertificateStores = value;
            }
        }

        /// <summary>
        /// If enabled librdkafka will initialize the PRNG with srand(current_time.milliseconds)
        /// on the first invocation of rd_kafka_new() (required only if rand_r() is not available
        /// on your platform). If disabled the application must call srand() prior to calling
        /// rd_kafka_new(). default: true importance: low
        /// </summary>
        [StreamConfigProperty("enable.random.seed")]
        public bool? EnableRandomSeed
        {
            get => _config.EnableRandomSeed;
            set
            {
                _config.EnableRandomSeed = value;
                _consumerConfig.EnableRandomSeed = value;
                _producerConfig.EnableRandomSeed = value;
                _adminClientConfig.EnableRandomSeed = value;
            }
        }

        /// <summary>
        /// Apache Kafka topic creation is asynchronous and it takes some time for a new
        /// topic to propagate throughout the cluster to all brokers. If a client requests
        /// topic metadata after manual topic creation but before the topic has been fully
        /// propagated to the broker the client is requesting metadata from, the topic will
        /// seem to be non-existent and the client will mark the topic as such, failing queued
        /// produced messages with `ERR__UNKNOWN_TOPIC`. This setting delays marking a topic
        /// as non-existent until the configured propagation max time has passed. The maximum
        /// propagation time is calculated from the time the topic is first referenced in
        /// the client, e.g., on produce(). default: 30000 importance: low
        /// </summary>
        [StreamConfigProperty("topic.metadata.propagation.max.ms")]
        public int? TopicMetadataPropagationMaxMs
        {
            get => _config.TopicMetadataPropagationMaxMs;
            set
            {
                _config.TopicMetadataPropagationMaxMs = value;
                _consumerConfig.TopicMetadataPropagationMaxMs = value;
                _producerConfig.TopicMetadataPropagationMaxMs = value;
                _adminClientConfig.TopicMetadataPropagationMaxMs = value;
            }
        }

        #endregion

        #region ConsumerConfig
        
        /// <summary>
        /// Controls how to read messages written transactionally: `read_committed` - only
        /// return transactional messages which have been committed. `read_uncommitted` -
        /// return all messages, even transactional messages which have been aborted. default:
        /// read_committed importance: high
        /// </summary>
        [StreamConfigProperty("isolation.level")]
        public IsolationLevel? IsolationLevel
        {
            get { return _consumerConfig.IsolationLevel; }
            set
            {
                if (Guarantee != ProcessingGuarantee.EXACTLY_ONCE)
                    _consumerConfig.IsolationLevel = value;
                else if (!changeGuarantee)
                    throw new StreamsException($"You can't update IsolationLevel because your processing guarantee is exactly-once");
            }
        }

        /// <summary>
        /// How long to postpone the next fetch request for a topic+partition in case of
        /// a fetch error. default: 500 importance: medium
        /// </summary>
        [StreamConfigProperty("fetch.error.backoff.ms")]
        public int? FetchErrorBackoffMs { get { return _consumerConfig.FetchErrorBackoffMs; } set { _consumerConfig.FetchErrorBackoffMs = value; } }

        /// <summary>
        /// Minimum number of bytes the broker responds with. If fetch.wait.max.ms expires
        /// the accumulated data will be sent to the client regardless of this setting. default:
        /// 1 importance: low
        /// </summary>
        [StreamConfigProperty("fetch.min.bytes")]
        public int? FetchMinBytes { get { return _consumerConfig.FetchMinBytes; } set { _consumerConfig.FetchMinBytes = value; } }

        /// <summary>
        /// Maximum amount of data the broker shall return for a Fetch request. Messages
        /// are fetched in batches by the consumer and if the first message batch in the
        /// first non-empty partition of the Fetch request is larger than this value, then
        /// the message batch will still be returned to ensure the consumer can make progress.
        /// The maximum message batch size accepted by the broker is defined via `message.max.bytes`
        /// (broker config) or `max.message.bytes` (broker topic config). `fetch.max.bytes`
        /// is automatically adjusted upwards to be at least `message.max.bytes` (consumer
        /// config). default: 52428800 importance: medium
        /// </summary>
        [StreamConfigProperty("fetch.max.bytes")]
        public int? FetchMaxBytes { get { return _consumerConfig.FetchMaxBytes; } set { _consumerConfig.FetchMaxBytes = value; } }

        /// <summary>
        /// Initial maximum number of bytes per topic+partition to request when fetching
        /// messages from the broker. If the client encounters a message larger than this
        /// value it will gradually try to increase it until the entire message can be fetched.
        /// default: 1048576 importance: medium
        /// </summary>
        [StreamConfigProperty("max.partition.fetch.bytes")]
        public int? MaxPartitionFetchBytes { get { return _consumerConfig.MaxPartitionFetchBytes; } set { _consumerConfig.MaxPartitionFetchBytes = value; } }

        /// <summary>
        /// Maximum time the broker may wait to fill the response with fetch.min.bytes. default:
        /// 100 importance: low
        /// </summary>
        [StreamConfigProperty("fetch.wait.max.ms")]
        public int? FetchWaitMaxMs { get { return _consumerConfig.FetchWaitMaxMs; } set { _consumerConfig.FetchWaitMaxMs = value; } }

        /// <summary>
        /// Maximum number of kilobytes per topic+partition in the local consumer queue.
        /// This value may be overshot by fetch.message.max.bytes. This property has higher
        /// priority than queued.min.messages. default: 1048576 importance: medium
        /// </summary>
        [StreamConfigProperty("queued.max.messages.kbytes")]
        public int? QueuedMaxMessagesKbytes { get { return _consumerConfig.QueuedMaxMessagesKbytes; } set { _consumerConfig.QueuedMaxMessagesKbytes = value; } }

        /// <summary>
        /// Minimum number of messages per topic+partition librdkafka tries to maintain in
        /// the local consumer queue. default: 100000 importance: medium
        /// </summary>
        [StreamConfigProperty("queued.min.messages")]
        public int? QueuedMinMessages { get { return _consumerConfig.QueuedMinMessages; } set { _consumerConfig.QueuedMinMessages = value; } }

        /// <summary>
        /// Automatically store offset of last message provided to application. The offset
        /// store is an in-memory store of the next offset to (auto-)commit for each partition.
        /// default: true importance: high
        /// </summary>
        [StreamConfigProperty("enable.auto.offset.store", true)]
        public bool? EnableAutoOffsetStore { get { return _consumerConfig.EnableAutoOffsetStore; } private set { _consumerConfig.EnableAutoOffsetStore = value; } }

        /// <summary>
        /// Automatically and periodically commit offsets in the background. Note: setting
        /// this to false does not prevent the consumer from fetching previously committed
        /// start offsets. To circumvent this behaviour set specific start offsets per partition
        /// in the call to assign(). default: true importance: high
        /// </summary>
        [StreamConfigProperty("enable.auto.commit", true)]
        public bool? EnableAutoCommit { get { return _consumerConfig.EnableAutoCommit; } private set { _consumerConfig.EnableAutoCommit = value; } }

        /// <summary>
        /// Maximum allowed time between calls to consume messages (e.g., rd_kafka_consumer_poll())
        /// for high-level consumers. If this interval is exceeded the consumer is considered
        /// failed and the group will rebalance in order to reassign the partitions to another
        /// consumer group member. Warning: Offset commits may be not possible at this point.
        /// Note: It is recommended to set `enable.auto.offset.store=false` for long-time
        /// processing applications and then explicitly store offsets (using offsets_store())
        /// *after* message processing, to make sure offsets are not auto-committed prior
        /// to processing has finished. The interval is checked two times per second. See
        /// KIP-62 for more information. default: 300000 importance: high
        /// </summary>
        [StreamConfigProperty("max.poll.interval.ms")]
        public int? MaxPollIntervalMs { get { return _consumerConfig.MaxPollIntervalMs; } set { _consumerConfig.MaxPollIntervalMs = value; } }

        /// <summary>
        /// How often to query for the current client group coordinator. If the currently
        /// assigned coordinator is down the configured query interval will be divided by
        /// ten to more quickly recover in case of coordinator reassignment. default: 600000
        /// importance: low
        /// </summary>
        [StreamConfigProperty("coordinator.query.interval.ms")]
        public int? CoordinatorQueryIntervalMs { get { return _consumerConfig.CoordinatorQueryIntervalMs; } set { _consumerConfig.CoordinatorQueryIntervalMs = value; } }

        /// <summary>
        /// Group protocol type default: consumer importance: low
        /// </summary>
        [StreamConfigProperty("group.protocol.type")]
        public string GroupProtocolType { get { return _consumerConfig.GroupProtocolType; } set { _consumerConfig.GroupProtocolType = value; } }

        /// <summary>
        /// Group session keepalive heartbeat interval. default: 3000 importance: low
        /// </summary>
        [StreamConfigProperty("heartbeat.interval.ms")]
        public int? HeartbeatIntervalMs { get { return _consumerConfig.HeartbeatIntervalMs; } set { _consumerConfig.HeartbeatIntervalMs = value; } }

        /// <summary>
        /// Client group session and failure detection timeout. The consumer sends periodic
        /// heartbeats (heartbeat.interval.ms) to indicate its liveness to the broker. If
        /// no hearts are received by the broker for a group member within the session timeout,
        /// the broker will remove the consumer from the group and trigger a rebalance. The
        /// allowed range is configured with the **broker** configuration properties `group.min.session.timeout.ms`
        /// and `group.max.session.timeout.ms`. Also see `max.poll.interval.ms`. default:
        /// 10000 importance: high
        /// </summary>
        [StreamConfigProperty("session.timeout.ms")]
        public int? SessionTimeoutMs { get { return _consumerConfig.SessionTimeoutMs; } set { _consumerConfig.SessionTimeoutMs = value; } }

        /// <summary>
        /// Name of partition assignment strategy to use when elected group leader assigns
        /// partitions to group members. default: range importance: medium
        /// </summary>
        [StreamConfigProperty("partition.assignment.strategy")]
        public PartitionAssignmentStrategy? PartitionAssignmentStrategy { get { return _consumerConfig.PartitionAssignmentStrategy; } set { _consumerConfig.PartitionAssignmentStrategy = value; } }

        /// <summary>
        /// Action to take when there is no initial offset in offset store or the desired
        /// offset is out of range: 'smallest','earliest' - automatically reset the offset
        /// to the smallest offset, 'largest','latest' - automatically reset the offset to
        /// the largest offset, 'error' - trigger an error which is retrieved by consuming
        /// messages and checking 'message->err'. default: largest importance: high
        /// </summary>
        [StreamConfigProperty("auto.offset.reset")]
        public AutoOffsetReset? AutoOffsetReset { get { return _consumerConfig.AutoOffsetReset; } set { _consumerConfig.AutoOffsetReset = value; } }

        /// <summary>
        /// A comma separated list of fields that may be optionally set in Confluent.Kafka.ConsumeResult`2
        /// objects returned by the Confluent.Kafka.Consumer`2.Consume(System.TimeSpan) method.
        /// Disabling fields that you do not require will improve throughput and reduce memory
        /// consumption. Allowed values: headers, timestamp, topic, all, none default: all
        /// importance: low
        /// </summary>
        [StreamConfigProperty("consume.result.fields")]
        public string ConsumeResultFields { set { _consumerConfig.ConsumeResultFields = value; } }

        /// <summary>
        /// Emit RD_KAFKA_RESP_ERR__PARTITION_EOF event whenever the consumer reaches the
        /// end of a partition. default: false importance: low
        /// </summary>
        [StreamConfigProperty("enable.partition.eof")]
        public bool? EnablePartitionEof { get { return _consumerConfig.EnablePartitionEof; } set { _consumerConfig.EnablePartitionEof = value; } }

        /// <summary>
        /// Verify CRC32 of consumed messages, ensuring no on-the-wire or on-disk corruption
        /// to the messages occurred. This check comes at slightly increased CPU usage. default:
        /// false importance: medium
        /// </summary>
        [StreamConfigProperty("check.crcs")]
        public bool? CheckCrcs { get { return _consumerConfig.CheckCrcs; } set { _consumerConfig.CheckCrcs = value; } }

        /// <summary>
        /// Allow automatic topic creation on the broker when subscribing to or assigning
        /// non-existent topics. The broker must also be configured with `auto.create.topics.enable=true`
        /// for this configuraiton to take effect. Note: The default value (false) is different
        /// from the Java consumer (true). Requires broker version >= 0.11.0.0, for older
        /// broker versions only the broker configuration applies. default: false importance:
        /// low
        /// </summary>
        [StreamConfigProperty("allow.auto.create.topics")]
        public bool? AllowAutoCreateTopics { get { return _consumerConfig.AllowAutoCreateTopics; } set { _consumerConfig.AllowAutoCreateTopics = value; } }

        #endregion

        #region ProducerConfig
        
        /// <summary>
        /// The threshold of outstanding not yet transmitted broker requests needed to backpressure
        /// the producer's message accumulator. If the number of not yet transmitted requests
        /// equals or exceeds this number, produce request creation that would have otherwise
        /// been triggered (for example, in accordance with linger.ms) will be delayed. A
        /// lower number yields larger and more effective batches. A higher value can improve
        /// latency when using compression on slow machines. default: 1 importance: low
        /// </summary>
        [StreamConfigProperty("queue.buffering.backpressure.threshold")]
        public int? QueueBufferingBackpressureThreshold { get { return _producerConfig.QueueBufferingBackpressureThreshold; } set { _producerConfig.QueueBufferingBackpressureThreshold = value; } }

        /// <summary>
        /// The backoff time in milliseconds before retrying a protocol request. default:
        /// 100 importance: medium
        /// </summary>
        [StreamConfigProperty("retry.backoff.ms")]
        public int? RetryBackoffMs { get { return _producerConfig.RetryBackoffMs; } set { _producerConfig.RetryBackoffMs = value; } }

        /// <summary>
        /// How many times to retry sending a failing Message. **Note:** retrying may cause
        /// reordering unless `enable.idempotence` is set to true. default: 2 importance:
        /// high
        /// </summary>
        [StreamConfigProperty("message.send.max.retries")]
        public int? MessageSendMaxRetries { get { return _producerConfig.MessageSendMaxRetries; } set { _producerConfig.MessageSendMaxRetries = value; } }

        /// <summary>
        /// Delay in milliseconds to wait for messages in the producer queue to accumulate
        /// before constructing message batches (MessageSets) to transmit to brokers. A higher
        /// value allows larger and more effective (less overhead, improved compression)
        /// batches of messages to accumulate at the expense of increased message delivery
        /// latency. default: 5 importance: high
        /// </summary>
        [StreamConfigProperty("linger.ms")]
        public double? LingerMs { get { return _producerConfig.LingerMs; } set { _producerConfig.LingerMs = value; } }

        /// <summary>
        /// Maximum total message size sum allowed on the producer queue. This queue is shared
        /// by all topics and partitions. This property has higher priority than queue.buffering.max.messages.
        /// default: 1048576 importance: high
        /// </summary>
        [StreamConfigProperty("queue.buffering.max.kbytes")]
        public int? QueueBufferingMaxKbytes { get { return _producerConfig.QueueBufferingMaxKbytes; } set { _producerConfig.QueueBufferingMaxKbytes = value; } }

        /// <summary>
        /// Maximum number of messages allowed on the producer queue. This queue is shared
        /// by all topics and partitions. default: 100000 importance: high
        /// </summary>
        [StreamConfigProperty("queue.buffering.max.messages")]
        public int? QueueBufferingMaxMessages { get { return _producerConfig.QueueBufferingMaxMessages; } set { _producerConfig.QueueBufferingMaxMessages = value; } }

        /// <summary>
        /// **EXPERIMENTAL**: subject to change or removal. When set to `true`, any error
        /// that could result in a gap in the produced message series when a batch of messages
        /// fails, will raise a fatal error (ERR__GAPLESS_GUARANTEE) and stop the producer.
        /// Messages failing due to `message.timeout.ms` are not covered by this guarantee.
        /// Requires `enable.idempotence=true`. default: false importance: low
        /// </summary>
        [StreamConfigProperty("enable.gapless.guarantee")]
        public bool? EnableGaplessGuarantee { get { return _producerConfig.EnableGaplessGuarantee; } set { _producerConfig.EnableGaplessGuarantee = value; } }

        /// <summary>
        /// When set to `true`, the producer will ensure that messages are successfully produced
        /// exactly once and in the original produce order. The following configuration properties
        /// are adjusted automatically (if not modified by the user) when idempotence is
        /// enabled: `max.in.flight.requests.per.connection=5` (must be less than or equal
        /// to 5), `retries=INT32_MAX` (must be greater than 0), `acks=all`, `queuing.strategy=fifo`.
        /// Producer instantation will fail if user-supplied configuration is incompatible.
        /// default: false importance: high
        /// </summary>
        [StreamConfigProperty("enable.idempotence")]
        public bool? EnableIdempotence
        {
            get { return _producerConfig.EnableIdempotence; }
            set
            {
                if (Guarantee != ProcessingGuarantee.EXACTLY_ONCE)
                    _producerConfig.EnableIdempotence = value;
                else if (!changeGuarantee)
                    throw new StreamsException($"You can't update EnableIdempotence because your processing guarantee is exactly-once");
            }
        }

        /// <summary>
        /// Enables the transactional producer. The transactional.id is used to identify
        /// the same transactional producer instance across process restarts. It allows the
        /// producer to guarantee that transactions corresponding to earlier instances of
        /// the same producer have been finalized prior to starting any new transactions,
        /// and that any zombie instances are fenced off. If no transactional.id is provided,
        /// then the producer is limited to idempotent delivery (if enable.idempotence is
        /// set). Requires broker version >= 0.11.0. default: '' importance: high
        /// </summary>
        [StreamConfigProperty("transaction.id")]
        public string TransactionalId { get { return _producerConfig.TransactionalId; } set { _producerConfig.TransactionalId = value; } }

        /// <summary>
        /// compression codec to use for compressing message sets. This is the default value
        /// for all topics, may be overridden by the topic configuration property `compression.codec`.
        /// default: none importance: medium
        /// </summary>
        [StreamConfigProperty("compression.type")]
        public CompressionType? CompressionType { get { return _producerConfig.CompressionType; } set { _producerConfig.CompressionType = value; } }

        ///  <summary>
        ///  Compression level parameter for algorithm selected by configuration property
        ///  `compression.codec`. Higher values will result in better compression at the cost
        ///  of more CPU usage. Usable range is algorithm-dependent: [0-9] for gzip; [0-12]
        ///  for lz4; only 0 for snappy; -1 = codec-dependent default compression level. default:
        ///  -1 importance: medium
        ///  </summary>
        [StreamConfigProperty("compression.level")]
        public int? CompressionLevel { get { return _producerConfig.CompressionLevel; } set { _producerConfig.CompressionLevel = value; } }

        /// <summary>
        /// Partitioner: `random` - random distribution, `consistent` - CRC32 hash of key
        /// (Empty and NULL keys are mapped to single partition), `consistent_random` - CRC32
        /// hash of key (Empty and NULL keys are randomly partitioned), `murmur2` - Java
        /// Producer compatible Murmur2 hash of key (NULL keys are mapped to single partition),
        /// `murmur2_random` - Java Producer compatible Murmur2 hash of key (NULL keys are
        /// randomly partitioned. This is functionally equivalent to the default partitioner
        /// in the Java Producer.). default: murmur2_random importance: high
        /// </summary>
        [StreamConfigProperty("partitioner")]
        public Partitioner? Partitioner { get { return _producerConfig.Partitioner; } set { _producerConfig.Partitioner = value; } }

        /// <summary>
        /// Local message timeout. This value is only enforced locally and limits the time
        /// a produced message waits for successful delivery. A time of 0 is infinite. This
        /// is the maximum time librdkafka may use to deliver a message (including retries).
        /// Delivery error occurs when either the retry count or the message timeout are
        /// exceeded. The message timeout is automatically adjusted to `transaction.timeout.ms`
        /// if `transactional.id` is configured. default: 300000 importance: high
        /// </summary>
        [StreamConfigProperty("message.timeout.ms")]
        public int? MessageTimeoutMs { get { return _producerConfig.MessageTimeoutMs; } set { _producerConfig.MessageTimeoutMs = value; } }

        /// <summary>
        /// The ack timeout of the producer request in milliseconds. This value is only enforced
        /// by the broker and relies on `request.required.acks` being != 0. default: 5000
        /// importance: medium
        /// </summary>
        [StreamConfigProperty("request.timeout.ms")]
        public int? RequestTimeoutMs { get { return _producerConfig.RequestTimeoutMs; } set { _producerConfig.RequestTimeoutMs = value; } }

        /// <summary>
        /// A comma separated list of fields that may be optionally set in delivery reports.
        /// Disabling delivery report fields that you do not require will improve maximum
        /// throughput and reduce memory usage. Allowed values: key, value, timestamp, headers,
        /// all, none. default: all importance: low
        /// </summary>
        [StreamConfigProperty("delivery.report.fields")]
        public string DeliveryReportFields { get { return _producerConfig.DeliveryReportFields; } set { _producerConfig.DeliveryReportFields = value; } }

        /// <summary>
        /// Specifies whether to enable notification of delivery reports. Typically you should
        /// set this parameter to true. Set it to false for "fire and forget" semantics and
        /// a small boost in performance. default: true importance: low
        /// </summary>
        [StreamConfigProperty("enable.delivery.reports")]
        public bool? EnableDeliveryReports { get { return _producerConfig.EnableDeliveryReports; } set { _producerConfig.EnableDeliveryReports = value; } }

        /// <summary>
        /// Specifies whether or not the producer should start a background poll thread to
        /// receive delivery reports and event notifications. Generally, this should be set
        /// to true. If set to false, you will need to call the Poll function manually. default:
        /// true importance: low
        /// </summary>
        [StreamConfigProperty("enable.background.poll")]
        public bool? EnableBackgroundPoll { get { return _producerConfig.EnableBackgroundPoll; } set { _producerConfig.EnableBackgroundPoll = value; } }

        /// <summary>
        /// The maximum amount of time in milliseconds that the transaction coordinator will
        /// wait for a transaction status update from the producer before proactively aborting
        /// the ongoing transaction. If this value is larger than the `transaction.max.timeout.ms`
        /// setting in the broker, the init_transactions() call will fail with ERR_INVALID_TRANSACTION_TIMEOUT.
        /// The transaction timeout automatically adjusts `message.timeout.ms` and `socket.timeout.ms`,
        /// unless explicitly configured in which case they must not exceed the transaction
        /// timeout (`socket.timeout.ms` must be at least 100ms lower than `transaction.timeout.ms`).
        /// default: 60000 importance: medium
        /// </summary>
        [StreamConfigProperty("transaction.timeout.ms")]
        public int? TransactionTimeoutMs { get { return _producerConfig.TransactionTimeoutMs; } set { _producerConfig.TransactionTimeoutMs = value; } }

        /// <summary>
        /// Maximum number of messages batched in one MessageSet. The total MessageSet size
        /// is also limited by message.max.bytes. default: 10000 importance: medium
        /// </summary>
        [StreamConfigProperty("batch.num.messages")]
        public int? BatchNumMessages { get { return _producerConfig.BatchNumMessages; } set { _producerConfig.BatchNumMessages = value; } }

        /// <summary>
        /// Maximum size (in bytes) of all messages batched in one MessageSet, including
        /// protocol framing overhead. This limit is applied after the first message has
        /// been added to the batch, regardless of the first message's size, this is to ensure
        /// that messages that exceed batch.size are produced. The total MessageSet size
        /// is also limited by batch.num.messages and message.max.bytes. default: 1000000
        /// importance: medium
        /// </summary>
        [StreamConfigProperty("batch.size")]
        public int? BatchSize { get { return _producerConfig.BatchSize; } set { _producerConfig.BatchSize = value; } }

        /// <summary>
        /// Delay in milliseconds to wait to assign new sticky partitions for each topic.
        /// By default, set to double the time of linger.ms. To disable sticky behavior,
        /// set to 0. This behavior affects messages with the key NULL in all cases, and
        /// messages with key lengths of zero when the consistent_random partitioner is in
        /// use. These messages would otherwise be assigned randomly. A higher value allows
        /// for more effective batching of these messages. default: 10 importance: low
        /// </summary>
        [StreamConfigProperty("sticky.partitioning.linger.ms")]
        public int? StickyPartitioningLingerMs { get { return _producerConfig.StickyPartitioningLingerMs; } set { _producerConfig.StickyPartitioningLingerMs = value; } }

        #endregion

        #region AdminConfig
        
        #endregion

        #region Ctor

        private void InitializeReflectedProperties()
        {
            foreach (var p in this.GetType().GetProperties())
            {
                var streamConfigAttr = p.GetCustomAttribute<StreamConfigPropertyAttribute>();
                if (streamConfigAttr != null && !streamConfigAttr.ReadOnly)
                    cacheProperties.Add(streamConfigAttr.KeyName, p);
            }
        }
        
        /// <summary>
        /// Constructor empty
        /// </summary>
        public StreamConfig() 
            : this(null)
        { }

        /// <summary>
        /// Constructor with a dictionary of properties.
        /// This is stream properties. 
        /// <para>
        /// If you want to set specific properties for consumer, producer, admin client or global, please use <see cref="IStreamConfig.AddConfig"/>.
        /// </para>
        /// </summary>
        /// <param name="properties">Dictionary of stream properties</param>
        public StreamConfig(IDictionary<string, dynamic> properties)
        {
            InitializeReflectedProperties();
            
            ClientId = null;
            NumStreamThreads = 1;
            DefaultKeySerDes = new ByteArraySerDes();
            DefaultValueSerDes = new ByteArraySerDes();
            DefaultTimestampExtractor = new FailOnInvalidTimestamp();
            Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            TransactionTimeout = TimeSpan.FromSeconds(10);
            PollMs = 100;
            MaxPollRecords = 500;
            MaxPollRestoringRecords = 1000;
            MaxTaskIdleMs = 0;
            BufferedRecordsPerPartition = Int32.MaxValue;
            InnerExceptionHandler = (_) => ExceptionHandlerResponse.FAIL;
            ProductionExceptionHandler = (_) => ProductionExceptionHandlerResponse.FAIL;
            DeserializationExceptionHandler = (_, _, _) => ExceptionHandlerResponse.FAIL;
            RocksDbConfigHandler = (_, _) => { };
            FollowMetadata = false;
            StateDir = Path.Combine(Path.GetTempPath(), "streamiz-kafka-net");
            ReplicationFactor = -1;
            WindowStoreChangelogAdditionalRetentionMs = (long)TimeSpan.FromDays(1).TotalMilliseconds;
            OffsetCheckpointManager = null;
            MetricsIntervalMs = (long)TimeSpan.FromSeconds(30).TotalMilliseconds;
            MetricsRecording = MetricsRecordingLevel.INFO;
            LogProcessingSummary = TimeSpan.FromMinutes(1);
            MetricsReporter = (_) => { }; // nothing by default, maybe another behavior in future
            ExposeLibrdKafkaStats = false;
            StartTaskDelayMs = 5000;
            ParallelProcessing = false;
            MaxDegreeOfParallelism = 8;
            StateStoreCacheMaxBytes = 5 * 1024 * 1024;

            _consumerConfig = new ConsumerConfig();
            _producerConfig = new ProducerConfig();
            _adminClientConfig = new AdminClientConfig();
            _config = new ClientConfig();

            MaxPollIntervalMs = 300000;
            EnableAutoCommit = false;
            EnableAutoOffsetStore = false;
            PartitionAssignmentStrategy = Confluent.Kafka.PartitionAssignmentStrategy.CooperativeSticky;
            Partitioner = Confluent.Kafka.Partitioner.Murmur2Random;

            Logger = LoggerFactory.Create(builder =>
            {
                builder.SetMinimumLevel(LogLevel.Information);
                builder.AddConsole();
            });
            
            if (properties != null)
            {
                foreach (var k in properties)
                    AddConfig(k.Key, k.Value);
            }
        }

        #endregion

        #region IStreamConfig Impl
        
        private static Dictionary<string, string> EnumNameToConfigValueSubstitutes = new Dictionary<string, string>()
        {
            {
                "saslplaintext",
                "sasl_plaintext"
            },
            {
                "saslssl",
                "sasl_ssl"
            },
            {
                "consistentrandom",
                "consistent_random"
            },
            {
                "murmur2random",
                "murmur2_random"
            },
            {
                "readcommitted",
                "read_committed"
            },
            {
                "readuncommitted",
                "read_uncommitted"
            },
            {
                "cooperativesticky",
                "cooperative-sticky"
            }
        };

        private void SetObject(ClientConfig properties, string name, object val)
        {
            if (val is Enum)
            {
                string lowerInvariant = val.ToString().ToLowerInvariant();
                if (EnumNameToConfigValueSubstitutes.TryGetValue(lowerInvariant, out string str))
                    properties.Set(name, str);
                else
                    properties.Set(name, lowerInvariant);
            }
            else
                properties.Set(name, val.ToString());
        }
        
        /// <summary>
        /// Add a new key/value configuration.
        /// </summary>
        /// <param name="key">New key</param>
        /// <param name="value">New value</param>
        public void AddConfig(string key, dynamic value)
        {
            if(cacheProperties.ContainsKey(key))
                cacheProperties[key].SetValue(this, value);
            else
            {
                if (key.StartsWith(mainConsumerPrefix))
                    SetObject(_overrideMainConsumerConfig, key.Replace(mainConsumerPrefix, string.Empty), value);
                else  if (key.StartsWith(globalConsumerPrefix))
                    SetObject(_overrideGlobalConsumerConfig, key.Replace(globalConsumerPrefix, string.Empty), value);
                else if (key.StartsWith(restoreConsumerPrefix))
                    SetObject(_overrideRestoreConsumerConfig, key.Replace(restoreConsumerPrefix, string.Empty), value);
                else if (key.StartsWith(producerPrefix))
                    SetObject(_overrideProducerConfig, key.Replace(producerPrefix, string.Empty), value);
                else if(key.StartsWith(externalConsumerPrefix))
                    SetObject(_overrideExternalConsumerConfig, key.Replace(externalConsumerPrefix, string.Empty), value);
                else if(key.StartsWith(externalProducerPrefix))
                    SetObject(_overrideExternalProducerConfig, key.Replace(externalProducerPrefix, string.Empty), value);
                else
                    configProperties.AddOrUpdate(key, (object)value);
            }
        }
        
        /// <summary>
        /// Authorize your streams application to follow metadata (timestamp, topic, partition, offset and headers) during processing record.
        /// You can use <see cref="StreamizMetadata"/> to get these metadatas. (Default : false)
        /// </summary>
        [StreamConfigProperty("" + followMetadataCst)]
        public bool FollowMetadata
        {
            get => configProperties[followMetadataCst];
            set => configProperties.AddOrUpdate(followMetadataCst, value);
        }

        /// <summary>
        /// The number of threads to execute stream processing.
        /// </summary>
        [StreamConfigProperty("" + numStreamThreadsCst)]
        public int NumStreamThreads
        {
            get => configProperties[numStreamThreadsCst];
            set
            {
                if (value >= 1)
                    configProperties.AddOrUpdate(numStreamThreadsCst, value);
                else
                    throw new StreamConfigException($"NumStreamThreads value must always be greather than 1");
            }
        }

        /// <summary>
        /// An ID prefix string used for the client IDs of internal consumer, producer and restore-consumer, with pattern '&lt;client.id&gt;-StreamThread-&lt;threadSequenceNumber&gt;-&lt;consumer|producer|restore-consumer&gt;'.
        /// </summary>
        [StreamConfigProperty("" + clientIdCst)]
        public string ClientId
        {
            get => configProperties[clientIdCst];
            set => configProperties.AddOrUpdate(clientIdCst, value);
        }

        /// <summary>
        /// An identifier for the stream processing application. Must be unique within the Kafka cluster. It is used as 1) the default client-id prefix, 2) the group-id for membership management, 3) the changelog topic prefix.
        /// </summary>
        [StreamConfigProperty("" + applicatonIdCst)]
        public string ApplicationId
        {
            get => configProperties.ContainsKey(applicatonIdCst) ? configProperties[applicatonIdCst] : null;
            set => configProperties.AddOrUpdate(applicatonIdCst, value);
        }

        /// <summary>
        /// Default key serdes for consumer and materialized state store
        /// </summary>
        [StreamConfigProperty("" + defaultKeySerDesCst)]
        public ISerDes DefaultKeySerDes
        {
            get => configProperties[defaultKeySerDesCst];
            set => configProperties.AddOrUpdate(defaultKeySerDesCst, value);
        }

        /// <summary>
        /// Default value serdes for consumer and materialized state store
        /// </summary>
        [StreamConfigProperty("" + defaultValueSerDesCst)]
        public ISerDes DefaultValueSerDes
        {
            get => configProperties[defaultValueSerDesCst];
            set => configProperties.AddOrUpdate(defaultValueSerDesCst, value);
        }

        /// <summary>
        /// Default timestamp extractor class that implements the <see cref="ITimestampExtractor"/> interface.
        /// </summary>
        [StreamConfigProperty("" + defaultTimestampExtractorCst)]
        public ITimestampExtractor DefaultTimestampExtractor
        {
            get => configProperties[defaultTimestampExtractorCst];
            set => configProperties.AddOrUpdate(defaultTimestampExtractorCst, value);
        }

        /// <summary>
        /// Initial list of brokers as a CSV list of broker host or host:port. default:
        /// '' importance: high
        /// </summary>
        [StreamConfigProperty("bootstrap.servers")]
        public string BootstrapServers
        {
            get => _config.BootstrapServers;
            set
            {
                _config.BootstrapServers = value;
                _consumerConfig.BootstrapServers = value;
                _producerConfig.BootstrapServers = value;
                _adminClientConfig.BootstrapServers = value;
            }
        }

        /// <summary>
        /// The processing guarantee that should be used. Possible values are <see cref="ProcessingGuarantee.AT_LEAST_ONCE"/> (default) and <see cref="ProcessingGuarantee.EXACTLY_ONCE"/>
        /// Note that exactly-once processing requires a cluster of at least three brokers by default what is the recommended setting for production; for development you can change this, by adjusting broker setting
        /// <code>transaction.state.log.replication.factor</code> and <code>transaction.state.log.min.isr</code>.
        /// </summary>
        [StreamConfigProperty("" + processingGuaranteeCst)]
        public ProcessingGuarantee Guarantee
        {
            get => configProperties[processingGuaranteeCst];
            set
            {
                changeGuarantee = true;
                if (value == ProcessingGuarantee.EXACTLY_ONCE)
                {
                    IsolationLevel = Confluent.Kafka.IsolationLevel.ReadCommitted;
                    EnableIdempotence = true;
                    MaxInFlight = 5;
                    MessageSendMaxRetries = Int32.MaxValue;
                    CommitIntervalMs = EOS_DEFAULT_COMMIT_INTERVAL_MS;
                }
                else if (value == ProcessingGuarantee.AT_LEAST_ONCE)
                    CommitIntervalMs = DEFAULT_COMMIT_INTERVAL_MS;

                configProperties.AddOrUpdate(processingGuaranteeCst, value);
                changeGuarantee = false;
            }
        }

        /// <summary>
        /// Timeout used for transaction related operations. (Default : 10 seconds).
        /// </summary>
        [StreamConfigProperty("" + transactionTimeoutCst)]
        public TimeSpan TransactionTimeout
        {
            get => configProperties[transactionTimeoutCst];
            set => configProperties.AddOrUpdate(transactionTimeoutCst, value);
        }

        /// <summary>
        /// The frequency with which to save the position of the processor. (Note, if <see cref="IStreamConfig.Guarantee"/> is set to <see cref="ProcessingGuarantee.EXACTLY_ONCE"/>, the default value is <see cref="StreamConfig.EOS_DEFAULT_COMMIT_INTERVAL_MS"/>,
        /// otherwise the default value is <see cref="StreamConfig.DEFAULT_COMMIT_INTERVAL_MS"/>)
        /// </summary>
        [StreamConfigProperty("" + commitIntervalMsCst)]
        public long CommitIntervalMs
        {
            get => configProperties[commitIntervalMsCst];
            set => configProperties.AddOrUpdate(commitIntervalMsCst, value);
        }

        /// <summary>
        /// The amount of time in milliseconds to block waiting for input. (Default : 100)
        /// </summary>
        [StreamConfigProperty("" + pollMsCst)]
        public long PollMs
        {
            get => configProperties[pollMsCst];
            set => configProperties.AddOrUpdate(pollMsCst, value);
        }

        /// <summary>
        /// The maximum number of records returned in a single call to poll(). (Default: 500)
        /// </summary>
        [StreamConfigProperty("" + maxPollRecordsCst)]
        public long MaxPollRecords
        {
            get => configProperties[maxPollRecordsCst];
            set => configProperties.AddOrUpdate(maxPollRecordsCst, value);
        }

        /// <summary>
        /// The maximum number of records returned in a polling restore phase. (Default: 1000)
        /// </summary>
        [StreamConfigProperty("" + maxPollRestoringRecordsCst)]
        public long MaxPollRestoringRecords
        {
            get => configProperties[maxPollRestoringRecordsCst];
            set => configProperties.AddOrUpdate(maxPollRestoringRecordsCst, value);
        }

        /// <summary>
        /// Maximum amount of time a stream task will stay idle when not all of its partition buffers contain records, to avoid potential out-of-order record processing across multiple input streams. (Default: 0)
        /// </summary>
        [StreamConfigProperty("" + maxTaskIdleCst)]
        public long MaxTaskIdleMs
        {
            get => configProperties[maxTaskIdleCst];
            set => configProperties.AddOrUpdate(maxTaskIdleCst, value);
        }

        /// <summary>
        /// Maximum number of records to buffer per partition. (Default: 1000)
        /// </summary>
        [StreamConfigProperty("" + bufferedRecordsPerPartitionCst)]
        [Obsolete("Librdkafka clients manage internally a backpressure. So this configuration will be remove in the next release.")]
        public long BufferedRecordsPerPartition
        {
            get => configProperties[bufferedRecordsPerPartitionCst];
            set => configProperties.AddOrUpdate(bufferedRecordsPerPartitionCst, value);
        }

        /// <summary>
        /// Directory location for state store. This path must be unique for each streams instance sharing the same underlying filesystem.
        /// Default value : $TMP_DIR_ENVIRONMENT$/streamiz-kafka-net
        /// </summary>
        [StreamConfigProperty("" + stateDirCst)]
        public string StateDir
        {
            get => configProperties[stateDirCst];
            set => configProperties.AddOrUpdate(stateDirCst, value);
        }

        /// <summary>
        /// The replication factor for change log topics topics created by the stream processing application. Default is -1, meaning default broker replication factor setting.
        /// </summary>
        [StreamConfigProperty("" + replicationFactorCst)]
        public int ReplicationFactor
        {
            get => configProperties[replicationFactorCst];
            set => configProperties.AddOrUpdate(replicationFactorCst, value);
        }

        /// <summary>
        /// Added to a windows maintainMs to ensure data is not deleted from the log prematurely. Allows for clock drift. Default is 1 day.
        /// </summary>
        [StreamConfigProperty("" + windowstoreChangelogAdditionalRetentionMsCst)]
        public long WindowStoreChangelogAdditionalRetentionMs
        {
            get => configProperties[windowstoreChangelogAdditionalRetentionMsCst];
            set => configProperties.AddOrUpdate(windowstoreChangelogAdditionalRetentionMsCst, value);
        }

        /// <summary>
        /// Manager which track offset saved in local state store
        /// </summary>
        [StreamConfigProperty("" + offsetCheckpointManagerCst)]
        public IOffsetCheckpointManager OffsetCheckpointManager
        {
            get => configProperties[offsetCheckpointManagerCst];
            set => configProperties.AddOrUpdate(offsetCheckpointManagerCst, value);
        }
        
        /// <summary>
        /// A Rocks DB config handler function
        /// </summary>
        [StreamConfigProperty("" + rocksDbConfigSetterCst)]
        public Action<string, RocksDbOptions> RocksDbConfigHandler
        {
            get => configProperties[rocksDbConfigSetterCst];
            set => configProperties.AddOrUpdate(rocksDbConfigSetterCst, value);
        }

        /// <summary>
        /// Inner exception handling function called during processing.
        /// </summary>
        [StreamConfigProperty("" + innerExceptionHandlerCst)]
        public Func<Exception, ExceptionHandlerResponse> InnerExceptionHandler
        {
            get => configProperties[innerExceptionHandlerCst];
            set => configProperties.AddOrUpdate(innerExceptionHandlerCst, value);
        }

        /// <summary>
        /// Deserialization exception handling function called when deserialization exception during kafka consumption is raise.
        /// </summary>
        [StreamConfigProperty("" + deserializationExceptionHandlerCst)]
        public Func<ProcessorContext, ConsumeResult<byte[], byte[]>, Exception, ExceptionHandlerResponse> DeserializationExceptionHandler
        {
            get => configProperties[deserializationExceptionHandlerCst];
            set => configProperties.AddOrUpdate(deserializationExceptionHandlerCst, value);
        }

        /// <summary>
        /// Production exception handling function called when kafka produce exception is raise.
        /// </summary>
        [StreamConfigProperty("" + productionExceptionHandlerCst)]
        public Func<DeliveryReport<byte[], byte[]>, ProductionExceptionHandlerResponse> ProductionExceptionHandler  
        {
            get => configProperties[productionExceptionHandlerCst];
            set => configProperties.AddOrUpdate(productionExceptionHandlerCst, value);
        }

        /// <summary>
        /// Delay between two invocations of MetricsReporter().
        /// Minimum and default value : 30 seconds
        /// </summary>
        [StreamConfigProperty("" + metricsIntervalMsCst)]
        public long MetricsIntervalMs
        {
            get => configProperties[metricsIntervalMsCst];
            set
            {
                if (value < MetricsMinIntervalMs)
                    value = MetricsMinIntervalMs;
                configProperties.AddOrUpdate(metricsIntervalMsCst, value);
            }
        }
        
        /// <summary>
        /// The reporter expose a list of sensors throw by a stream thread every <see cref="MetricsIntervalMs"/>.
        /// This reporter has the responsibility to export sensors and metrics into another platform.
        /// Streamiz package provide one reporter for Prometheus (see Streamiz.Kafka.Net.Metrics.Prometheus package).
        /// </summary>
        [StreamConfigProperty("" + metricsReportCst)]
        public Action<IEnumerable<Sensor>> MetricsReporter
        {
            get => configProperties[metricsReportCst];
            set => configProperties.AddOrUpdate(metricsReportCst, value);
        }
        
        /// <summary>
        /// Boolean which indicate if librdkafka handle statistics should be exposed ot not. (default: false)
        /// Only mainConsumer and producer will be concerned.
        /// </summary>
        [StreamConfigProperty("" + exposeLibrdKafkaCst)]
        public bool ExposeLibrdKafkaStats 
        {
            get => configProperties[exposeLibrdKafkaCst];
            set => configProperties.AddOrUpdate(exposeLibrdKafkaCst, value);
        }
        
        /// <summary>
        /// The highest recording level for metrics (default: INFO).
        /// </summary>
        [StreamConfigProperty("" + metricsRecordingLevelCst)]
        public MetricsRecordingLevel MetricsRecording 
        {
            get => configProperties[metricsRecordingLevelCst];
            set => configProperties.AddOrUpdate(metricsRecordingLevelCst, value);
        }

        /// <summary>
        /// Time wait before completing the start task of <see cref="KafkaStream"/>. (default: 5000)
        /// </summary>
        [StreamConfigProperty("" + startTaskDelayMsCst)]
        public long StartTaskDelayMs
        {
            get => configProperties[startTaskDelayMsCst];
            set => configProperties.AddOrUpdate(startTaskDelayMsCst, value);
        }

        /// <summary>
        /// Enables parallel processing for messages (default: false)
        /// </summary>
        [StreamConfigProperty("" + parallelProcessingCst)]
        public bool ParallelProcessing
        {
            get => configProperties[parallelProcessingCst];
            set => configProperties.AddOrUpdate(parallelProcessingCst, value);
        }

        /// <summary>
        /// The max number of concurrent messages processing by thread. (default: 8)
        /// Only valid if ParallelProcessing is true
        /// </summary>
        [StreamConfigProperty("" + maxDegreeOfParallelismCst)]
        public int MaxDegreeOfParallelism
        {
            get => configProperties[maxDegreeOfParallelismCst];
            set => configProperties.AddOrUpdate(maxDegreeOfParallelismCst, value);
        }
        
        /// <summary>
        /// Aims to log (info level) processing summary records per thread every X minutes. (default: 1 minute)
        /// </summary>
        [StreamConfigProperty("" + logProcessingSummaryCst)]
        public TimeSpan LogProcessingSummary
        {
            get => configProperties[logProcessingSummaryCst];
            set => configProperties.AddOrUpdate(logProcessingSummaryCst, value);
        }
        
        /// <summary>
        /// Maximum number of memory bytes to be used for state stores cache for a single store. (default: 5Mb)
        /// </summary>
        [StreamConfigProperty("" + stateStoreCacheMaxBytesCst)]

        public long StateStoreCacheMaxBytes        
        {
            get => configProperties[stateStoreCacheMaxBytesCst];
            set => configProperties.AddOrUpdate(stateStoreCacheMaxBytesCst, value);
        }

        /// <summary>
        /// Get the configs to the <see cref="IProducer{TKey, TValue}"/>
        /// </summary>
        /// <returns>Return <see cref="ProducerConfig"/> for building <see cref="IProducer{TKey, TValue}"/> instance.</returns>
        public ProducerConfig ToProducerConfig() => ToProducerConfig(ClientId);

        /// <summary>
        /// Get the configs to the <see cref="IProducer{TKey, TValue}"/> with specific <paramref name="clientId"/>
        /// </summary>
        /// <param name="clientId">Producer client ID</param>
        /// <returns>Return <see cref="ProducerConfig"/> for building <see cref="IProducer{TKey, TValue}"/> instance.</returns>
        public ProducerConfig ToProducerConfig(string clientId)
        {
            ProducerConfig config = new ProducerConfig(_producerConfig.Union(_config).Distinct(new KeyValueComparer()).ToDictionary());
            foreach(var kv in _overrideProducerConfig)
                config.Set(kv.Key, kv.Value);
            config.ClientId = clientId;
            return config;
        }

        /// <summary>
        /// Get the configs to the external <see cref="IProducer{TKey, TValue}"/> with specific <paramref name="clientId"/>
        /// </summary>
        /// <param name="clientId">Producer client ID</param>
        /// <returns>Return <see cref="ProducerConfig"/> for building <see cref="IProducer{TKey, TValue}"/> instance.</returns>
        public ProducerConfig ToExternalProducerConfig(string clientId)
        {
            ProducerConfig config = new ProducerConfig(_producerConfig.Union(_config).Distinct(new KeyValueComparer()).ToDictionary());
            foreach(var kv in _overrideExternalProducerConfig)
                config.Set(kv.Key, kv.Value);
            config.ClientId = clientId;
            return config;
        }

        /// <summary>
        /// Get the configs to the <see cref="IConsumer{TKey, TValue}"/>
        /// </summary>
        /// <returns>Return <see cref="ConsumerConfig"/> for building <see cref="IConsumer{TKey, TValue}"/> instance.</returns>
        public ConsumerConfig ToConsumerConfig() => ToConsumerConfig(ClientId);

        /// <summary>
        /// Get the configs to the <see cref="IConsumer{TumerKey, TValue}"/> with specific <paramref name="clientId"/>
        /// </summary>
        /// <param name="clientId">Consumer client ID</param>
        /// <param name="override">Override the configuration or not</param>
        /// <returns>Return <see cref="ConsumerConfig"/> for building <see cref="IConsumer{TKey, TValue}"/> instance.</returns>
        public ConsumerConfig ToConsumerConfig(string clientId, bool @override = true)
        {
            if (!configProperties.ContainsKey(applicatonIdCst))
                throw new StreamConfigException($"Key {applicatonIdCst} was not found. She is mandatory for getting consumer config");

            var config = new ConsumerConfig(_consumerConfig.Union(_config).Distinct(new KeyValueComparer()).ToDictionary());
            if(@override)
            {
                _overrideMainConsumerConfig.EnableAutoCommit = false;
                _overrideMainConsumerConfig.EnableAutoOffsetStore = false;
                foreach (var kv in _overrideMainConsumerConfig)
                    config.Set(kv.Key, kv.Value);
            }
            config.GroupId = ApplicationId;
            config.ClientId = clientId;
            return config;
        }

        /// <summary>
        /// Get the configs to the external <see cref="IConsumer{TumerKey, TValue}"/> with specific <paramref name="clientId"/>
        /// </summary>
        /// <param name="clientId">Consumer client ID</param>
        /// <returns>Return <see cref="ConsumerConfig"/> for building <see cref="IConsumer{TKey, TValue}"/> instance.</returns>
        public ConsumerConfig ToExternalConsumerConfig(string clientId)
        {
            if (!configProperties.ContainsKey(applicatonIdCst))
                throw new StreamConfigException($"Key {applicatonIdCst} was not found. She is mandatory for getting consumer config");

            var config = new ConsumerConfig(_consumerConfig.Union(_config).Distinct(new KeyValueComparer()).ToDictionary());
            foreach (var kv in _overrideExternalConsumerConfig)
                config.Set(kv.Key, kv.Value);
            
            config.EnableAutoCommit = false;
            config.EnableAutoOffsetStore = false;
            config.GroupId = ApplicationId;
            config.ClientId = clientId;
            return config;
        }

        /// <summary>
        /// Get the configs to the restore <see cref="IConsumer{TKey, TValue}"/> with specific <paramref name="clientId"/>.
        /// Restore consumer is using to restore persistent state store.
        /// </summary>
        /// <param name="clientId">Consumer client ID</param>
        /// <returns>Return <see cref="ConsumerConfig"/> for building <see cref="IConsumer{TKey, TValue}"/> instance.</returns>
        public ConsumerConfig ToRestoreConsumerConfig(string clientId)
        {
            var config = ToConsumerConfig(clientId, false);
            _overrideRestoreConsumerConfig.EnableAutoCommit = false;
            _overrideRestoreConsumerConfig.EnableAutoOffsetStore = false;
            foreach(var kv in _overrideRestoreConsumerConfig)
                config.Set(kv.Key, kv.Value);
            config.GroupId = $"{ApplicationId}-restore-group";
            return config;
        }

        /// <summary>
        /// Get the configs to the restore <see cref="IConsumer{TKey, TValue}"/> with specific <paramref name="clientId"/>.
        /// Global consumer is using to update the global state stores.
        /// </summary>
        /// <param name="clientId">Consumer client ID</param>
        /// <returns>Return <see cref="ConsumerConfig"/> for building <see cref="IConsumer{TKey, TValue}"/> instance.</returns>
        public ConsumerConfig ToGlobalConsumerConfig(string clientId)
        {
            var config = ToConsumerConfig(clientId, false);
            _overrideGlobalConsumerConfig.EnableAutoCommit = false;
            _overrideGlobalConsumerConfig.EnableAutoOffsetStore = false;
            foreach(var kv in _overrideGlobalConsumerConfig)
                config.Set(kv.Key, kv.Value);
            config.GroupId = $"{ApplicationId}-Global-{Guid.NewGuid()}";
            config.AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest;
            return config;
        }

        /// <summary>
        /// Get the configs to the <see cref="IAdminClient"/> with specific <paramref name="clientId"/>
        /// </summary>
        /// <param name="clientId">Admin client ID</param>
        /// <returns>Return <see cref="AdminClientConfig"/> for building <see cref="IAdminClient"/> instance.</returns>
        public AdminClientConfig ToAdminConfig(string clientId)
        {
            var config = new AdminClientConfig(_adminClientConfig.Union(_config).Distinct(new KeyValueComparer()).ToDictionary());
            config.ClientId = clientId;
            return config;
        }
        
        /// <summary>
        /// Get the config value of the key. Null if any key found
        /// </summary>
        /// <param name="key">Key searched</param>
        /// <returns>Return the config value of the key, null otherwise</returns>
        public dynamic Get(string key)
        {
            if (configProperties.ContainsKey(key))
                return configProperties[key];

            if (key.StartsWith("main.consumer."))
                return _overrideMainConsumerConfig.Get(key.Replace("main.consumer.", ""));
            if (key.StartsWith("restore.consumer."))
                return _overrideRestoreConsumerConfig.Get(key.Replace("restore.consumer.", ""));
            if (key.StartsWith("global.consumer."))
                return _overrideGlobalConsumerConfig.Get(key.Replace("global.consumer.", ""));
            if (key.StartsWith("producer."))
                return _overrideProducerConfig.Get(key.Replace("producer.", ""));
            if (cacheProperties.ContainsKey(key))
                return cacheProperties[key].GetValue(this);
            
            var allConfigs = _adminClientConfig
                .Union(_consumerConfig)
                .Union(_producerConfig)
                .Distinct(new KeyValueComparer())
                .ToDictionary();
            
            return allConfigs.ContainsKey(key) ? allConfigs[key] : null;
        }
        
        /// <summary>
        /// Return new instance of <see cref="StreamConfig"/>.
        /// </summary>
        /// <returns>Return new instance of <see cref="StreamConfig"/>.</returns>
        public IStreamConfig Clone()
        {
            var config = new StreamConfig();

            config._consumerConfig = new ConsumerConfig(_consumerConfig);
            config._producerConfig = new ProducerConfig(_producerConfig);
            config._adminClientConfig = new AdminClientConfig(_adminClientConfig);
            config._config = new ClientConfig(_config);
            
            foreach(var kv in configProperties)
                config.AddConfig(kv.Key, kv.Value);

            config.Logger = Logger;

            return config;
        }

        #endregion

        #region ISchemaRegistryConfig Impl

        /// <summary>
        /// Specifies the timeout for requests to Confluent Schema Registry. default: 30000
        /// </summary>
        [StreamConfigProperty("" + schemaRegistryRequestTimeoutMsCst)]
        public int? SchemaRegistryRequestTimeoutMs
        {
            get => configProperties.ContainsKey(schemaRegistryRequestTimeoutMsCst) ? configProperties[schemaRegistryRequestTimeoutMsCst] : null;
            set => configProperties.AddOrUpdate(schemaRegistryRequestTimeoutMsCst, value);
        }

        /// <summary>
        /// Specifies the maximum number of schemas CachedSchemaRegistryClient should cache locally. default: 1000
        /// </summary>
        [StreamConfigProperty("" + schemaRegistryMaxCachedSchemasCst)]
        public int? SchemaRegistryMaxCachedSchemas
        {
            get => configProperties.ContainsKey(schemaRegistryMaxCachedSchemasCst) ? configProperties[schemaRegistryMaxCachedSchemasCst] : null;
            set => configProperties.AddOrUpdate(schemaRegistryMaxCachedSchemasCst, value);
        }

        /// <summary>
        /// A comma-separated list of URLs for schema registry instances that are used register or lookup schemas.
        /// </summary>
        [StreamConfigProperty("" + schemaRegistryUrlCst)]
        public string SchemaRegistryUrl
        {
            get => configProperties.ContainsKey(schemaRegistryUrlCst) ? configProperties[schemaRegistryUrlCst] : null;
            set => configProperties.AddOrUpdate(schemaRegistryUrlCst, value);
        }

        /// <summary>
        ///    BasicAuthUserInfo
        /// </summary>
        [StreamConfigProperty("" + schemaRegistryBasicAuthUserInfoCst)]
        public string BasicAuthUserInfo
        {
            get => configProperties.ContainsKey(schemaRegistryBasicAuthUserInfoCst) ? configProperties[schemaRegistryBasicAuthUserInfoCst] : null;
            set => configProperties.AddOrUpdate(schemaRegistryBasicAuthUserInfoCst, value);
        }

        /// <summary>
        ///    BasicAuthCredentialsSource
        /// </summary>
        [StreamConfigProperty("" + schemaRegistryBasicAuthCredentialSourceCst)]
        public int? BasicAuthCredentialsSource
        {
            get => configProperties.ContainsKey(schemaRegistryBasicAuthCredentialSourceCst) ? configProperties[schemaRegistryBasicAuthCredentialSourceCst] : null;
            set => configProperties.AddOrUpdate(schemaRegistryBasicAuthCredentialSourceCst, value);
        }

        /// <summary>
        /// Specifies whether or not the serializer should attempt to auto-register unrecognized schemas with Confluent Schema Registry. default: true
        /// </summary>
        [StreamConfigProperty("auto.register.schemas")]
        public bool? AutoRegisterSchemas
        {
            get => configProperties.ContainsKey(avroSerializerAutoRegisterSchemasCst) ? configProperties[avroSerializerAutoRegisterSchemasCst] : null;
            set
            {
                configProperties.AddOrUpdate(avroSerializerAutoRegisterSchemasCst, value);
                configProperties.AddOrUpdate(protobufAutoRegisterSchemasCst, value);
            }
        }

        /// <summary>
        /// The subject name strategy to use for schema registration / lookup. Possible values: <see cref="Streamiz.Kafka.Net.SubjectNameStrategy" />
        /// </summary>
        [StreamConfigProperty("subject.name.strategy")]
        public SubjectNameStrategy? SubjectNameStrategy
        {
            get => configProperties.ContainsKey(avroSerializerSubjectNameStrategyCst) ? configProperties[avroSerializerSubjectNameStrategyCst] : null;
            set
            {
                configProperties.AddOrUpdate(avroSerializerSubjectNameStrategyCst, value);
                configProperties.AddOrUpdate(protobufSerializerSubjectNameStrategyCst, value);
            }
        }

        /// <summary>
        ///    Specifies the initial size (in bytes) of the buffer used for message
        ///    serialization. Use a value high enough to avoid resizing the buffer, but small
        ///    enough to avoid excessive memory use. Inspect the size of the byte array returned
        ///    by the Serialize method to estimate an appropriate value. Note: each call to
        ///    serialize creates a new buffer. default: 1024
        /// </summary>
        [StreamConfigProperty("serializer.buffer.bytes")]
        public int? BufferBytes
        {
            get => configProperties.ContainsKey(avroSerializerBufferBytesCst) ? configProperties[avroSerializerBufferBytesCst] : null;
            set
            {
                configProperties.AddOrUpdate(avroSerializerBufferBytesCst, value);
                configProperties.AddOrUpdate(protobufSerializerBufferBytesCst, value);
            }
        }

        /// <summary>
        ///    Specifies whether or not the serializer should use the latest subject
        ///    version for serialization. WARNING: There is no check that the latest schema
        ///    is backwards compatible with the schema of the object being serialized. default:
        ///    false
        /// </summary>
        [StreamConfigProperty("serializer.use.last.version")]
        public bool? UseLatestVersion
        {
            get => configProperties.ContainsKey(avroSerializerUseLatestVersionCst) ? configProperties[avroSerializerUseLatestVersionCst] : null;
            set
            {
                configProperties.AddOrUpdate(avroSerializerUseLatestVersionCst, value);
                configProperties.AddOrUpdate(protobufSerializerUseLatestVersionCst, value);
            }
        }

        /// <summary>
        ///    Specifies whether or not the Protobuf serializer should skip known types when
        ///    resolving dependencies. default: false
        /// </summary>
        [StreamConfigProperty("serializer.skip.known.types")]
        public bool? SkipKnownTypes
        {
            get => configProperties.ContainsKey(protobufSerializerSkipKnownTypesCst) ? configProperties[protobufSerializerSkipKnownTypesCst] : null;
            set => configProperties.AddOrUpdate(protobufSerializerSkipKnownTypesCst, value);
        }

        /// <summary>
        ///    Specifies whether the Protobuf serializer should serialize message indexes without
        ///    zig-zag encoding. default: false
        /// </summary>
        [StreamConfigProperty("serializer.use.deprecated.format")]
        public bool? UseDeprecatedFormat
        {
            get => configProperties.ContainsKey(protobufSerializerUseDeprecatedFormatCst) ? configProperties[protobufSerializerUseDeprecatedFormatCst] : null;
            set => configProperties.AddOrUpdate(protobufSerializerUseDeprecatedFormatCst, value);
        }

        /// <summary>
        ///    Reference subject name strategy. default: ReferenceSubjectNameStrategy.ReferenceName
        /// </summary>
        [StreamConfigProperty("serializer.reference.subject.name.strategy")]
        public ReferenceSubjectNameStrategy? ReferenceSubjectNameStrategy
        {
            get => configProperties.ContainsKey(protobufSerializerReferenceSubjectNameStrategyCst) ? configProperties[protobufSerializerReferenceSubjectNameStrategyCst] : null;
            set => configProperties.AddOrUpdate(protobufSerializerReferenceSubjectNameStrategyCst, value);
        }

        #endregion

        #region ToString()

        /// <summary>
        /// Override ToString method
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            string replaceValue = "********";
            List<string> keysToNotDisplay = new List<string> {
                "sasl.password",
                "ssl.key.password",
                "ssl.keystore.password",
                "ssl.key.pem",
                "ssl.certificate.pem"
            };

            List<string> keysAlreadyPrint = new List<string>();

            StringBuilder sb = new StringBuilder();

            // stream config property
            sb.AppendLine();
            sb.AppendLine("\tStream property:");
            foreach (var kp in configProperties.Keys)
                sb.AppendLine($"\t\t{kp}: \t{configProperties[kp]}");

            // client config property
            sb.AppendLine("\tClient property:");
            var configs = _config.Intercept((kp) => keysToNotDisplay.Contains(kp.Key), replaceValue);
            foreach (var kp in configs)
            {
                sb.AppendLine($"\t\t{kp.Key}: \t{kp.Value}");
                keysAlreadyPrint.Add(kp.Key);
            }

            // consumer config property
            sb.AppendLine("\tConsumer property:");
            var consumersConfig = _consumerConfig
                                    .Except((i) => keysAlreadyPrint.Contains(i.Key))
                                    .Intercept((kp) => keysToNotDisplay.Contains(kp.Key), replaceValue);
            if (consumersConfig.Any())
            {
                foreach (var kp in consumersConfig)
                    sb.AppendLine($"\t\t{kp.Key}: \t{kp.Value}");
            }
            else
                sb.AppendLine($"\t\tNone");
            
            // override main consumer config property
            if (_overrideMainConsumerConfig.Any())
            {
                sb.AppendLine("\tOverride Main Consumer property:");
                var overrideMainConsumer = _overrideMainConsumerConfig
                    .Intercept((kp) => keysToNotDisplay.Contains(kp.Key), replaceValue);
                foreach (var kp in overrideMainConsumer)
                    sb.AppendLine($"\t\t{kp.Key}: \t{kp.Value}");
            }
            
            // override restore consumer config property
            if (_overrideRestoreConsumerConfig.Any())
            {
                sb.AppendLine("\tOverride Restore Consumer property:");
                var overrideRestoreConsumer = _overrideRestoreConsumerConfig
                    .Intercept((kp) => keysToNotDisplay.Contains(kp.Key), replaceValue);
                foreach (var kp in overrideRestoreConsumer)
                    sb.AppendLine($"\t\t{kp.Key}: \t{kp.Value}");
            }
            
            // override global consumer config property
            if (_overrideGlobalConsumerConfig.Any())
            {
                sb.AppendLine("\tOverride Global Consumer property:");
                var overrideGlobalConsumer = _overrideGlobalConsumerConfig
                    .Intercept((kp) => keysToNotDisplay.Contains(kp.Key), replaceValue);
                foreach (var kp in overrideGlobalConsumer)
                    sb.AppendLine($"\t\t{kp.Key}: \t{kp.Value}");
            }

            // override external consumer config property
            if (_overrideGlobalConsumerConfig.Any())
            {
                sb.AppendLine("\tOverride External Consumer property:");
                var overrideExtConsumer = _overrideExternalConsumerConfig
                    .Intercept((kp) => keysToNotDisplay.Contains(kp.Key), replaceValue);
                foreach (var kp in overrideExtConsumer)
                    sb.AppendLine($"\t\t{kp.Key}: \t{kp.Value}");
            }

            // producer config property
            sb.AppendLine("\tProducer property:");
            var producersConfig = _producerConfig
                                    .Except((i) => keysAlreadyPrint.Contains(i.Key))
                                    .Intercept((kp) => keysToNotDisplay.Contains(kp.Key), replaceValue);
            if (producersConfig.Any())
            {
                foreach (var kp in producersConfig)
                    sb.AppendLine($"\t\t{kp.Key}: \t{kp.Value}");
            }
            else
                sb.AppendLine($"\t\tNone");
            
            
            // override producer config property
            if (_overrideProducerConfig.Any())
            {
                sb.AppendLine("\tOverride Producer property:");
                var overrideProducer = _overrideProducerConfig
                    .Intercept((kp) => keysToNotDisplay.Contains(kp.Key), replaceValue);
                foreach (var kp in overrideProducer)
                    sb.AppendLine($"\t\t{kp.Key}: \t{kp.Value}");
            }

            // override external producer config property
            if (_overrideExternalProducerConfig.Any())
            {
                sb.AppendLine("\tOverride External Producer property:");
                var overrideExtProducer = _overrideExternalProducerConfig
                    .Intercept((kp) => keysToNotDisplay.Contains(kp.Key), replaceValue);
                foreach (var kp in overrideExtProducer)
                    sb.AppendLine($"\t\t{kp.Key}: \t{kp.Value}");
            }
            
            // admin config property
            sb.AppendLine("\tAdmin client property:");
            var adminsConfig = _adminClientConfig
                                    .Except((i) => keysAlreadyPrint.Contains(i.Key))
                                    .Intercept((kp) => keysToNotDisplay.Contains(kp.Key), replaceValue);
            if (adminsConfig.Any())
            {
                foreach (var kp in adminsConfig)
                    sb.AppendLine($"\t\t{kp.Key}: \t{kp.Value}");
            }
            else
                sb.AppendLine($"\t\tNone");

            return sb.ToString();
        }

        #endregion

        #region Logger Configuration

        /// <summary>
        /// Logger factory which will be used for logging
        /// </summary>
        public ILoggerFactory Logger { get; set; }

        #endregion
        
        #region Prefix

        /// <summary>
        /// Prefix the key with the main consumer prefix.
        /// Use this helper method if you want to override one specific configuration especially for the main consumer.
        /// </summary>
        /// <param name="key">Key configuration</param>
        /// <returns>the key for main consumer</returns>
        public string MainConsumerPrefix(string key)
            => $"{mainConsumerPrefix}{key}";
        
        /// <summary>
        /// Prefix the key with the global consumer prefix.
        /// Use this helper method if you want to override one specific configuration especially for the global consumer.
        /// </summary>
        /// <param name="key">Key configuration</param>
        /// <returns>the key for global consumer</returns>
        public string GlobalConsumerPrefix(string key)
            => $"{globalConsumerPrefix}{key}";
        
        /// <summary>
        /// Prefix the key with the restore consumer prefix.
        /// Use this helper method if you want to override one specific configuration especially for the restore consumer.
        /// </summary>
        /// <param name="key">Key configuration</param>
        /// <returns>the key for restore consumer</returns>
        public string RestoreConsumerPrefix(string key)
            => $"{restoreConsumerPrefix}{key}";

        /// <summary>
        /// Prefix the key with the external consumer prefix.
        /// Use this helper method if you want to override one specific configuration especially for the external consumer.
        /// </summary>
        /// <param name="key">Key configuration</param>
        /// <returns>the key for external consumer</returns>
        public string ExternalConsumerPrefix(string key)
            => $"{externalConsumerPrefix}{key}";
        
        /// <summary>
        /// Prefix the key with the main producer prefix.
        /// Use this helper method if you want to override one specific configuration especially for the main producer.
        /// </summary>
        /// <param name="key">Key configuration</param>
        /// <returns>the key for main producer</returns>
        public string ProducerPrefix(string key)
            => $"{producerPrefix}{key}";

        /// <summary>
        /// Prefix the key with the external producer prefix.
        /// Use this helper method if you want to override one specific configuration especially for the external producer.
        /// </summary>
        /// <param name="key">Key configuration</param>
        /// <returns>the key for external producer</returns>
        public string ExternalProducerPrefix(string key)
            => $"{externalProducerPrefix}{key}";

        #endregion
    }

    /// <summary>
    /// Implementation of <see cref="IStreamConfig"/>. Contains all configuration for your stream.
    /// By default, Kafka Streams does not allow users to overwrite the following properties (Streams setting shown in parentheses)
    ///    - EnableAutoCommit = (false) - Streams client will always disable/turn off auto committing
    /// If <see cref="IStreamConfig.Guarantee"/> is set to <see cref="ProcessingGuarantee.EXACTLY_ONCE"/>, Kafka Streams does not allow users to overwrite the following properties (Streams setting shown in parentheses):
    ///    - <see cref="IsolationLevel"/> (<see cref="IsolationLevel.ReadCommitted"/>) - Consumers will always read committed data only
    ///    - <see cref="StreamConfig.EnableIdempotence"/> (true) - Producer will always have idempotency enabled
    ///    - <see cref="StreamConfig.MaxInFlight"/> (5) - Producer will always have one in-flight request per connection
    /// If <see cref="IStreamConfig.Guarantee"/> is set to <see cref="ProcessingGuarantee.EXACTLY_ONCE"/>, Kafka Streams initialize the following properties :
    ///    - <see cref="StreamConfig.CommitIntervalMs"/> (<see cref="StreamConfig.EOS_DEFAULT_COMMIT_INTERVAL_MS"/>
    /// <exemple>
    /// <code>
    /// var config = new StreamConfig&lt;StringSerDes, StringSerDes&gt;();
    /// config.ApplicationId = "test-app";
    /// config.BootstrapServers = "localhost:9092";
    /// </code>
    /// </exemple>
    /// </summary>
    /// <typeparam name="KS">Default key serdes</typeparam>
    /// <typeparam name="VS">Default value serdes</typeparam>
    public class StreamConfig<KS, VS> : StreamConfig
        where KS : ISerDes, new()
        where VS : ISerDes, new()
    {
        /// <summary>
        /// Constructor empty
        /// </summary>
        public StreamConfig() : this(null) { }

        /// <summary>
        /// Constructor with properties. 
        /// <para>
        /// <see cref="IStreamConfig.DefaultKeySerDes"/> is set to <code>new KS();</code>
        /// <see cref="IStreamConfig.DefaultValueSerDes"/> is set to <code>new VS();</code>
        /// </para>
        /// </summary>
        /// <param name="properties">Dictionary of stream properties</param>
        public StreamConfig(IDictionary<string, dynamic> properties)
            : base(properties)
        {
            DefaultKeySerDes = new KS();
            DefaultValueSerDes = new VS();
        }
    }


    public static class StreamConfigExtensions
    {
        public static T Read<T>(this IStreamConfig config)
            where T : class
        {
            T t = Activator.CreateInstance<T>();
            
            foreach (var p in typeof(T).GetProperties())
            {
                var streamConfigAttr = p.GetCustomAttribute<StreamConfigPropertyAttribute>();
                if (streamConfigAttr != null && !streamConfigAttr.ReadOnly)
                {
                    var r = config.Get(streamConfigAttr.KeyName);
                    if (r != null)
                        p.SetValue(t, r);
                }
            }

            return t;
        }
    }
}