using System;
using Confluent.Kafka;
using Newtonsoft.Json;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Librdkafka;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Kafka
{
    /// <summary>
    /// Default builder used to provide Kafka clients builder
    /// </summary>
    public class DefaultKafkaClientBuilder
    {
        /// <summary>
        /// Get the consumer builder
        /// </summary>
        /// <param name="config"></param>
        /// <returns></returns>
        public virtual ConsumerBuilder<byte[], byte[]> GetConsumerBuilder(ConsumerConfig config)
            => new(config);
        
        /// <summary>
        /// Get the admin client builder
        /// </summary>
        /// <param name="config"></param>
        /// <returns></returns>
        public virtual AdminClientBuilder GetAdminBuilder(AdminClientConfig config)
            => new(config);

        /// <summary>
        /// Get the producer builder
        /// </summary>
        /// <param name="config"></param>
        /// <returns></returns>
        public virtual ProducerBuilder<byte[], byte[]> GetProducerBuilder(ProducerConfig config)
            => new(config);
    }
    
    /// <summary>
    /// Default <see cref="IKafkaSupplier"/> can be used to provide custom Kafka clients to a <see cref="KafkaStream"/> instance.
    /// </summary>
    public class DefaultKafkaClientSupplier : IKafkaSupplier
    {
        private readonly KafkaLoggerAdapter loggerAdapter;
        private readonly IStreamConfig streamConfig;
        private readonly bool exposeLibrdKafka;
        private readonly DefaultKafkaClientBuilder builderKafkaHandler;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="loggerAdapter"></param>
        /// <param name="streamConfig"></param>
        public DefaultKafkaClientSupplier(
            KafkaLoggerAdapter loggerAdapter,
            IStreamConfig streamConfig)
            : this(loggerAdapter, streamConfig, new DefaultKafkaClientBuilder())
        { }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="loggerAdapter"></param>
        /// <param name="streamConfig"></param>
        /// <param name="builderKafkaHandler"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public DefaultKafkaClientSupplier(
            KafkaLoggerAdapter loggerAdapter,
            IStreamConfig streamConfig,
            DefaultKafkaClientBuilder builderKafkaHandler)
        {
            this.loggerAdapter = loggerAdapter ?? throw new ArgumentNullException(nameof(loggerAdapter));
            this.streamConfig = streamConfig;
            exposeLibrdKafka = streamConfig?.ExposeLibrdKafkaStats ?? false;
            this.builderKafkaHandler = builderKafkaHandler ?? new DefaultKafkaClientBuilder();
        }

        /// <summary>
        /// Create an admin kafka client which is used for internal topic management.
        /// </summary>
        /// <param name="config">Admin configuration can't be null</param>
        /// <returns>Return an admin client instance</returns>
        public IAdminClient GetAdmin(AdminClientConfig config)
        {
            // No statistics handler is attached below, and an admin client is never polled. librdkafka
            // would still emit a statistics payload every StatisticsIntervalMs and enqueue it on the
            // client's event queue, where nothing dequeues it, so the queue grows for the lifetime of
            // the process. Statistics on this client are of no use to anyone: switch them off.
            config.StatisticsIntervalMs = 0;

            AdminClientBuilder builder = builderKafkaHandler.GetAdminBuilder(config);
            builder.SetLogHandler(loggerAdapter.LogAdmin);
            builder.SetErrorHandler(loggerAdapter.ErrorAdmin);
            return builder.Build();
        }

        /// <summary>
        /// Build a kafka consumer with <see cref="ConsumerConfig"/> instance and <see cref="IConsumerRebalanceListener"/> listener.
        /// </summary>
        /// <param name="config">Consumer configuration can't be null</param>
        /// <param name="rebalanceListener">Rebalance listener (Nullable)</param>
        /// <returns>Return a kafka consumer built</returns>
        public IConsumer<byte[], byte[]> GetConsumer(ConsumerConfig config, IConsumerRebalanceListener rebalanceListener)
        {
            ConsumerBuilder<byte[], byte[]> builder = builderKafkaHandler.GetConsumerBuilder(config);
            if (rebalanceListener != null)
            {
                builder.SetPartitionsAssignedHandler(rebalanceListener.PartitionsAssigned);
                builder.SetPartitionsRevokedHandler(rebalanceListener.PartitionsRevoked);
                builder.SetLogHandler(loggerAdapter.LogConsume);
                builder.SetErrorHandler(loggerAdapter.ErrorConsume);
            }
            
            if (exposeLibrdKafka)
            {
                // TODO : test librdkafka statistics with IntegrationTest (WIP see #82)
                var consumerStatisticsHandler = new ConsumerStatisticsHandler(
                    config.ClientId,
                    config is StreamizConsumerConfig streamizConsumerConfig  && streamizConsumerConfig.Config != null ?
                        streamizConsumerConfig.Config.ApplicationId :
                        streamConfig.ApplicationId, 
                    (config as StreamizConsumerConfig)?.ThreadId);
                consumerStatisticsHandler.Register(MetricsRegistry);
                builder.SetStatisticsHandler((c, stat) =>
                {
                    var statistics = JsonConvert.DeserializeObject<Statistics>(stat);
                    consumerStatisticsHandler.Publish(statistics);
                });
            }

            return builder.Build();
        }

        /// <summary>
        /// Build a kafka producer with <see cref="ProducerConfig"/> instance.
        /// </summary>
        /// <param name="config">Producer configuration can't be null</param>
        /// <returns>Return a kafka producer built</returns>
        public IProducer<byte[], byte[]> GetProducer(ProducerConfig config)
        {
            ProducerBuilder<byte[], byte[]> builder = builderKafkaHandler.GetProducerBuilder(config);
            builder.SetLogHandler(loggerAdapter.LogProduce);
            builder.SetErrorHandler(loggerAdapter.ErrorProduce);
            if (exposeLibrdKafka)
            {
                // TODO : test librdkafka statistics with IntegrationTest (WIP see #82)
                var producerStatisticsHandler = new ProducerStatisticsHandler(
                    config.ClientId,
                    config is StreamizProducerConfig streamizProducerConfig  && streamizProducerConfig.Config != null ?
                        streamizProducerConfig.Config.ApplicationId :
                        streamConfig.ApplicationId,
                    (config as StreamizProducerConfig)?.ThreadId,
                    (config as StreamizProducerConfig)?.Id?.ToString());
                producerStatisticsHandler.Register(MetricsRegistry);
                builder.SetStatisticsHandler((c, stat) =>
                {
                    var statistics = JsonConvert.DeserializeObject<Statistics>(stat);
                    producerStatisticsHandler.Publish(statistics);
                });
            }
            return builder.Build();
        }

        /// <summary>
        /// Build a kafka restore consumer with <see cref="ConsumerConfig"/> instance for read record to restore statestore.
        /// </summary>
        /// <param name="config">Restore consumer configuration can't be null</param>
        /// <returns>Return a kafka restore consumer built</returns>
        public IConsumer<byte[], byte[]> GetRestoreConsumer(ConsumerConfig config)
        {
            // Same reasoning as GetAdmin, with a sharper edge: this consumer has no statistics handler
            // either, and StoreChangelogReader stops polling it once every store has caught up. From
            // that point the client is idle for the lifetime of the process while librdkafka keeps
            // enqueuing statistics payloads that are never dequeued.
            config.StatisticsIntervalMs = 0;

            ConsumerBuilder<byte[], byte[]> builder = builderKafkaHandler.GetConsumerBuilder(config);
            builder.SetLogHandler(loggerAdapter.LogConsume);
            builder.SetErrorHandler(loggerAdapter.ErrorConsume);
            return builder.Build();
        }

        /// <summary>
        /// Build a kafka global consumer with <see cref="ConsumerConfig"/> which is used to consume records for <see cref="IGlobalKTable{K,V}"/>.
        /// </summary>
        /// <param name="config">Global consumer configuration can't be null</param>
        /// <returns>Return a kafka global consumer built</returns>
        public IConsumer<byte[], byte[]> GetGlobalConsumer(ConsumerConfig config)
        {
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            ConsumerBuilder<byte[], byte[]> builder = builderKafkaHandler.GetConsumerBuilder(config);
            builder.SetLogHandler(loggerAdapter.LogConsume);
            builder.SetErrorHandler(loggerAdapter.ErrorConsume);
            
            if (exposeLibrdKafka)
            {
                var consumerStatisticsHandler = new ConsumerStatisticsHandler(
                    config.ClientId,
                    config is StreamizConsumerConfig streamizConsumerConfig  && streamizConsumerConfig.Config != null ?
                        streamizConsumerConfig.Config.ApplicationId :
                        streamConfig.ApplicationId, 
                    (config as StreamizConsumerConfig)?.ThreadId, 
                    true);
                consumerStatisticsHandler.Register(MetricsRegistry);
                builder.SetStatisticsHandler((c, stat) =>
                {
                    var statistics = JsonConvert.DeserializeObject<Statistics>(stat);
                    consumerStatisticsHandler.Publish(statistics);
                });
            }
            
            return builder.Build();
        }
        
        /// <summary>
        /// Get or set the metrics registry.
        /// This registry will be capture all librdkafka statistics if <see cref="IStreamConfig.ExposeLibrdKafkaStats"/> is enable and forward these into the metrics reporter
        /// </summary>
        public StreamMetricsRegistry MetricsRegistry { get; set; }
    }
}