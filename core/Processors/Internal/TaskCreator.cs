using Confluent.Kafka;
using Streamiz.Kafka.Net.Kafka;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class TaskCreator : AbstractTaskCreator<StreamTask>
    {
        private readonly InternalTopologyBuilder builder;
        private readonly IStreamConfig configuration;
        private readonly string threadId;
        private readonly IKafkaSupplier kafkaSupplier;
        private readonly IProducer<byte[], byte[]> producer;

        public TaskCreator(InternalTopologyBuilder builder, IStreamConfig configuration, string threadId, IKafkaSupplier kafkaSupplier, IProducer<byte[], byte[]> producer)
            : base()
        {
            this.builder = builder;
            this.configuration = configuration;
            this.threadId = threadId;
            this.kafkaSupplier = kafkaSupplier;
            this.producer = producer;
        }

        public override StreamTask CreateTask(IConsumer<byte[], byte[]> consumer, TaskId id, IEnumerable<TopicPartition> partitions)
        {
            log.LogDebug($"Created task {id} with assigned partition {string.Join(",", partitions)}");
            return new StreamTask(
                threadId,
                id,
                partitions,
                builder.BuildTopology(id.Id),
                consumer,
                configuration,
                kafkaSupplier,
                producer);
        }
    }
}