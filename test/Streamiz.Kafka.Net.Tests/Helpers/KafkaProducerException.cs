using Confluent.Kafka;
using Streamiz.Kafka.Net.Mock.Sync;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Streamiz.Kafka.Net.Tests.Helpers
{
    internal class ProducerSyncException : SyncKafkaSupplier
    {
        private KafkaProducerException producerException = null;

        public override IProducer<byte[], byte[]> GetProducer(ProducerConfig config)
        {
            if (producerException == null)
            {
                var p = base.GetProducer(config) as SyncProducer;
                producerException = new KafkaProducerException(p);
            }
            return producerException;
        }
    }

    internal class KafkaProducerException : IProducer<byte[], byte[]>
    {
        private SyncProducer innerProducer;

        public KafkaProducerException(SyncProducer syncProducer)
        {
            this.innerProducer = syncProducer;
        }

        public Handle Handle => throw new NotImplementedException();

        public string Name => "";

        public void AbortTransaction(TimeSpan timeout)
        {
        }

        public void AbortTransaction()
        {
            throw new NotImplementedException();
        }

        public int AddBrokers(string brokers)
        {
            return 0;
        }

        public void BeginTransaction()
        {
        }

        public void CommitTransaction(TimeSpan timeout)
        {
        }

        public void CommitTransaction()
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
        }

        public int Flush(TimeSpan timeout)
        {
            return 0;
        }

        public void Flush(CancellationToken cancellationToken = default)
        {
        }

        public void InitTransactions(TimeSpan timeout)
        {
            
        }

        public int Poll(TimeSpan timeout)
        {
            return 0;
        }

        public void Produce(string topic, Message<byte[], byte[]> message, Action<DeliveryReport<byte[], byte[]>> deliveryHandler = null)
        {
            if (topic == "test")
                innerProducer.Produce(topic, message, deliveryHandler);
            else
                deliveryHandler(new DeliveryReport<byte[], byte[]>()
                {
                    Error = new Error(ErrorCode.Local_InvalidArg, "Invalid arg", false)
                });
        }

        public void Produce(TopicPartition topicPartition, Message<byte[], byte[]> message, Action<DeliveryReport<byte[], byte[]>> deliveryHandler = null)
        {
            if (topicPartition.Topic == "test")
                innerProducer.Produce(topicPartition, message, deliveryHandler);
            else
                deliveryHandler(new DeliveryReport<byte[], byte[]>()
                {
                    Error = new Error(ErrorCode.Local_InvalidArg, "Invalid arg", false)
                });
        }

        public async Task<DeliveryResult<byte[], byte[]>> ProduceAsync(string topic, Message<byte[], byte[]> message, CancellationToken cancellationToken = default)
        {
            if (topic == "test")
                return await innerProducer.ProduceAsync(topic, message, cancellationToken);
            else
                throw new NotImplementedException();
        }

        public async Task<DeliveryResult<byte[], byte[]>> ProduceAsync(TopicPartition topicPartition, Message<byte[], byte[]> message, CancellationToken cancellationToken = default)
        {
            if (topicPartition.Topic == "test")
                return await innerProducer.ProduceAsync(topicPartition, message, cancellationToken);
            else
                throw new NotImplementedException();
        }

        public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }
    }
}