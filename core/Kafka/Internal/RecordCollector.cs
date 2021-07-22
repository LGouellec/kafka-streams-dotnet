using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.Kafka.Internal
{
    internal class RecordCollector : IRecordCollector
    {
        // IF EOS DISABLED, ONE PRODUCER BY TASK BUT ONE INSTANCE RECORD COLLECTOR BY TASK
        // WHEN CLOSING TASK, WE MUST DISPOSE PRODUCER WHEN NO MORE INSTANCE OF RECORD COLLECTOR IS PRESENT
        // IT'S A GARBAGE COLLECTOR LIKE
        private static IDictionary<string, int> instanceProducer = new Dictionary<string, int>();
        private static object _lock = new object();

        private IProducer<byte[], byte[]> producer;
        private readonly IStreamConfig configuration;
        private readonly TaskId id;

        private readonly string logPrefix;
        private readonly ILogger log = Logger.GetLogger(typeof(RecordCollector));


        public RecordCollector(string logPrefix, IStreamConfig configuration, TaskId id) 
        {
            this.logPrefix = $"{logPrefix}";
            this.configuration = configuration;
            this.id = id;
        }

        public void Init(ref IProducer<byte[], byte[]> producer)
        {
            this.producer = producer;

            string producerName = producer.Name.Split("#")[0];
            lock (_lock)
            {
                if (instanceProducer.ContainsKey(producerName))
                    ++instanceProducer[producerName];
                else
                    instanceProducer.Add(producerName, 1);
            }
        }

        public void Close()
        {
            log.LogDebug("{LogPrefix}Closing producer", logPrefix);
            if(producer != null)
            {
                lock (_lock)
                {
                    string producerName = producer.Name.Split("#")[0];
                    if (--instanceProducer[producerName] <= 0)
                    {
                        producer.Dispose();
                        producer = null;
                    }
                }
            }
        }

        public void Flush()
        {
            log.LogDebug("{LogPrefix}Flushing producer", logPrefix);
            if (producer != null)
            {
                try
                {
                    producer.Flush();
                }catch(ObjectDisposedException)
                {
                    // has been disposed
                }
            }
        }

        public void Send<K, V>(string topic, K key, V value, Headers headers, long timestamp, ISerDes<K> keySerializer, ISerDes<V> valueSerializer)
            => SendInternal(topic, key, value, headers, null, timestamp, keySerializer, valueSerializer);

        public void Send<K, V>(string topic, K key, V value, Headers headers, int partition, long timestamp, ISerDes<K> keySerializer, ISerDes<V> valueSerializer)
            => SendInternal(topic, key, value, headers, partition, timestamp, keySerializer, valueSerializer);

        private bool IsFatalError(DeliveryReport<byte[], byte[]> report)
        {
            return report.Error.IsFatal ||
                    report.Error.Code == ErrorCode.TopicAuthorizationFailed ||
                    report.Error.Code == ErrorCode.GroupAuthorizationFailed ||
                    report.Error.Code == ErrorCode.ClusterAuthorizationFailed ||
                    report.Error.Code == ErrorCode.UnsupportedSaslMechanism ||
                    report.Error.Code == ErrorCode.SecurityDisabled ||
                    report.Error.Code == ErrorCode.SaslAuthenticationFailed ||
                    report.Error.Code == ErrorCode.TopicException ||
                    report.Error.Code == ErrorCode.Local_KeySerialization ||
                    report.Error.Code == ErrorCode.Local_ValueSerialization ||
                    report.Error.Code == ErrorCode.OffsetMetadataTooLarge;
        }

        private bool IsRecoverableError(DeliveryReport<byte[], byte[]> report)
        {
            return IsRecoverableError(report.Error);
        }

        private bool IsRecoverableError(Error error)
        {
            return error.Code == ErrorCode.TransactionCoordinatorFenced ||
                     error.Code == ErrorCode.UnknownProducerId ||
                     error.Code == ErrorCode.OutOfOrderSequenceNumber;
        }

        private void SendInternal<K, V>(string topic, K key, V value, Headers headers, int? partition, long timestamp, ISerDes<K> keySerializer, ISerDes<V> valueSerializer)
        {
            var k = key != null ? keySerializer.Serialize(key, new SerializationContext(MessageComponentType.Key, topic, headers)) : null;
            var v = value != null ? valueSerializer.Serialize(value, new SerializationContext(MessageComponentType.Value, topic, headers)) : null;

            void HandleError(DeliveryReport<byte[], byte[]> report){
                if (report.Error.IsError)
                {
                    StringBuilder sb = new StringBuilder();
                    sb.AppendLine($"{logPrefix}Error encountered sending record to topic {topic} for task {id} due to:");
                    sb.AppendLine($"{logPrefix}Error Code : {report.Error.Code.ToString()}");
                    sb.AppendLine($"{logPrefix}Message : {report.Error.Reason}");

                    if (IsFatalError(report))
                    {
                        sb.AppendLine($"{logPrefix}Written offsets would not be recorded and no more records would be sent since this is a fatal error.");
                        log.LogError(sb.ToString());
                        throw new StreamsException(sb.ToString());
                    }
                    else if (IsRecoverableError(report))
                    {
                        sb.AppendLine($"{logPrefix}Written offsets would not be recorded and no more records would be sent since the producer is fenced, indicating the task may be migrated out");
                        log.LogError(sb.ToString());
                        throw new TaskMigratedException(sb.ToString());
                    }
                    else
                    {
                        if (configuration.ProductionExceptionHandler(report) == ExceptionHandlerResponse.FAIL)
                        {
                            sb.AppendLine($"{logPrefix}Exception handler choose to FAIL the processing, no more records would be sent.");
                            log.LogError(sb.ToString());
                            throw new ProductionException(sb.ToString());
                        }
                        else
                        {
                            sb.AppendLine($"{logPrefix}Exception handler choose to CONTINUE processing in spite of this error but written offsets would not be recorded.");
                            log.LogError(sb.ToString());
                        }
                    }
                }
                else if (report.Status == PersistenceStatus.NotPersisted || report.Status == PersistenceStatus.PossiblyPersisted)
                {
                    log.LogWarning("{logPrefix}Record not persisted or possibly persisted: (timestamp {Timestamp}) topic=[{Topic}] partition=[{Partition}] offset=[{Offset}]. May config Retry configuration, depends your use case",
                        logPrefix, report.Message.Timestamp.UnixTimestampMs, topic, report.Partition, report.Offset);
                }
                else if (report.Status == PersistenceStatus.Persisted)
                {
                    log.LogDebug("{LogPrefix}Record persisted: (timestamp {Timestamp}) topic=[{Topic}] partition=[{Partition}] offset=[{Offset}]",
                        logPrefix, report.Message.Timestamp.UnixTimestampMs, topic, report.Partition, report.Offset);
                }

            }

            try
            {
                if (partition.HasValue)
                {
                    producer?.Produce(
                        new TopicPartition(topic, partition.Value),
                        new Message<byte[], byte[]>
                        {
                            Key = k,
                            Value = v,
                            Headers = headers,
                            Timestamp = new Timestamp(timestamp, TimestampType.CreateTime)
                        },
                        (report) =>
                        {
                            HandleError(report);
                        });
                }
                else
                {
                    producer?.Produce(
                           topic,
                           new Message<byte[], byte[]>
                           {
                               Key = k,
                               Value = v,
                               Headers = headers,
                               Timestamp = new Timestamp(timestamp, TimestampType.CreateTime)
                           },
                           (report) =>
                           {
                               HandleError(report);
                           });
                }
            }
            catch (ProduceException<byte[], byte[]> produceException)
            {
                if (IsRecoverableError(produceException.Error))
                {
                    throw new TaskMigratedException($"Producer got fenced trying to send a record [{logPrefix}] : {produceException.Message}");
                }
                else
                {
                    throw new StreamsException($"Error encountered trying to send record to topic {topic} [{logPrefix}] : {produceException.Message}");
                }
            }
        }
    }
}