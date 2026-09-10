using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Kafka.Internal
{
    internal class RecordCollector : IRecordCollector
    {
        private sealed class RetryRecord
        {
            public byte[] Key { get; set; }
            public byte[] Value { get; set; }
            public Headers Headers { get; set; }
            public Timestamp Timestamp { get; set; }
            public Partition Partition { get; set; }
            public string Topic { get; set; }
        }

        private sealed class RetryRecordContext
        {
            private readonly Dictionary<string, RetryRecord> records = new();
            private readonly Queue<RetryRecord> queueRecords = new();
            private const string RETRY_HEADER_KEY = "streamiz-retry-guid";

            public void AddRecord(RetryRecord record)
            {
                record.Headers ??= new Headers();
                
                if (record.Headers.TryGetLastBytes(RETRY_HEADER_KEY, out byte[] previousGuid))
                {
                    string oldGuidKey = Encoding.UTF8.GetString(previousGuid);
                    record.Headers.Remove(oldGuidKey);
                    records.Remove(oldGuidKey);
                }

                var newKey = Guid.NewGuid().ToString();
                record.Headers.AddOrUpdate(RETRY_HEADER_KEY, Encoding.UTF8.GetBytes(newKey));
                records.Add(newKey, record);
                queueRecords.Enqueue(record);
            }

            public void AckRecord(DeliveryReport<byte[], byte[]> report)
            {
                if (report.Headers != null &&
                    report.Headers.TryGetLastBytes(RETRY_HEADER_KEY, out var retryHeader))
                    records.Remove(Encoding.UTF8.GetString(retryHeader));
            }

            public RetryRecord NextRecord()
                => queueRecords.Count > 0 ? queueRecords.Dequeue() : null;

            public bool HasNext => queueRecords.Count > 0;

            public void Clear()
            {
                records.Clear();
                queueRecords.Clear();
            }
        }

        private StreamsProducer _producer;
        private readonly IStreamConfig _configuration;
        private readonly TaskId _id;
        private readonly Sensor _droppedRecordsSensor;
        private Exception _exception;

        private readonly string _logPrefix;
        private readonly ILogger log = Logger.GetLogger(typeof(RecordCollector));

        private readonly ConcurrentDictionary<TopicPartition, long> _collectorsOffsets = new();

        private readonly RetryRecordContext _retryRecordContext = new();

        public IDictionary<TopicPartition, long> CollectorOffsets => _collectorsOffsets.ToDictionary();

        public RecordCollector(string logPrefix, IStreamConfig configuration, TaskId id, StreamsProducer producer,
            Sensor droppedRecordsSensor)
        {
            _logPrefix = $"{logPrefix}";
            _configuration = configuration;
            _id = id;
            _droppedRecordsSensor = droppedRecordsSensor;
            _producer = producer;
        }

        public void Initialize()
        {
            if (_producer.EosEnabled)
                _producer.InitTransaction();
        }

        public void Close(bool dirty)
        {
            log.LogDebug($"{_logPrefix}Closing producer");

            if (_retryRecordContext.HasNext)
                log.LogWarning(
                    "There are messages still pending to retry in the backpressure queue. These messages won't longer flush into the corresponding topic !");

            _retryRecordContext.Clear();

            if (dirty)
            {
                if (_producer.EosEnabled)
                    _producer.AbortTransaction();
            }

            _collectorsOffsets.Clear();
            CheckForException();
        }

        public void Flush()
        {
            log.LogDebug("{LogPrefix}Flushing producer", _logPrefix);
            if (_producer != null)
            {
                while (_retryRecordContext.HasNext)
                    ProduceRetryRecord();

                _producer.Flush();
                CheckForException();
            }
        }

        public void Send<K, V>(string topic, K key, V value, Headers headers, long timestamp, ISerDes<K> keySerializer,
            ISerDes<V> valueSerializer)
            => SendInternal(topic, key, value, headers, null, timestamp, keySerializer, valueSerializer);

        public void Send<K, V>(string topic, K key, V value, Headers headers, int partition, long timestamp,
            ISerDes<K> keySerializer, ISerDes<V> valueSerializer)
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
            return StreamsProducer.IsRecoverable(report.Error);
        }

        private void SendInternal<K, V>(string topic, K key, V value, Headers headers, int? partition, long timestamp,
            ISerDes<K> keySerializer, ISerDes<V> valueSerializer)
        {
            CheckForException();
            
            Debug.Assert(_producer != null, nameof(_producer) + " != null");
            var k = key != null
                ? keySerializer.Serialize(key, new SerializationContext(MessageComponentType.Key, topic, headers))
                : null;
            var v = value != null
                ? valueSerializer.Serialize(value, new SerializationContext(MessageComponentType.Value, topic, headers))
                : null;
            
            while (_retryRecordContext.HasNext)
                ProduceRetryRecord();

            try
            {
                if (partition.HasValue)
                {
                    _producer.Produce(
                        new TopicPartition(topic, partition.Value),
                        new Message<byte[], byte[]>
                        {
                            Key = k,
                            Value = v,
                            Headers = headers,
                            Timestamp = new Timestamp(timestamp, TimestampType.CreateTime)
                        },
                        HandleError);
                }
                else
                {
                    _producer.Produce(
                        topic,
                        new Message<byte[], byte[]>
                        {
                            Key = k,
                            Value = v,
                            Headers = headers,
                            Timestamp = new Timestamp(timestamp, TimestampType.CreateTime)
                        },
                        HandleError);
                }
            }
            catch (ProduceException<byte[], byte[]> produceException)
            {
                if (StreamsProducer.IsRecoverable(produceException.Error))
                    throw new TaskMigratedException(
                        $"Producer got fenced trying to send a record: {produceException.Message}");

                ManageProduceException(produceException);
            }
        }

        private void HandleError(DeliveryReport<byte[], byte[]> report)
        {
            if (report.Error.IsError)
            {
                StringBuilder sb = new StringBuilder();
                sb.AppendLine(
                    $"{_logPrefix}Error encountered sending record to topic {report.Topic} for task {_id} due to:");
                sb.AppendLine($"{_logPrefix}Error Code : {report.Error.Code.ToString()}");
                sb.AppendLine($"{_logPrefix}Message : {report.Error.Reason}");

                if (IsFatalError(report))
                {
                    sb.AppendLine(
                        $"{_logPrefix}Written offsets would not be recorded and no more records would be sent since this is a fatal error.");
                    log.LogError(sb.ToString());
                    _exception = new StreamsException(sb.ToString());
                }
                else if (IsRecoverableError(report))
                {
                    sb.AppendLine(
                        $"{_logPrefix}Written offsets would not be recorded and no more records would be sent since the producer is fenced, indicating the task may be migrated out");
                    log.LogError(sb.ToString());
                    _exception = new TaskMigratedException(sb.ToString());
                }
                else
                {
                    var exceptionResponse = _configuration.ProductionExceptionHandler(report);
                    if (exceptionResponse == ProductionExceptionHandlerResponse.FAIL)
                    {
                        sb.AppendLine(
                            $"{_logPrefix}Exception handler choose to FAIL the processing, no more records would be sent.");
                        log.LogError(sb.ToString());
                        _exception = new ProductionException(sb.ToString());
                    }
                    else if (exceptionResponse == ProductionExceptionHandlerResponse.RETRY)
                    {
                        sb.AppendLine(
                            $"{_logPrefix}Exception handler choose to RETRY sending the message to the next iteration");
                        log.LogWarning(sb.ToString());
                        var retryRecord = new RetryRecord
                        {
                            Key = report.Key,
                            Value = report.Value,
                            Headers = report.Headers,
                            // DeliveryReport.Timestamp is often LogAppendTime/NotAvailable after a
                            // failed produce; Confluent.Kafka only accepts Default/CreateTime on produce.
                            Timestamp = new Timestamp(report.Timestamp.UnixTimestampMs, TimestampType.CreateTime),
                            Partition = report.Partition,
                            Topic = report.Topic
                        };
                        _retryRecordContext.AddRecord(retryRecord);
                    }
                    else
                    {
                        sb.AppendLine(
                            $"{_logPrefix}Exception handler choose to CONTINUE processing in spite of this error but written offsets would not be recorded.");
                        log.LogError(sb.ToString());
                        _droppedRecordsSensor.Record();
                    }
                }
            }
            else if (report.Status == PersistenceStatus.NotPersisted ||
                     report.Status == PersistenceStatus.PossiblyPersisted)
            {
                log.LogWarning(
                    "{logPrefix}Record not persisted or possibly persisted: (timestamp {Timestamp}) topic=[{Topic}] partition=[{Partition}] offset=[{Offset}]. May config Retry configuration, depends your use case",
                    _logPrefix, report.Message.Timestamp.UnixTimestampMs, report.Topic, report.Partition,
                    report.Offset);
            }
            else if (report.Status == PersistenceStatus.Persisted)
            {
                log.LogDebug(
                    "{LogPrefix}Record persisted: (timestamp {Timestamp}) topic=[{Topic}] partition=[{Partition}] offset=[{Offset}]",
                    _logPrefix, report.Message.Timestamp.UnixTimestampMs, report.Topic, report.Partition,
                    report.Offset);

                if(_collectorsOffsets.TryGetValue(report.TopicPartition, out var offset) && offset < report.Offset.Value)
                    _collectorsOffsets.TryUpdate(report.TopicPartition, report.Offset.Value, offset);
                else
                    _collectorsOffsets.TryAdd(report.TopicPartition, report.Offset);
                _retryRecordContext.AckRecord(report);
            }
        }

        private void ProduceRetryRecord()
        {
            var retryRecord = _retryRecordContext.NextRecord();
            if (retryRecord != null)
            {
                try
                {
                    _producer.Produce(
                        new TopicPartition(retryRecord.Topic, retryRecord.Partition),
                        new Message<byte[], byte[]>
                        {
                            Key = retryRecord.Key,
                            Value = retryRecord.Value,
                            Headers = retryRecord.Headers,
                            Timestamp = new Timestamp(
                                retryRecord.Timestamp.UnixTimestampMs,
                                TimestampType.CreateTime)
                        }, HandleError);
                }
                catch (ProduceException<byte[], byte[]> produceException)
                {
                    if (StreamsProducer.IsRecoverable(produceException.Error))
                        throw new TaskMigratedException(
                            $"Producer got fenced trying to send a record: {produceException.Message}");

                    ManageProduceException(produceException);
                }
            }
        }

        private void ManageProduceException(ProduceException<byte[], byte[]> produceException)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine(
                $"{_logPrefix}Error encountered sending record to topic {produceException.DeliveryResult.Topic} for task {_id} due to:");
            sb.AppendLine($"{_logPrefix}Error Code : {produceException.Error.Code.ToString()}");
            sb.AppendLine($"{_logPrefix}Message : {produceException.Error.Reason}");

            var buildDeliveryReport =
                new DeliveryReport<byte[], byte[]>
                {
                    Message = produceException.DeliveryResult.Message,
                    Status = produceException.DeliveryResult.Status,
                    TopicPartitionOffsetError = new TopicPartitionOffsetError(
                        produceException.DeliveryResult.TopicPartitionOffset,
                        produceException.Error)
                };

            var exceptionHandlerResponse = _configuration.ProductionExceptionHandler(buildDeliveryReport);

            if (exceptionHandlerResponse == ProductionExceptionHandlerResponse.FAIL)
            {
                sb.AppendLine(
                    $"{_logPrefix}Exception handler choose to FAIL the processing, no more records would be sent.");
                log.LogError(sb.ToString());
                throw new StreamsException(
                    $"Error encountered trying to send record to topic {produceException.DeliveryResult.Topic} [{_logPrefix}] : {produceException.Message}");
            }

            if (exceptionHandlerResponse == ProductionExceptionHandlerResponse.RETRY)
            {
                sb.AppendLine(
                    $"{_logPrefix}Exception handler choose to RETRY sending the message to the next iteration");
                log.LogWarning(sb.ToString());
                var retryRecord = new RetryRecord
                {
                    Key = buildDeliveryReport.Key,
                    Value = buildDeliveryReport.Value,
                    Headers = buildDeliveryReport.Headers,
                    Timestamp = new Timestamp(
                        buildDeliveryReport.Timestamp.UnixTimestampMs,
                        TimestampType.CreateTime),
                    Partition = buildDeliveryReport.Partition,
                    Topic = buildDeliveryReport.Topic
                };
                _retryRecordContext.AddRecord(retryRecord);
            }
            else if (exceptionHandlerResponse == ProductionExceptionHandlerResponse.CONTINUE)
            {
                sb.AppendLine(
                    $"{_logPrefix}Exception handler choose to CONTINUE processing in spite of this error but written offsets would not be recorded.");
                log.LogError(sb.ToString());
            }
        }

        private void CheckForException()
        {
            if (_exception == null) return;
            var e = _exception;
            _exception = null;
            throw e;
        }

        public int PartitionsFor(string topic)
            => _producer.PartitionsFor(topic);
    }
}