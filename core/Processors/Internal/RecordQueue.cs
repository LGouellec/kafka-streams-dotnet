using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class RecordQueue :
        IRecordQueue<ConsumeResult<byte[], byte[]>>,
        IComparable<RecordQueue>
    {
        // LOGGER + NAME
        private readonly string logPrefix;
        private readonly ILogger log = Logger.GetLogger(typeof(RecordQueue));

        private readonly List<ConsumeResult<byte[], byte[]>> queue;
        private ConsumeResult<byte[], byte[]> currentRecord = null;
        private long partitionTime = -1;

        private readonly ITimestampExtractor timestampExtractor;
        private readonly TopicPartition topicPartition;
        private readonly ISourceProcessor sourceProcessor;

        public RecordQueue(
            string logPrefix,
            string nameQueue,
            ITimestampExtractor timestampExtractor,
            TopicPartition topicPartition,
            ISourceProcessor sourceProcessor)
        {
            this.logPrefix = $"{logPrefix}- recordQueue [{nameQueue}] ";
            queue = new List<ConsumeResult<byte[], byte[]>>();

            this.timestampExtractor = timestampExtractor;
            this.topicPartition = topicPartition;
            this.sourceProcessor = sourceProcessor;
        }

        public long HeadRecordTimestamp
            => currentRecord == null ? -1 : currentRecord.Message.Timestamp.UnixTimestampMs;

        public ISourceProcessor Processor => sourceProcessor;

        #region IRecordQueue Impl

        public int Size => queue.Count + (currentRecord == null ? 0 : 1);

        public bool IsEmpty => Size == 0;

        public int Queue(ConsumeResult<byte[], byte[]> item)
        {
            log.LogDebug("{LogPrefix}Adding new record in queue", logPrefix);
            queue.Add(item);
            UpdateHeadRecord();
            log.LogDebug("{LogPrefix}Record added in queue. New size : {Size}", logPrefix, Size);
            return Size;
        }

        public ConsumeResult<byte[], byte[]> Poll()
        {
            log.LogDebug("{LogPrefix}Polling record in queue", logPrefix);
            var record = currentRecord;
            currentRecord = null;
            UpdateHeadRecord();
            log.LogDebug("{LogPrefix}{Record}record polled. ({RecordInfo})", logPrefix, record == null ? "No r" : "R",
                record != null
                    ? $"Record info [Topic:{record.Topic}|Partition:{record.Partition}|Offset:{record.Offset}]"
                    : "");
            return record;
        }

        public void Clear()
        {
            queue.Clear();
            currentRecord = null;
            partitionTime = -1;
            log.LogDebug("{LogPrefix} cleared !", logPrefix);
        }

        #endregion

        private void UpdateHeadRecord()
        {
            while (currentRecord == null && queue.Count > 0)
            {
                var record = queue[0];
                var recordObject = ToConsumeObject(record);
                long timestamp = -1;
                try
                {
                    timestamp = timestampExtractor.Extract(recordObject, partitionTime);
                }
                catch (Exception e)
                {
                    throw new StreamsException("Fatal error in Timestamp extractor callback", e);
                }

                log.LogDebug("");
                if (timestamp < 0)
                {
                    log.LogWarning(
                        "Skipping record due to negative extracted timestamp. topic=[{Topic}] partition=[{Partition}] offset=[{Offset}] extractedTimestamp=[{Timestamp}] extractor=[{TimestampExtractor}]",
                        record.Topic, record.Partition, record.Offset, timestamp, timestampExtractor.GetType().Name);
                    continue;
                }

                currentRecord = record;
                currentRecord.Message.Timestamp = new Timestamp(timestamp, currentRecord.Message.Timestamp.Type);
                partitionTime = Math.Max(partitionTime, timestamp);
                queue.RemoveAt(0);
            }
        }

        private ConsumeResult<object, object> ToConsumeObject(ConsumeResult<byte[], byte[]> record)
        {
            return new ConsumeResult<object, object>
            {
                Topic = record.Topic,
                IsPartitionEOF = record.IsPartitionEOF,
                Offset = record.Offset,
                TopicPartitionOffset = record.TopicPartitionOffset,
                Message = new Message<object, object>
                {
                    Headers = record.Message.Headers,
                    Timestamp = record.Message.Timestamp,
                    Key = sourceProcessor.DeserializeKey(record).Bean,
                    Value = sourceProcessor.DeserializeValue(record).Bean
                },
                Partition = record.Partition
            };
        }

        public int CompareTo(RecordQueue other)
            => HeadRecordTimestamp.CompareTo(other.HeadRecordTimestamp);
    }
}
