using Confluent.Kafka;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Streamiz.Kafka.Net.Mock.Sync
{
    internal class SyncConsumer : IConsumer<byte[], byte[]>, IConsumerGroupMetadata
    {
        internal class SyncConsumerOffset
        {
            public long OffsetCommitted { get; set; } = 0L;
            public long OffsetConsumed { get; set; } = 0L;

            public SyncConsumerOffset(long offset)
            {
                OffsetCommitted = offset;
                OffsetConsumed = offset;
            }
            public SyncConsumerOffset(long offsetCommit, long offsetConsumed)
            {
                OffsetCommitted = offsetCommit;
                OffsetConsumed = offsetConsumed;
            }

            public SyncConsumerOffset()
            {
            }
        }

        private ConsumerConfig config;
        private readonly SyncProducer producer;

        private readonly IDictionary<string, SyncConsumerOffset> offsets = new Dictionary<string, SyncConsumerOffset>();
        private readonly IDictionary<TopicPartition, bool> partitionsState = new Dictionary<TopicPartition, bool>();

        public IConsumerRebalanceListener Listener { get; private set; }

        public SyncConsumer(SyncProducer producer)
        {
            this.producer = producer;
        }

        public void UseConfig(ConsumerConfig config)
        {
            this.config = config;
        }

        #region IConsumer Impl

        public string MemberId => config.GroupId;

        public List<TopicPartition> Assignment { get; private set; } = new List<TopicPartition>();

        public List<string> Subscription { get; private set; } = new List<string>();

        public IConsumerGroupMetadata ConsumerGroupMetadata => this;

        public Handle Handle => null;

        public string Name => config.ClientId;

        public int AddBrokers(string brokers) => 0;

        public void Assign(TopicPartition partition)
        {
            if (!offsets.ContainsKey(partition.Topic))
            {
                offsets.Add(partition.Topic, new SyncConsumerOffset());
                Assignment.Add(partition);
            }
        }

        public void Assign(TopicPartitionOffset partition)
        {
            long offset = 0;
            if (partition.Offset.Value >= 0)
                offset = partition.Offset.Value;

            if (!offsets.ContainsKey(partition.Topic))
            {
                offsets.Add(partition.Topic, new SyncConsumerOffset(offset));
                Assignment.Add(partition.TopicPartition);
            }
            else
                offsets[partition.Topic] = new SyncConsumerOffset(offset);
        }

        public void Assign(IEnumerable<TopicPartitionOffset> partitions)
        {
            foreach (var p in partitions)
                Assign(p);
        }

        public void Assign(IEnumerable<TopicPartition> partitions)
        {
            foreach (var p in partitions)
                Assign(p);
        }

        public void Close()
        {
            Assignment.Clear();
            Subscription.Clear();
        }

        public List<TopicPartitionOffset> Commit()
        {
            foreach (var kp in offsets)
            {
                var o = producer.GetHistory(kp.Key).Count() + 1;
                offsets[kp.Key] = new SyncConsumerOffset(o);
            }
            return offsets.Select(k => new TopicPartitionOffset(new TopicPartition(k.Key, 0), k.Value.OffsetCommitted)).ToList();
        }

        public void Commit(IEnumerable<TopicPartitionOffset> offsets)
        {
            foreach (var offset in offsets)
            {
                if (this.offsets.ContainsKey(offset.Topic))
                {
                    this.offsets[offset.Topic] = new SyncConsumerOffset(offset.Offset);
                }
                else
                {
                    this.offsets.Add(offset.Topic, new SyncConsumerOffset(offset.Offset));
                }
            }

        }

        public void Commit(ConsumeResult<byte[], byte[]> result)
            => Commit(new List<TopicPartitionOffset> { new TopicPartitionOffset(result.TopicPartition, result.Offset + 1) });

        public List<TopicPartitionOffset> Committed(TimeSpan timeout)
        {
            List<TopicPartitionOffset> r = new List<TopicPartitionOffset>();
            foreach (var kp in offsets)
                r.Add(new TopicPartitionOffset(new TopicPartition(kp.Key, 0), kp.Value.OffsetCommitted));
            return r;
        }

        public List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout)
        {
            List<TopicPartitionOffset> r = new List<TopicPartitionOffset>();
            foreach (var kp in offsets)
                if(partitions.Select(t => t.Topic).Distinct().Contains(kp.Key))
                    r.Add(new TopicPartitionOffset(new TopicPartition(kp.Key, 0), kp.Value.OffsetCommitted));
            return r;
        }

        public void Dispose()
        {
            Close();
        }

        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
        {
            return new WatermarkOffsets(0L, producer.GetHistory(topicPartition.Topic).Count());
        }

        public List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout)
        {
            // TODO : 
            throw new NotImplementedException();
        }

        public void Pause(IEnumerable<TopicPartition> partitions)
        {
            foreach (var p in partitions)
            {
                if (partitionsState.ContainsKey(p))
                    partitionsState[p] = true;
                else
                    partitionsState.Add(p, true);
            }
        }

        public Offset Position(TopicPartition partition)
        {
            // TODO
            throw new NotImplementedException();
        }

        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout)
        {
            // TODO
            throw new NotImplementedException();
        }

        public void Resume(IEnumerable<TopicPartition> partitions)
        {
            foreach (var p in partitions)
            {
                if (partitionsState.ContainsKey(p))
                    partitionsState[p] = false;
                else
                    partitionsState.Add(p, false);
            }
        }

        public void Seek(TopicPartitionOffset tpo)
        {
            // TODO
            throw new NotImplementedException();
        }

        public void StoreOffset(TopicPartitionOffset offset)
        {
            // TODO
            throw new NotImplementedException();
        }

        public void StoreOffset(ConsumeResult<byte[], byte[]> result)
        {
            // TODO
            throw new NotImplementedException();
        }

        public void Subscribe(IEnumerable<string> topics)
        {
            Subscription.AddRange(topics);
            foreach (var t in topics)
            {
                if (!offsets.ContainsKey(t))
                {
                    offsets.Add(t, new SyncConsumerOffset(0L));
                    Assignment.Add(new TopicPartition(t, 0));
                }
            }
            Listener?.PartitionsAssigned(this, Assignment);
        }

        public void Subscribe(string topic)
        {
            Subscription.Add(topic);
            if (!offsets.ContainsKey(topic))
            {
                offsets.Add(topic, new SyncConsumerOffset(0L));
                Assignment.Add(new TopicPartition(topic, 0));
            }
            Listener?.PartitionsAssigned(this, Assignment);
        }

        public void Unassign()
        {
            var offsets = this.Committed(TimeSpan.FromSeconds(1));
            Assignment.Clear();
            Listener?.PartitionsRevoked(this, offsets);
            this.offsets.Clear();
        }

        public void Unsubscribe()
        {
            this.Unassign();
            Subscription.Clear();
        }

        public ConsumeResult<byte[], byte[]> Consume(int millisecondsTimeout)
            => Consume(TimeSpan.FromMilliseconds(millisecondsTimeout));

        public ConsumeResult<byte[], byte[]> Consume(CancellationToken cancellationToken = default)
        {
            if (Subscription.Count == 0)
                throw new StreamsException("No subscription have been done !");

            return ConsumeInternal(TimeSpan.FromSeconds(10));
        }

        public ConsumeResult<byte[], byte[]> Consume(TimeSpan timeout)
        {
            if (Subscription.Count == 0 && Assignment.Count == 0)
                throw new StreamsException("No subscription have been done !");

            return ConsumeInternal(timeout);
        }

        private ConsumeResult<byte[], byte[]> ConsumeInternal(TimeSpan timeout)
        {
            DateTime dt = DateTime.Now;
            ConsumeResult<byte[], byte[]> result = null;

            foreach (var kp in offsets)
            {
                if (timeout != TimeSpan.Zero && (dt + timeout) < DateTime.Now)
                    break;

                var tp = new TopicPartition(kp.Key, 0);
                if (producer != null && 
                    ((partitionsState.ContainsKey(tp) && !partitionsState[tp]) || 
                    !partitionsState.ContainsKey(tp)))
                {
                    var messages = producer.GetHistory(kp.Key).ToArray();
                    if (messages.Length > kp.Value.OffsetConsumed)
                    {
                        result = new ConsumeResult<byte[], byte[]>
                        {
                            Offset = kp.Value.OffsetConsumed,
                            Topic = kp.Key,
                            Partition = 0,
                            Message = messages[kp.Value.OffsetConsumed]
                        };
                        ++kp.Value.OffsetConsumed;
                        return result;
                    }
                }
            }
            return result;
        }

        #endregion

        internal void SetRebalanceListener(IConsumerRebalanceListener rebalanceListener)
        {
            Listener = rebalanceListener;
        }

        public void IncrementalAssign(IEnumerable<TopicPartitionOffset> partitions)
        {
            throw new NotImplementedException();
        }

        public void IncrementalAssign(IEnumerable<TopicPartition> partitions)
        {
            throw new NotImplementedException();
        }

        public void IncrementalUnassign(IEnumerable<TopicPartition> partitions)
        {
            throw new NotImplementedException();
        }
    }
}
