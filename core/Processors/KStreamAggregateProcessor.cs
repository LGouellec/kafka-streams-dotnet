using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using System;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamAggregateProcessor<K, V, T> : StatefullProcessor<K, V, K, T>
    {
        private readonly Initializer<T> initializer;
        private readonly Aggregator<K, V, T> aggregator;


        public KStreamAggregateProcessor(string storeName, bool enableSendOldValues, Initializer<T> initializer, Aggregator<K, V, T> aggregator)
            : base(storeName, enableSendOldValues)
        {
            this.initializer = initializer;
            this.aggregator = aggregator;
        }


        public override void Process(K key, V value)
        {
            if (key == null || value == null)
            {
                log.LogWarning($"Skipping record due to null key or value. key=[{key}] value=[{value}] topic=[{Context.Topic}] partition=[{Context.Partition}] offset=[{Context.Offset}]");
                return;
            }

            ValueAndTimestamp<T> oldAggAndTimestamp = store.Get(key);
            T oldAgg, newAgg;
            long newTimestamp;

            if (oldAggAndTimestamp == null)
            {
                oldAgg = initializer.Apply();
                newTimestamp = Context.Timestamp;
            }
            else
            {
                oldAgg = oldAggAndTimestamp.Value;
                newTimestamp = Math.Max(Context.Timestamp, oldAggAndTimestamp.Timestamp);
            }

            newAgg = aggregator.Apply(key, value, oldAgg);

            store.Put(key, ValueAndTimestamp<T>.Make(newAgg, newTimestamp));
            tupleForwarder.MaybeForward(key, newAgg, sendOldValues ? oldAgg : default, newTimestamp);
        }
    }
}