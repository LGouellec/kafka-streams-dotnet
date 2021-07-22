using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using System;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamReduceProcessor<K, V> : StatefullProcessor<K, V, K, V>
    {
        private readonly Reducer<V> reducer;

        public KStreamReduceProcessor(Reducer<V> reducer, string storeName, bool sendOldValues)
            : base(storeName, sendOldValues)
        {
            this.reducer = reducer;
        }

        public override void Process(K key, V value)
        {
            // If the key or value is null we don't need to proceed
            if (key == null || value == null)
            {
                log.LogWarning($"Skipping record due to null key or value. key=[{key}] value=[{value}] topic=[{Context.Topic}] partition=[{Context.Partition}] offset=[{Context.Offset}]");
                return;
            }

            ValueAndTimestamp<V> oldAggAndTimestamp = store.Get(key);
            V oldAgg, newAgg;
            long newTimestamp;

            if (oldAggAndTimestamp == null)
            {
                oldAgg = default;
                newAgg = value;
                newTimestamp = Context.Timestamp;
            }
            else
            {
                oldAgg = oldAggAndTimestamp.Value;
                newAgg = reducer.Apply(oldAgg, value);
                newTimestamp = Math.Max(Context.Timestamp, oldAggAndTimestamp.Timestamp);
            }

            store.Put(key, ValueAndTimestamp<V>.Make(newAgg, newTimestamp));
            tupleForwarder.MaybeForward(key, newAgg, sendOldValues ? oldAgg : default, newTimestamp);
        }
    }
}