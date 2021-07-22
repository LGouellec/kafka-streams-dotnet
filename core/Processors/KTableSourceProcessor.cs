using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KTableSourceProcessor<K, V> : AbstractProcessor<K, V>
    {
        private readonly string storeName;
        private readonly string queryableName;
        private readonly bool sendOldValues;

        private ITimestampedKeyValueStore<K, V> store;
        private TimestampedTupleForwarder<K, V> tupleForwarder;

        public KTableSourceProcessor(string storeName, string queryableName, bool sendOldValues)
        {
            this.storeName = storeName;
            this.queryableName = queryableName;
            this.sendOldValues = sendOldValues;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);

            if (this.queryableName != null)
            {
                store = (ITimestampedKeyValueStore<K, V>)context.GetStateStore(queryableName);
                tupleForwarder = new TimestampedTupleForwarder<K, V>(this, sendOldValues);
            }
        }

        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);
            if (key == null)
            {
                log.LogWarning($"Skipping record due to null key. topic=[{Context.Topic}] partition=[{Context.Partition}] offset=[{Context.Offset}]");
                return;
            }

            if (queryableName != null)
            {
                 ValueAndTimestamp<V> oldValueAndTimestamp = store.Get(key);
                 V oldValue;
                if (oldValueAndTimestamp != null)
                {
                    oldValue = oldValueAndTimestamp.Value;
                    if (Context.Timestamp < oldValueAndTimestamp.Timestamp)
                    {
                        log.LogWarning($"Detected out-of-order KTable update for {store.Name} at offset {Context.Offset}, partition {Context.Partition}.");
                    }
                }
                else
                {
                    oldValue = default(V);
                }
                store.Put(key, ValueAndTimestamp<V>.Make(value, Context.Timestamp));
                tupleForwarder.MaybeForward(key, value, oldValue);
            }
            else
            {
                this.Forward<K, Change<V>>(key, new Change<V>(default(V), value));
            }
        }
    }
}
