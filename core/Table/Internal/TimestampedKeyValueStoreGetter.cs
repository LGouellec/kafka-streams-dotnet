using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Table.Internal
{
    internal class TimestampedKeyValueStoreGetter<K, V> : IKTableValueGetter<K, V>
    {
        private readonly string storeName;
        private ITimestampedKeyValueStore<K, V> store;

        public TimestampedKeyValueStoreGetter(string storeName)
        {
            this.storeName = storeName;
        }

        public void Close() { }

        public ValueAndTimestamp<V> Get(K key) 
            => store.Get(key);

        public void Init(ProcessorContext context) =>
            store = (ITimestampedKeyValueStore<K, V>)context.GetStateStore(storeName);
    }
}
