using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Cache;
using Streamiz.Kafka.Net.State.Logging;
using Streamiz.Kafka.Net.State.Metered;
using Streamiz.Kafka.Net.State.Supplier;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// Key/Value store builder
    /// </summary>
    /// <typeparam name="K"></typeparam>
    /// <typeparam name="V"></typeparam>
    public class KeyValueStoreBuilder<K, V>
        : AbstractStoreBuilder<K, V, IKeyValueStore<K, V>>
    {
        private readonly IKeyValueBytesStoreSupplier supplier;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="supplier"></param>
        /// <param name="keySerde"></param>
        /// <param name="valueSerde"></param>
        public KeyValueStoreBuilder(IKeyValueBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde) 
            : base(supplier.Name, keySerde, valueSerde)
        {
            this.supplier = supplier;
        }

        /// <summary>
        /// False every time
        /// </summary>
        public override bool IsWindowStore => false;
        
        /// <summary>
        /// Not supported
        /// </summary>
        /// <exception cref="IllegalStateException"></exception>
        public override long RetentionMs => throw new IllegalStateException("RetentionMs is not supported when not a window store");
        
        /// <summary>
        /// Build the state store
        /// </summary>
        /// <returns></returns>
        public override IKeyValueStore<K, V> Build()
        {
            var store = supplier.Get();

            return new MeteredKeyValueStore<K, V>(
                WrapCaching(WrapLogging(store)),
                keySerdes,
                valueSerdes,
                supplier.MetricsScope);
        }
        
        private IKeyValueStore<Bytes, byte[]> WrapLogging(IKeyValueStore<Bytes, byte[]> inner)
        {
            return !LoggingEnabled || !inner.IsLocally ? 
                inner : new ChangeLoggingKeyValueBytesStore(inner);
        }

        private IKeyValueStore<Bytes, byte[]> WrapCaching(IKeyValueStore<Bytes, byte[]> inner)
        {
            return !CachingEnabled ? inner : new CachingKeyValueStore(inner);
        }
    }
}