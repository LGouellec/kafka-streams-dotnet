using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Table.Internal;
using System;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph
{
    internal class KStreamAggregate<K, V, T> : IKStreamAggProcessorSupplier<K, K, V, T>
    {
        private readonly string storeName;
        private readonly Initializer<T> initializer;
        private readonly Aggregator<K, V, T> aggregator;

        private bool sendOldValues = false;

        public KStreamAggregate(string storeName, Func<T> initializer, Func<K, V, T, T> aggregator)
            : this(storeName, new WrappedInitializer<T>(initializer), new WrappedAggregator<K, V, T>(aggregator))
        {}

        public KStreamAggregate(string storeName, Initializer<T> initializer, Aggregator<K, V, T> aggregator)
        {
            this.storeName = storeName;
            this.initializer = initializer;
            this.aggregator = aggregator;
        }

        public void EnableSendingOldValues()
        {
            sendOldValues = true;
        }

        public IProcessor<K, V> Get() => new KStreamAggregateProcessor<K, V, T>(storeName, sendOldValues, initializer, aggregator);

        public IKTableValueGetterSupplier<K, T> View()
            => new GenericKTableValueGetterSupplier<K, T>(
                new string[] { storeName },
                new TimestampedKeyValueStoreGetter<K, T>(storeName));
    }
}
