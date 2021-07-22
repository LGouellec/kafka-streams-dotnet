using Streamiz.Kafka.Net.Processors;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph
{
    internal class KStreamMapValues<K, V, V1> : IProcessorSupplier<K, V>
    {
        private readonly IValueMapperWithKey<K, V, V1> mapper;

        public KStreamMapValues(IValueMapperWithKey<K, V, V1> mapper)
        {
            this.mapper = mapper;
        }

        public IProcessor<K, V> Get() => new KStreamMapProcessor<K, V, K, V1>(
            new WrappedKeyValueMapper<K, V, KeyValuePair<K, V1>>(
                (key, value) => new KeyValuePair<K, V1>(key, this.mapper.Apply(key, value))));
    }
}
