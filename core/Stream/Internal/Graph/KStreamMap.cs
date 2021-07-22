using Streamiz.Kafka.Net.Processors;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph
{
    internal class KStreamMap<K, V, K1, V1> : IProcessorSupplier<K, V>
    {
        public IKeyValueMapper<K, V, KeyValuePair<K1, V1>> Mapper { get; }

        public KStreamMap(IKeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper)
        {
            this.Mapper = mapper;
        }

        public IProcessor<K, V> Get() => new KStreamMapProcessor<K, V, K1, V1>(this.Mapper);
    }
}
