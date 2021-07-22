using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph
{
    // TODO : keyMapper
    internal class KStreamGlobalKTableJoin<K1, K2, V1, V2, VR> : IProcessorSupplier<K1, V1>
    {
        private readonly IKTableValueGetterSupplier<K2, V2> supplier;
        private readonly IValueJoiner<V1, V2, VR> valueJoiner;
        private readonly IKeyValueMapper<K1, V1, K2> keyMapper;
        private readonly bool leftJoin;

        public KStreamGlobalKTableJoin(IKTableValueGetterSupplier<K2, V2> supplier, IValueJoiner<V1, V2, VR> valueJoiner, IKeyValueMapper<K1, V1, K2> keyMapper, bool leftJoin)
        {
            this.supplier = supplier;
            this.valueJoiner = valueJoiner;
            this.leftJoin = leftJoin;
            this.keyMapper = keyMapper;
        }

        public IProcessor<K1, V1> Get()
            => new KStreamKTableJoinProcessor<K1, K2, V1, V2, VR>(
                supplier.Get(),
                keyMapper,
                valueJoiner,
                leftJoin);
    }
}
