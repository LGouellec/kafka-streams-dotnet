using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamKTableJoinProcessor<K1, K2, V1, V2, R> : AbstractProcessor<K1, V1>
    {
        private readonly IKTableValueGetter<K2, V2> valueGetter;
        private readonly IKeyValueMapper<K1, V1, K2> mapper;
        private readonly IValueJoiner<V1, V2, R> joiner;
        private readonly bool leftJoin;

        public KStreamKTableJoinProcessor(IKTableValueGetter<K2, V2> valueGetter, IKeyValueMapper<K1, V1, K2> keyValueMapper, IValueJoiner<V1, V2, R> valueJoiner, bool leftJoin)
        {
            this.valueGetter = valueGetter;
            mapper = keyValueMapper;
            joiner = valueJoiner;
            this.leftJoin = leftJoin;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);
            valueGetter.Init(context);
        }

        public override void Process(K1 key, V1 value)
        {
            // If the key or value is null we don't need to proceed
            if (key == null || value == null)
            {
                log.LogWarning($"Skipping record due to null key or value. key=[{key}] value=[{value}] topic=[{Context.Topic}] partition=[{Context.Partition}] offset=[{Context.Offset}]");
                return;
            }
            else
            {
                K2 mappedKey = mapper.Apply(key, value);
                V2 value2 = mappedKey == null ? default : ValueAndTimestamp.GetValueOrNull(valueGetter.Get(mappedKey));
                if (leftJoin || value2 != null)
                    Forward(key, joiner.Apply(value, value2));
            }
        }
    }
}