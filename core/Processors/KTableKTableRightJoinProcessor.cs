using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table.Internal;
using System;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KTableKTableRightJoinProcessor<K, V1, V2, VR> : AbstractKTableKTableJoinProcessor<K, V1, V2, VR>
    {

        public KTableKTableRightJoinProcessor(IKTableValueGetter<K, V2> iKTableValueGetter, IValueJoiner<V1, V2, VR> valueJoiner, bool sendOldValues)
            : base(iKTableValueGetter, valueJoiner, sendOldValues)
        {
        }

        public override void Process(K key, Change<V1> value)
        {
            if (key == null)
            {
                log.LogWarning($"{logPrefix}Skipping record due to null key. change=[{value}] topic=[{Context.Topic}] partition=[{Context.Partition}] offset=[{Context.Offset}]");
                return;
            }

            VR newValue, oldValue = default;
            long resultTs;
            var valueAndTimestampLeft = valueGetter.Get(key);
            if (valueAndTimestampLeft == null)
            {
                return;
            }

            resultTs = Math.Max(Context.Timestamp, valueAndTimestampLeft.Timestamp);
            newValue = joiner.Apply(value.NewValue, valueAndTimestampLeft.Value);

            if (sendOldValues)
            {
                oldValue = joiner.Apply(value.OldValue, valueAndTimestampLeft.Value);
            }

            Forward(key, new Change<VR>(oldValue, newValue), resultTs);
        }

     }
}
