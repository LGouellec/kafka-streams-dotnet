using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table.Internal;
using System;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KTableKTableOuterJoinProcessor<K, V1, V2, VR> : AbstractKTableKTableJoinProcessor<K, V1, V2, VR>
    {
        public KTableKTableOuterJoinProcessor(IKTableValueGetter<K, V2> iKTableValueGetter, IValueJoiner<V1, V2, VR> valueJoiner, bool sendOldValues):
            base(iKTableValueGetter, valueJoiner, sendOldValues)
        {
        }

        public override void Process(K key, Change<V1> value)
        {
            if (key == null)
            {
                log.LogWarning($"{logPrefix}Skipping record due to null key. change=[{value}] topic=[{Context.Topic}] partition=[{Context.Partition}] offset=[{Context.Offset}]");
                return;
            }

            VR newValue = default, oldValue = default;
            var valueAndTimestamp2 = valueGetter.Get(key);
            long resultTs;
            if (valueAndTimestamp2 == null)
            {
                if (value.NewValue == null && value.OldValue == null)
                {
                    return;
                }

                resultTs = Context.Timestamp;
            }
            else
            {
                resultTs = Math.Max(Context.Timestamp, valueAndTimestamp2.Timestamp);
            }

            if (valueAndTimestamp2 != null || value.NewValue != null)
            {
                newValue = joiner.Apply(value.NewValue, valueAndTimestamp2 != null ? valueAndTimestamp2.Value : default);
            }

            if (sendOldValues && (valueAndTimestamp2 != null || value.OldValue != null))
            {
                oldValue = joiner.Apply(value.OldValue, valueAndTimestamp2 != null ? valueAndTimestamp2.Value : default);
            }

            Forward(key, new Change<VR>(oldValue, newValue), resultTs);
        }
    }
}