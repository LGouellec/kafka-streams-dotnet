using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table.Internal;
using System;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KTableKTableLeftJoinProcessor<K, V1, V2, VR> : AbstractKTableKTableJoinProcessor<K, V1, V2, VR>
    {
        public KTableKTableLeftJoinProcessor(IKTableValueGetter<K, V2> valueGetter, IValueJoiner<V1, V2, VR> valueJoiner, bool sendOldValues)
            : base(valueGetter, valueJoiner, sendOldValues)
        {
        }

        public override void Process(K key, Change<V1> value)
        {
            if (key == null)
            {
                log.LogWarning($"{logPrefix}Skipping record due to null key. change=[{value}] topic=[{Context.Topic}] partition=[{Context.Partition}] offset=[{Context.Offset}]");
                return;
            }

            VR newValue = default;
            VR oldValue = default;
            var valueAndTsRight = valueGetter.Get(key);
            long timestampRight, resultTimestamp;

            if (valueAndTsRight == null)
            {
                if (value.NewValue == null && value.OldValue == null)
                {
                    return;
                }

                timestampRight = -1;
            }
            else
            {
                timestampRight = valueAndTsRight.Timestamp;
            }

            resultTimestamp = Math.Max(Context.Timestamp, timestampRight);

            if (value.NewValue != null)
            {
                newValue = joiner.Apply(value.NewValue, valueAndTsRight == null ? default : valueAndTsRight.Value);
            }

            if (sendOldValues && value.OldValue != null)
            {
                oldValue = joiner.Apply(value.OldValue, valueAndTsRight == null ? default : valueAndTsRight.Value);
            }

            Forward(key, new Change<VR>(oldValue, newValue), resultTimestamp);
        }
    }
}
