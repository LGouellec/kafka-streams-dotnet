using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using System;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamKStreamJoinProcessor<K, V, V1, VR> : AbstractProcessor<K, V>
    {
        private readonly string windowStoreName;
        private readonly long beforeMs;
        private readonly long afterMs;
        private readonly IValueJoiner<V, V1, VR> joiner;
        private readonly bool outer;

        private IWindowStore<K, V1> window;


        public KStreamKStreamJoinProcessor(
            string name,
            string windowStoreName,
            long beforeMs,
            long afterMs,
            IValueJoiner<V, V1, VR> joiner,
            bool outer)
            : base(name)
        {
            this.windowStoreName = windowStoreName;
            this.beforeMs = beforeMs;
            this.afterMs = afterMs;
            this.joiner = joiner;
            this.outer = outer;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);
            window = (IWindowStore<K, V1>)context.GetStateStore(windowStoreName);
        }

        public override void Process(K key, V value)
        {
            if (key == null || value == null)
            {
                log.LogWarning($"Skipping record due to null key or value. key=[{key}] value=[{value}] topic=[{Context.Topic}] partition=[{Context.Partition}] offset=[{Context.Offset}]");
                return;
            }

            long inputRecordTimestamp = Context.Timestamp;
            long timeFrom = Math.Max(0L, inputRecordTimestamp - beforeMs);
            long timeTo = Math.Max(0L, inputRecordTimestamp + afterMs);
            bool needOuterJoin = outer;

            using (var enumerator = window.Fetch(key, timeFrom, timeTo))
            {
                while (enumerator.MoveNext())
                {
                    needOuterJoin = false;
                    var otherRecord = enumerator.Current;
                    Forward(key,
                        joiner.Apply(value, otherRecord.Value.Value),
                        Math.Max(inputRecordTimestamp, otherRecord.Value.Key));
                }
            }

            if (needOuterJoin)
                Forward(key, joiner.Apply(value, default));
        }
    }
}
