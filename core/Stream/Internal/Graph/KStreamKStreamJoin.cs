using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph
{
    internal class KStreamKStreamJoin<K, V, V1, VR> : IProcessorSupplier<K, V>
    {
        private readonly string name;
        private readonly string otherWindowName;
        private readonly long beforeMs;
        private readonly long afterMs;
        private readonly IValueJoiner<V, V1, VR> joiner;
        private readonly bool outer;

        public KStreamKStreamJoin(string name, string otherWindowName, long beforeMs, long afterMs, IValueJoiner<V, V1, VR> joiner, bool outer)
        {
            this.name = name;
            this.otherWindowName = otherWindowName;
            this.beforeMs = beforeMs;
            this.afterMs = afterMs;
            this.joiner = joiner;
            this.outer = outer;
        }

        public IProcessor<K, V> Get()
            => new KStreamKStreamJoinProcessor<K, V, V1, VR>(name, otherWindowName, beforeMs, afterMs, joiner, outer);
    }
}
