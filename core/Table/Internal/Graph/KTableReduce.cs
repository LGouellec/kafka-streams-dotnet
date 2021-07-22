using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Table.Internal.Graph
{
    internal class KTableReduce<K, V> : IKTableProcessorSupplier<K, V, V>
    {
        private readonly string storeName;
        private readonly Reducer<V> adder;
        private readonly Reducer<V> substractor;

        private bool sendOldValues = false;


        public KTableReduce(string storeName, Reducer<V> adder, Reducer<V> substractor)
        {
            this.storeName = storeName;
            this.adder = adder;
            this.substractor = substractor;
        }

        public IKTableValueGetterSupplier<K, V> View
            => new KTableMaterializedValueGetterSupplier<K, V>(storeName);

        public void EnableSendingOldValues()
        {
            sendOldValues = true;
        }

        public IProcessor<K, Change<V>> Get()
            => new KTableReduceProcessor<K, V>(storeName, sendOldValues, adder, substractor);
    }
}
