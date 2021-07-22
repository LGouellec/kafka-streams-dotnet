using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Table.Internal
{
    internal interface IKTableValueGetter<K,V>
    {
        void Init(ProcessorContext context);

        ValueAndTimestamp<V> Get(K key);

        void Close();
    }
}
