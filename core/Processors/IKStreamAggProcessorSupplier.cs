using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    internal interface IKStreamAggProcessorSupplier<K, V>
    {
        IKTableValueGetterSupplier<K, V> View();
        void EnableSendingOldValues();
    }

    internal interface IKStreamAggProcessorSupplier<K, RK, V, T> : 
        IProcessorSupplier<K, V>, IKStreamAggProcessorSupplier<RK, T>
    {
        new void EnableSendingOldValues();
    }
}
