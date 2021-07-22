using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    internal interface IKTableProcessorSupplier
    {
        void EnableSendingOldValues();
    }

    internal interface IKTableProcessorSupplier<K,V,T> : IKTableProcessorSupplier, IProcessorSupplier<K, Change<V>>
    {
        IKTableValueGetterSupplier<K, T> View { get; }
    }
}
