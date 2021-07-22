using System;

namespace Streamiz.Kafka.Net.Table.Internal
{
    internal interface IKTableValueGetterSupplier<K,V>
    {
        IKTableValueGetter<K, V> Get();

        String[] StoreNames { get; }
    }
}
