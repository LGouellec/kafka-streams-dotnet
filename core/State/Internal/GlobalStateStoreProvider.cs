using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class GlobalStateStoreProvider
    {
        private readonly IDictionary<string, IStateStore> globalStateStores;

        public GlobalStateStoreProvider(IDictionary<string, IStateStore> globalStateStores)
        {
            this.globalStateStores = globalStateStores;
        }

        public IEnumerable<T> Stores<T, K, V>(StoreQueryParameters<T, K, V> storeQueryParameters)
            where T : class
        {
            string storeName = storeQueryParameters.StoreName;
            IQueryableStoreType<T, K, V> queryableStoreType = storeQueryParameters.QueryableStoreType;
            IStateStore stateStore;
            if (!globalStateStores.TryGetValue(storeName, out stateStore) || !queryableStoreType.Accepts(stateStore))
            {
                return Enumerable.Empty<T>();
            }
            
            if (!stateStore.IsOpen)
            {
                throw new InvalidStateStoreException($"the state store, {storeName}, is not open.");
            }
            if (stateStore is ITimestampedKeyValueStore<K, V> && queryableStoreType is KeyValueStoreType<K, V>)
            {
                return new[] { new ReadOnlyKeyValueStoreFacade<K, V>(stateStore as ITimestampedKeyValueStore<K, V>) as T };
            }
            else if (stateStore is ITimestampedWindowStore<K, V> && queryableStoreType is WindowStoreType<K, V>)
            {
                return new[] { new ReadOnlyWindowStoreFacade<K, V>(stateStore as ITimestampedWindowStore<K, V>) as T };
            }
            else
                return new[] { stateStore as T };
        }
    }
}
