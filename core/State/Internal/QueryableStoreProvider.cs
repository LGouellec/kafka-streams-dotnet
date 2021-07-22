using Streamiz.Kafka.Net.Errors;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.State.Internal
{
    /// <summary>
    /// A wrapper over all of the <see cref="IStateStoreProvider{T, K, V}"/>s in a Topology
    /// </summary>
    internal class QueryableStoreProvider
    {
        private readonly IEnumerable<StreamThreadStateStoreProvider> storeProviders;
        private readonly GlobalStateStoreProvider globalStateStoreProvider;

        public QueryableStoreProvider(IEnumerable<StreamThreadStateStoreProvider> storeProviders,
            GlobalStateStoreProvider globalStateStoreProvider)
        {
            this.storeProviders = new List<StreamThreadStateStoreProvider>(storeProviders);
            this.globalStateStoreProvider = globalStateStoreProvider;
        }

        /// <summary>
        /// Get a composite object wrapping the instances of the <see cref="Processors.IStateStore"/> with the provided
        /// storeName and <see cref="IQueryableStoreType{T, K, V}"/>
        /// </summary>
        /// <typeparam name="T">The expected type of the returned store</typeparam>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="V">Value type</typeparam>
        /// <param name="storeQueryParameters">parameters to be used when querying for store</param>
        /// <returns>A composite object that wraps the store instances.</returns>
        public T GetStore<T, K, V>(StoreQueryParameters<T, K, V> storeQueryParameters) 
            where T : class
        {
            IEnumerable<T> globalStore = this.globalStateStoreProvider.Stores(storeQueryParameters);
            if (globalStore.Any())
            {
                return storeQueryParameters.QueryableStoreType.Create(new GlobalStateStoreProviderFacade<T, K, V>(this.globalStateStoreProvider), storeQueryParameters.StoreName);
            }

            IEnumerable<T> allStores = this.storeProviders
                .SelectMany(store => store.Stores(storeQueryParameters));

            if (!allStores.Any())
            {
                throw new InvalidStateStoreException($"The state store, {storeQueryParameters.StoreName}, may have migrated to another instance.");
            }

            return storeQueryParameters
                .QueryableStoreType
                .Create(
                    new WrappingStoreProvider<T, K, V>(this.storeProviders, storeQueryParameters),
                    storeQueryParameters.StoreName);
        }
    }
}
