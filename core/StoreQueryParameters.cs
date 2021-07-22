using Streamiz.Kafka.Net.State.Internal;

namespace Streamiz.Kafka.Net
{
    /// <summary>
    /// Allows you to create <see cref="StoreQueryParameters{T, K, V}"/> you can later use to  pass a variety of parameters when fetching a store for interactive query.
    /// </summary>
    public static class StoreQueryParameters
    {
        /// <summary>
        /// Creates <see cref="StoreQueryParameters{T, K, V}"/> with specified storeName and queryableStoreType
        /// </summary>
        /// <typeparam name="T">The type of the Store to be fetched</typeparam>
        /// <typeparam name="K">The type of key</typeparam>
        /// <typeparam name="V">The type of value</typeparam>
        /// <param name="storeName">The name of the state store that should be queried.</param>
        /// <param name="queryableStoreType">The <see cref="IQueryableStoreType{T, K, V}"/> for which key is queried by the user.</param>
        /// <returns><see cref="StoreQueryParameters{T, K, V}"/> with provided <code>name</code> and <code>queryableStoreType</code></returns>
        public static StoreQueryParameters<T, K, V> FromNameAndType<T, K, V>(string storeName, IQueryableStoreType<T, K, V> queryableStoreType) where T : class
        {
            return new StoreQueryParameters<T, K, V>(storeName, queryableStoreType, null, false);
        }
    }

    /// <summary>
    /// Allows you to pass a variety of parameters when fetching a store for interactive query.
    /// </summary>
    /// <typeparam name="T">The type of the Store to be fetched</typeparam>
    /// <typeparam name="K">Key type</typeparam>
    /// <typeparam name="V">Value type</typeparam>
    public class StoreQueryParameters<T, K, V> 
        where T : class
    {
        //public int? Partition { get; private set; }

        //public bool StaleStores { get; private set; }

        /// <summary>
        /// The name of the state store that should be queried.
        /// </summary>
        public string StoreName { get; private set; }

        /// <summary>
        /// The <see cref="IQueryableStoreType{T, K, V}"/> for which key is queried by the user.
        /// </summary>
        public IQueryableStoreType<T, K, V> QueryableStoreType { get; private set; }

        internal StoreQueryParameters(string storeName, IQueryableStoreType<T, K, V> queryableStoreType, int? partition, bool staleStores)
        {
            StoreName = storeName;
            QueryableStoreType = queryableStoreType;
            //Partition = partition;
            //StaleStores = staleStores;
        }

        //public StoreQueryParameters<T> WithPartition(int partition)
        //{
        //    return new StoreQueryParameters<T>(StoreName, QueryableStoreType, partition, StaleStores);
        //}

        //public StoreQueryParameters<T> EnableStaleStores()
        //{
        //    return new StoreQueryParameters<T>(StoreName, QueryableStoreType, Partition, true);
        //}
    }
}
