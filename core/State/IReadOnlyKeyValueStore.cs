using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.State.Enumerator;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// A key-value store that only supports read operations.
    /// Implementations should be thread-safe as concurrent reads and writes are expected.
    /// Please note that this contract defines the thread-safe read functionality only; it does not
    /// guarantee anything about whether the actual instance is writable by another thread, or
    /// whether it uses some locking mechanism under the hood. For this reason, making dependencies
    /// between the read and write operations on different StateStore instances can cause concurrency
    /// problems like deadlock.
    /// </summary>
    /// <typeparam name="K">the key type</typeparam>
    /// <typeparam name="V">the value type</typeparam>
    public interface IReadOnlyKeyValueStore<K, V>
    {
        /// <summary>
        /// Get the value corresponding to this key.
        /// </summary>
        /// <param name="key">the key to fetch</param>
        /// <returns>The value or null if no value is found.</returns>
        V Get(K key);

        /// <summary>
        /// Get an enumerator over a given range of keys. This enumerator must be closed after use.
        /// Order is not guaranteed as bytes lexicographical ordering might not represent key order.
        /// </summary>
        /// <param name="from">The first key that could be in the range, where iteration starts from.</param>
        /// <param name="to">The last key that could be in the range, where iteration ends.</param>
        /// <returns>The enumerator for this range, from smallest to largest bytes.</returns>
        IKeyValueEnumerator<K, V> Range(K from, K to);

        /// <summary>
        /// Get a reverser enumerator over a given range of keys. This enumerator must be closed after use.
        /// Order is not guaranteed as bytes lexicographical ordering might not represent key order.
        /// </summary>
        /// <param name="from">The first key that could be in the range, where iteration starts from.</param>
        /// <param name="to">The last key that could be in the range, where iteration ends.</param>
        /// <returns>The reverse enumerator for this range, from smallest to largest bytes.</returns>
        /// <exception cref="InvalidStateStoreException">if the store is not initialized</exception>
        IKeyValueEnumerator<K, V> ReverseRange(K from, K to);

        /// <summary>
        /// Return an enumerator over all keys in this store. No ordering guarantees are provided.
        /// </summary>
        /// <returns>An enumerator of all key/value pairs in the store.</returns>
        /// <exception cref="InvalidStateStoreException">if the store is not initialized</exception>
        IEnumerable<KeyValuePair<K, V>> All();

        /// <summary>
        /// Return a reverse enumerator over all keys in this store. No ordering guarantees are provided.
        /// </summary>
        /// <returns>A reverse enumerator of all key/value pairs in the store.</returns>
        /// <exception cref="InvalidStateStoreException">if the store is not initialized</exception>
        IEnumerable<KeyValuePair<K, V>> ReverseAll();

        /// <summary>
        /// Return an approximate count of key-value mappings in this store.
        /// The count is not guaranteed to be exact in order to accommodate stores
        /// where an exact count is expensive to calculate.
        /// </summary>
        /// <returns>an approximate count of key-value mappings in the store.</returns>
        long ApproximateNumEntries();
    }
}
