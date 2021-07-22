using Streamiz.Kafka.Net.State.Enumerator;
using System;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// TODO : reverseAll(), reverseFetch() ..
    /// A window store that only supports read operations.
    /// Implementations should be thread-safe as concurrent reads and writes are expected.
    /// </summary>
    /// <typeparam name="K">Type of keys</typeparam>
    /// <typeparam name="V">Type of values</typeparam>
    public interface IReadOnlyWindowStore<K,V>
    {
        /// <summary>
        /// Get the value of key from a window.
        /// </summary>
        /// <param name="key">the key to fetch</param>
        /// <param name="time">start timestamp (inclusive) of the window</param>
        /// <returns>The value or null if no value is found in the window</returns>
        V Fetch(K key, long time);

        /// <summary>
        /// Get all the key-value pairs with the given key and the time range from all the existing windows.
        /// This iterator must be closed after use.
        /// <p>
        /// The time range is inclusive and applies to the starting timestamp of the window.
        /// For example, if we have the following windows:
        /// <pre>
        /// +-------------------------------+
        /// |  key  | start time | end time |
        /// +-------+------------+----------+
        /// |   A   |     10     |    20    |
        /// +-------+------------+----------+
        /// |   A   |     15     |    25    |
        /// +-------+------------+----------+
        /// |   A   |     20     |    30    |
        /// +-------+------------+----------+
        /// |   A   |     25     |    35    |
        /// +--------------------------------
        /// </pre>
        /// And we call <code>store.Fetch("A", 10.ToDate(), 20.ToDate())</code> then the results will contain the first
        /// three windows from the table above, i.e., all those where 10 &lt;= start time &lt;= 20.
        /// </p>
        /// For each key, the iterator guarantees ordering of windows, starting from the oldest/earliest
        /// available window to the newest/latest window.
        /// </summary>
        /// <param name="key">the key to fetch</param>
        /// <param name="from">time range start (inclusive)</param>
        /// <param name="to">time range end (inclusive)</param>
        /// <returns>an iterator over key-value pairs <code>timestamp, value</code></returns>
        IWindowStoreEnumerator<V> Fetch(K key, DateTime from, DateTime to);

        /// <summary>
        /// Get all the key-value pairs with the given key and the time range from all the existing windows.
        /// This iterator must be closed after use.
        /// <p>
        /// The time range is inclusive and applies to the starting timestamp of the window.
        /// For example, if we have the following windows:
        /// <pre>
        /// +-------------------------------+
        /// |  key  | start time | end time |
        /// +-------+------------+----------+
        /// |   A   |     10     |    20    |
        /// +-------+------------+----------+
        /// |   A   |     15     |    25    |
        /// +-------+------------+----------+
        /// |   A   |     20     |    30    |
        /// +-------+------------+----------+
        /// |   A   |     25     |    35    |
        /// +--------------------------------
        /// </pre>
        /// And we call <code>store.Fetch("A", 10, 20)</code> then the results will contain the first
        /// three windows from the table above, i.e., all those where 10 &lt;= start time &lt;= 20.
        /// </p>
        /// For each key, the iterator guarantees ordering of windows, starting from the oldest/earliest
        /// available window to the newest/latest window.
        /// </summary>
        /// <param name="key">the key to fetch</param>
        /// <param name="from">time range start (inclusive)</param>
        /// <param name="to">time range end (inclusive)</param>
        /// <returns>an iterator over key-value pairs <code>timestamp, value</code></returns>
        IWindowStoreEnumerator<V> Fetch(K key, long from, long to);

        /// <summary>
        /// Gets all the key-value pairs in the existing windows.
        /// </summary>
        /// <returns>an iterator over windowed key-value pairs <code>Windowed&lt;K&gt;, value</code></returns>
        IKeyValueEnumerator<Windowed<K>, V> All();

        /// <summary>
        /// Gets all the key-value pairs that belong to the windows within in the given time range.
        /// </summary>
        /// <param name="from">the beginning of the time slot from which to search (inclusive)</param>
        /// <param name="to">the end of the time slot from which to search (inclusive)</param>
        /// <returns>an iterator over windowed key-value pairs <code>Windowed&lt;K&gt;, value</code></returns>
        IKeyValueEnumerator<Windowed<K>, V> FetchAll(DateTime from, DateTime to);
    }
}
