using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State.Enumerator
{
    /// <summary>
    /// Iterator interface of <see cref="KeyValuePair{K, V}"/>.
    /// Users must call its <code>Dispose()</code> method explicitly upon completeness to release resources,
    /// or use "using" keyword.
    /// </summary>
    /// <typeparam name="K">Type of keys</typeparam>
    /// <typeparam name="V">Type of values</typeparam>
    public interface IKeyValueEnumerator<K, V> : IEnumerator<KeyValuePair<K, V>?>
    {
        /// <summary>
        /// Peek at the next key without advancing the iterator.
        /// </summary>
        /// <returns>the key of the next value that would be returned from the next call to next</returns>
        K PeekNextKey();
    }
}
