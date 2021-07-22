using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State.Enumerator
{
    /// <summary>
    /// Iterator interface of <see cref="KeyValuePair{K, V}"/> with key typed long used for <see cref="IReadOnlyWindowStore{K, V}.Fetch(K, DateTime, DateTime)"/>.
    /// Users must call its <code>Dispose()</code> method explicitly upon completeness to release resources,
    /// or use "using" keyword.
    /// </summary>
    /// <typeparam name="V">Type of values</typeparam>
    public interface IWindowStoreEnumerator<V> : IKeyValueEnumerator<long, V>
    {
    }
}
