using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State.Enumerator
{
    public static class EnumeratorExtensions
    {
        public static IKeyValueEnumerator<K, V> ToWrap<K, V>(this IEnumerable<KeyValuePair<K, V>> enumerable)
            => new WrapEnumerableKeyValueEnumerator<K, V>(enumerable);

        public static IKeyValueEnumerator<K1, V1> Transform<K, V, K1, V1>(this IKeyValueEnumerator<K, V> enumerator, Func<K, V, KeyValuePair<K1, V1>> function)
            => new TransformKeyValueEnumerator<K, V, K1, V1>(enumerator, function);
    }
}
