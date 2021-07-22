using System;
using System.Collections;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State.Enumerator
{
    internal class TransformKeyValueEnumerator<K, V, K1, V1> : IKeyValueEnumerator<K1, V1>
    {
        private IKeyValueEnumerator<K, V> enumerator;
        private Func<K, V, KeyValuePair<K1, V1>> function;

        public TransformKeyValueEnumerator(IKeyValueEnumerator<K, V> enumerator, Func<K, V, KeyValuePair<K1, V1>> function)
        {
            this.enumerator = enumerator;
            this.function = function;
        }

        public KeyValuePair<K1, V1>? Current
        {
            get
            {
                if (enumerator.Current.HasValue)
                {
                    return function.Invoke(enumerator.Current.Value.Key, enumerator.Current.Value.Value);
                }
                else
                    return null;
            }
        }

        object IEnumerator.Current => Current;

        public void Dispose()
            => enumerator.Dispose();

        public bool MoveNext()
            => enumerator.MoveNext();

        public K1 PeekNextKey()
        {
            if (enumerator.Current.HasValue)
            {
                return function.Invoke(enumerator.Current.Value.Key, enumerator.Current.Value.Value).Key;
            }
            else
                return default(K1);
        }

        public void Reset()
            => enumerator.Reset();
    }
}
