using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.State.Enumerator
{
    internal class CompositeKeyValueEnumerator<K, V, S> : IKeyValueEnumerator<K, V>
    {
        private readonly List<S> storeIterator;
        private readonly Func<S, IKeyValueEnumerator<K, V>> nextIterator;
        private IKeyValueEnumerator<K, V> current;
        private int index = 0;

        public CompositeKeyValueEnumerator(
            IEnumerable<S> storeEnumerable,
            Func<S, IKeyValueEnumerator<K, V>> nextIterator)
        {
            storeIterator = new List<S>(storeEnumerable);
            this.nextIterator = nextIterator;
            current = null;
        }

        public KeyValuePair<K, V>? Current => current.Current;

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            CloseCurrentEnumerator();
        }

        private void CloseCurrentEnumerator()
        {
            current?.Dispose();
            current = null;
        }

        public bool MoveNext()
        {
            while ((current == null || !current.MoveNext()) && index < storeIterator.Count)
            {
                CloseCurrentEnumerator();
                current = nextIterator?.Invoke(storeIterator[index]);
                ++index;
            }

            return current != null && current.Current.HasValue;
        }

        public K PeekNextKey()
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
            CloseCurrentEnumerator();
            index = 0;
        }
    }

    internal class CompositeKeyValueEnumerator<K, V> : IKeyValueEnumerator<K, V>
    {
        private List<IKeyValueEnumerator<K, V>> enumerator;
        private IKeyValueEnumerator<K, V> current = null;
        private int index = 0;

        public CompositeKeyValueEnumerator(IEnumerable<IKeyValueEnumerator<K, V>> enumerable)
        {
            this.enumerator = enumerable.ToList();
        }

        private void CloseCurrentEnumerator()
        {
            current?.Dispose();
        }

        public KeyValuePair<K, V>? Current => current.Current;

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            CloseCurrentEnumerator();
            index = 0;
            enumerator.Clear();
        }

        public bool MoveNext()
        {
            bool hasNext = false;
            while ((current == null || !(hasNext = current.MoveNext())) && index < enumerator.Count)
            {
                CloseCurrentEnumerator();
                current = enumerator[index];
                ++index;
            }

            return current != null && hasNext;
        }

        public K PeekNextKey()
        {
            if (current.Current.HasValue)
                return current.Current.Value.Key;
            else
                return default(K);
        }

        public void Reset()
        {
            current?.Reset();
            current = null;
            index = 0;
        }
    }
}