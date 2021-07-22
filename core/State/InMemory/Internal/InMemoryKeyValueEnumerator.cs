using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Enumerator;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.State.InMemory.Internal
{
    internal class InMemoryKeyValueEnumerator : IKeyValueEnumerator<Bytes, byte[]>
    {
        private readonly IKeyValueEnumerator<Bytes, byte[]> enumerator;

        public InMemoryKeyValueEnumerator(IEnumerable<KeyValuePair<Bytes, byte[]>> values, bool forward)
        {
            enumerator = forward ? values.ToWrap() : values.Reverse().ToWrap();
        }

        public KeyValuePair<Bytes, byte[]>? Current => enumerator.Current;

        object IEnumerator.Current => Current;

        public void Dispose()
            => enumerator.Dispose();

        public bool MoveNext()
            => enumerator.MoveNext();

        public Bytes PeekNextKey()
            => enumerator.Current.Value.Key;

        public void Reset()
            => enumerator.Reset();
    }
}
