using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Internal;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Mock.Kafka
{
    internal class MockReadOnlyKeyValueStore<K, V> : IStateStore, IReadOnlyKeyValueStore<K, V>
    {
        private readonly IEnumerable<IStateStore> stores;

        public string Name => "MockStore";

        public bool Persistent => false;
        public bool IsLocally => true;

        public bool IsOpen => true;

        public MockReadOnlyKeyValueStore(IEnumerable<IStateStore> stores)
        {
            this.stores = stores;
        }

        private IEnumerable<IReadOnlyKeyValueStore<K, V>> GetAllStores()
        {
            var readonlystores = stores
                .OfType<IReadOnlyKeyValueStore<K, V>>()
                .ToList();

            var timestamp = stores
                .OfType<ITimestampedKeyValueStore<K, V>>()
                .Select(s => new ReadOnlyKeyValueStoreFacade<K, V>(s));

            readonlystores.AddRange(timestamp);
            return readonlystores;
        }

        public IEnumerable<KeyValuePair<K, V>> All() => GetAllStores().SelectMany(x => x.All());

        public IEnumerable<KeyValuePair<K, V>> ReverseAll() => GetAllStores().SelectMany(x => x.ReverseAll());

        public long ApproximateNumEntries() => GetAllStores().Sum(x => x.ApproximateNumEntries());

        public V Get(K key)
        {
            IEnumerable<IReadOnlyKeyValueStore<K, V>> allStores = GetAllStores();
            var item = allStores.FirstOrDefault(x => x.Get(key) != null);
            return item != null ? item.Get(key) : default;
        }

        public void Init(ProcessorContext context, IStateStore root)
        {
            context.Register(root, null);
        }

        public void Flush()
        {
            // NOTHING
        }

        public void Close()
        {
            // NOTHING
        }

        public IKeyValueEnumerator<K, V> Range(K from, K to)
            => new CompositeKeyValueEnumerator<K, V>(GetAllStores().Select(x => x.Range(from, to)));

        public IKeyValueEnumerator<K, V> ReverseRange(K from, K to)
            => new CompositeKeyValueEnumerator<K, V>(GetAllStores().Select(x => x.ReverseRange(from, to)));

    }
}