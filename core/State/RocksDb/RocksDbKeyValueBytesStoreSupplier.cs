using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Supplier;

namespace Streamiz.Kafka.Net.State.RocksDb
{
    public class RocksDbKeyValueBytesStoreSupplier : IKeyValueBytesStoreSupplier
    {
        public RocksDbKeyValueBytesStoreSupplier(string name)
        {
            Name = name;
        }

        public string Name { get; }

        public IKeyValueStore<Bytes, byte[]> Get() =>
            new RocksDbKeyValueStore(Name);
    }
}
