using Streamiz.Kafka.Net.Azure.RemoteStorage.Internal;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Supplier;

namespace Streamiz.Kafka.Net.Azure.RemoteStorage
{
    public class AzureRemoteStorageSupplier : IKeyValueBytesStoreSupplier
    {
        private AzureRemoteStorageOptions AzureRemoteStorageOptions { get; set; }
        public string Name { get; set; }

        public AzureRemoteStorageSupplier(AzureRemoteStorageOptions azureRemoteStorageOptions, string name)
        {
            AzureRemoteStorageOptions = azureRemoteStorageOptions;
            Name = name;
        }

        public IKeyValueStore<Bytes, byte[]> Get()
            => new AzureTableStore(AzureRemoteStorageOptions, Name);

        public string MetricsScope => "azure-remote-storage";
    }
}