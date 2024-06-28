using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Azure.RemoteStorage
{
    public static class AzureRemoteStorage
    {
        public static Materialized<K, V, IKeyValueStore<Bytes, byte[]>> @As<K, V>( 
            AzureRemoteStorageOptions azureRemoteStorageOptions = null,
            string storeName = null)
        {
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized
                = Materialized<K, V, IKeyValueStore<Bytes, byte[]>>
                    .Create(new AzureRemoteStorageSupplier(azureRemoteStorageOptions, storeName))
                    .WithName(storeName)
                    .WithLoggingDisabled(); // explicitly disabled logging
            
            return materialized;
        }
        
        public static Materialized<K, V, IKeyValueStore<Bytes, byte[]>> @As<K, V>(
            string storeName = null)
        {
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized
                = Materialized<K, V, IKeyValueStore<Bytes, byte[]>>
                    .Create(new AzureRemoteStorageSupplier(null, storeName))
                    .WithName(storeName)
                    .WithLoggingDisabled(); // explicitly disabled logging
            
            return materialized;
        }
        
        public static Materialized<K, V, IKeyValueStore<Bytes, byte[]>> @As<K, V>()
        {
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized
                = Materialized<K, V, IKeyValueStore<Bytes, byte[]>>
                    .Create(new AzureRemoteStorageSupplier(null, null))
                    .WithName(null)
                    .WithLoggingDisabled(); // explicitly disabled logging
            
            return materialized;
        }
    }
}