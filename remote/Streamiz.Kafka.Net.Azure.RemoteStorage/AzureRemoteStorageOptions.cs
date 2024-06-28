namespace Streamiz.Kafka.Net.Azure.RemoteStorage
{
    public class AzureRemoteStorageOptions
    {
        public const string storageUriCst = "azure.remote.storage.uri";
        public const string accountNameCst = "azure.remote.storage.account.name";
        public const string storageAccountKeyCst = "azure.remote.storage.account.key";
        
        [StreamConfigProperty(storageUriCst)]
        public string StorageUri { get; set; }
        
        [StreamConfigProperty(accountNameCst)]
        public string AccountName { get; set; }
        
        [StreamConfigProperty(storageAccountKeyCst)]
        public string StorageAccountKey { get; set; }
    }
}