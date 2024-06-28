using System;
using Azure;
using Azure.Data.Tables;

namespace Streamiz.Kafka.Net.Azure.RemoteStorage.Internal
{
    public class AzureTableEntity : ITableEntity
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset? Timestamp { get; set; }
        public ETag ETag { get; set; }
        public byte[] Value { get; set; }
        public byte[] Key { get; set; }
    }
}