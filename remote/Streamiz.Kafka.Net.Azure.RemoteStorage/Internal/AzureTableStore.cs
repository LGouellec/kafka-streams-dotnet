using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using Azure.Data.Tables;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Enumerator;

namespace Streamiz.Kafka.Net.Azure.RemoteStorage.Internal
{
    internal class AzureTableStore : IKeyValueStore<Bytes, byte[]>
    {
        private readonly AzureRemoteStorageOptions _azureRemoteStorageOptions;
        private TableClient _tableClient;
        private ProcessorContext _context;
        private string _partitionKey;
        private readonly string _azureTableName;

        public string Name { get; }
        public bool Persistent => true; 
        
        public bool IsLocally => false; // avoid changelog topic
        public bool IsOpen { get; private set; }

        public AzureTableStore(AzureRemoteStorageOptions azureRemoteStorageOptions,
            string name)
        {
            _azureRemoteStorageOptions = azureRemoteStorageOptions ?? new AzureRemoteStorageOptions();
            // azure table doesn't allow special characters on the table name
            _azureTableName = name.Replace("_", string.Empty).Replace("-", string.Empty); 
            Name = name;
        }

        #region IKeyValueStore impl

        public void Init(ProcessorContext context, IStateStore root)
        {
            _partitionKey = context.Id.ToString();
            var remoteStorageOptions = context.Configuration?.Read<AzureRemoteStorageOptions>();

            if (remoteStorageOptions != null)
            {
                _azureRemoteStorageOptions.StorageUri = _azureRemoteStorageOptions.StorageUri ?? remoteStorageOptions.StorageUri; 
                _azureRemoteStorageOptions.StorageAccountKey = _azureRemoteStorageOptions.StorageAccountKey ?? remoteStorageOptions.StorageAccountKey;
                _azureRemoteStorageOptions.AccountName = _azureRemoteStorageOptions.AccountName ?? remoteStorageOptions.AccountName;
            }

            if (_azureRemoteStorageOptions.StorageUri == null ||
                _azureRemoteStorageOptions.StorageAccountKey == null ||
                _azureRemoteStorageOptions.AccountName == null)
                throw new StreamConfigException(
                    "Azure remote store needs StorageUri, StorageAccountKey and AccountName configuration.");

            _tableClient = new TableClient(
                new Uri(_azureRemoteStorageOptions.StorageUri),
                _azureTableName,
                new TableSharedKeyCredential(_azureRemoteStorageOptions.AccountName,
                    _azureRemoteStorageOptions.StorageAccountKey));

            _tableClient.CreateIfNotExists();
            IsOpen = true;
            
            if (root != null)
            {
                // register the store
                context.Register(root, (key, value, _) => Put(key, value));
            }
        }

        public void Flush()
        {
            /* nothing */
        }

        public void Close()
        {
            /* nothing */
        }

        public byte[] Get(Bytes key)
        {
            var hashKey = ToHashKey(key);

            var entity = _tableClient.GetEntityIfExists<AzureTableEntity>(
                _partitionKey,
                hashKey);

            return entity.HasValue ? entity.Value.Value : null;
        }

        public IKeyValueEnumerator<Bytes, byte[]> Range(Bytes from, Bytes to)
            => new WrapEnumerableKeyValueEnumerator<Bytes, byte[]>(InternalRange(from, to, true));

        public IKeyValueEnumerator<Bytes, byte[]> ReverseRange(Bytes from, Bytes to)
            => new WrapEnumerableKeyValueEnumerator<Bytes, byte[]>(InternalRange(from, to, false));

        public IEnumerable<KeyValuePair<Bytes, byte[]>> All()
            => InternalAll(true);

        public IEnumerable<KeyValuePair<Bytes, byte[]>> ReverseAll()
            => InternalAll(false);

        // No way to return the count of records in Azure Tables. Used only for test, anyway
        public long ApproximateNumEntries()
            => 0;

        public void Put(Bytes key, byte[] value)
        {
            var hashKey = ToHashKey(key);
            var entity = new AzureTableEntity
            {
                Value = value,
                Key = key.Get,
                PartitionKey = _partitionKey,
                RowKey = hashKey
            };

            _tableClient.UpsertEntity(entity, TableUpdateMode.Replace);
        }

        public byte[] PutIfAbsent(Bytes key, byte[] value)
        {
            var originalValue = Get(key);
            if (originalValue == null)
                Put(key, value);
            return originalValue;
        }

        public void PutAll(IEnumerable<KeyValuePair<Bytes, byte[]>> entries)
        {
            _tableClient.SubmitTransaction(
                entries.Select(e =>
                    new TableTransactionAction(TableTransactionActionType.UpsertReplace, new AzureTableEntity()
                    {
                        Key = e.Key.Get,
                        Value = e.Value,
                        PartitionKey = _partitionKey,
                        RowKey = ToHashKey(e.Key)
                    })));
        }

        public byte[] Delete(Bytes key)
        {
            _tableClient.DeleteEntity(_partitionKey, ToHashKey(key));
            return null;
        }

        #endregion

        #region Private

        private string ToHashKey(Bytes bytesKey)
        {
            return Convert.ToBase64String(bytesKey.Get, Base64FormattingOptions.None);
        }

        private Bytes FromHashKey(string hashKey)
        {
            return Bytes.Wrap(Convert.FromBase64String(hashKey));
        }

        private IEnumerable<KeyValuePair<Bytes, byte[]>> InternalAll(bool forward)
        {
            var results = _tableClient.Query<AzureTableEntity>(t
                => t.PartitionKey == _partitionKey);

            var orderedEnumerable =
                forward ? results.OrderBy(e => e.Timestamp) : results.OrderByDescending(e => e.Timestamp);

            return orderedEnumerable.Select(e => new KeyValuePair<Bytes, byte[]>(Bytes.Wrap(e.Key), e.Value));
        }

        private IEnumerable<KeyValuePair<Bytes, byte[]>> InternalRange(Bytes from, Bytes to, bool forward)
        {
            var results = _tableClient.Query<AzureTableEntity>(t =>
                BytesComparer.Compare(t.Key, from.Get) >= 0 && BytesComparer.Compare(t.Key, to.Get) <= 0);

            var resultsOrdered = forward
                ? results.OrderBy(e => e.Timestamp)
                : results.OrderByDescending(e => e.Timestamp);

            return resultsOrdered.Select(e => new KeyValuePair<Bytes, byte[]>(Bytes.Wrap(e.Key), e.Value));
        }

        #endregion
    }
}