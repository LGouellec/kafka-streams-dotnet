using RocksDbSharp;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State.Enumerator;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.State.RocksDb
{
    #region RocksDb Enumerator Wrapper

    internal class WrappedRocksRbKeyValueEnumerator : IKeyValueEnumerator<Bytes, byte[]>
    {
        private readonly IKeyValueEnumerator<Bytes, byte[]> wrapped;
        private readonly Func<WrappedRocksRbKeyValueEnumerator, bool> closingCallback;
        private bool disposed = false;

        public WrappedRocksRbKeyValueEnumerator(IKeyValueEnumerator<Bytes, byte[]> enumerator, Func<WrappedRocksRbKeyValueEnumerator, bool> closingCallback)
        {
            this.wrapped = enumerator;
            this.closingCallback = closingCallback;
        }

        public KeyValuePair<Bytes, byte[]>? Current => wrapped.Current;

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            if (!disposed)
            {
                wrapped.Dispose();
                closingCallback?.Invoke(this);
                disposed = true;
            }
            else
                throw new ObjectDisposedException("Enumerator was disposed");

        }

        public bool MoveNext()
            => wrapped.MoveNext();

        public Bytes PeekNextKey()
            => wrapped.PeekNextKey();

        public void Reset()
            => wrapped.Reset();
    }
    
    #endregion

    public class RocksDbKeyValueStore : IKeyValueStore<Bytes, byte[]>
    {
        private static readonly ILogger log = Logger.GetLogger(typeof(RocksDbKeyValueStore));
        private readonly ISet<WrappedRocksRbKeyValueEnumerator> openIterators = new HashSet<WrappedRocksRbKeyValueEnumerator>();


        private const Compression COMPRESSION_TYPE = Compression.No;
        private const Compaction COMPACTION_STYLE = Compaction.Universal;
        private const long WRITE_BUFFER_SIZE = 16 * 1024 * 1024L;
        private const long BLOCK_CACHE_SIZE = 50 * 1024 * 1024L;
        private const long BLOCK_SIZE = 4096L;
        private const int MAX_WRITE_BUFFERS = 3;
        private const string DB_FILE_DIR = "rocksdb";
        private readonly string parentDir;

        private WriteOptions writeOptions;

        internal DirectoryInfo DbDir { get; private set; }
        internal RocksDbSharp.RocksDb Db { get; set; }
        internal IRocksDbAdapter DbAdapter { get; private set; }
        internal ProcessorContext InternalProcessorContext { get; set; }
        protected Func<byte[], byte[], int> KeyComparator { get; set; }

        public RocksDbKeyValueStore(string name)
            : this(name, DB_FILE_DIR)
        {
        }

        public RocksDbKeyValueStore(string name, string parentDir)
        {
            Name = name;
            this.parentDir = parentDir;
            KeyComparator = CompareKey;
        }

        #region Store Impl

        public string Name { get; }

        public bool Persistent => true;

        public bool IsOpen { get; private set; }

        public IEnumerable<KeyValuePair<Bytes, byte[]>> All()
            => All(true);

        public long ApproximateNumEntries()
        {
            CheckStateStoreOpen();
            long num = 0;
            try
            {
                num = DbAdapter.ApproximateNumEntries();
            }
            catch (RocksDbSharp.RocksDbException e)
            {
                throw new ProcessorStateException("Error while getting value for key from store {Name}", e);
            }

            return num > 0 ? num : 0;
        }

        public void Close()
        {
            if (!IsOpen)
                return;

            if (openIterators.Count != 0)
            {
                log.LogWarning("Closing {OpenIteratorsCount} open iterators for store {Name}", openIterators.Count, Name);
                for (int i = 0; i < openIterators.Count; ++i)
                    openIterators.ElementAt(i).Dispose();
            }

            IsOpen = false;
            DbAdapter.Close();
            Db.Dispose();

            DbAdapter = null;
            Db = null;
        }

        public byte[] Delete(Bytes key)
        {
            CheckStateStoreOpen();
            byte[] oldValue = null;

            try
            {
                oldValue = DbAdapter.GetOnly(key.Get);
            }
            catch (RocksDbSharp.RocksDbException e){
                throw new ProcessorStateException("Error while getting value for key from store {Name}", e);
            }

            Put(key, null);
            return oldValue;
        }

        public void Flush()
        {
            CheckStateStoreOpen();
            if (Db == null)
                return;
            
            try
            {
                DbAdapter.Flush();
            }
            catch (RocksDbSharp.RocksDbException e)
            {
                throw new ProcessorStateException("Error while getting value for key from store {Name}", e);
            }
        }

        public byte[] Get(Bytes key)
        {
            CheckStateStoreOpen();
            try
            {
                return DbAdapter.Get(key.Get);
            }
            catch (RocksDbSharp.RocksDbException e) {
                throw new ProcessorStateException($"Error while getting value for key from store {Name}", e);
            }
        }

        public void Init(ProcessorContext context, IStateStore root)
        {
            InternalProcessorContext = context;
            OpenDatabase(context);

            // TODO : batch behavior
            context.Register(root, (k, v) => Put(k, v));
        }

        public void Put(Bytes key, byte[] value)
        {
            CheckStateStoreOpen();
            DbAdapter.Put(key.Get, value);
        }

        public void PutAll(IEnumerable<KeyValuePair<Bytes, byte[]>> entries)
        {
            try
            {
                using (var batch = new WriteBatch())
                {
                    DbAdapter.PrepareBatch(entries, batch);
                    Db.Write(batch, writeOptions);
                }
            }
            catch (RocksDbSharp.RocksDbException e)
            {
                throw new ProcessorStateException($"Error while batch writing to store {Name}", e);
            }
        }

        public byte[] PutIfAbsent(Bytes key, byte[] value)
        {
            var originalValue = Get(key);
            if (originalValue == null)
                Put(key, value);

            return originalValue;
        }

        public IKeyValueEnumerator<Bytes, byte[]> Range(Bytes from, Bytes to)
            => Range(from, to, true);

        public IKeyValueEnumerator<Bytes, byte[]> ReverseRange(Bytes from, Bytes to)
            => Range(from, to, false);

        public IEnumerable<KeyValuePair<Bytes, byte[]>> ReverseAll()
            => All(false);

        #endregion

        #region Private

        protected void OpenDatabase(ProcessorContext context)
        {
            DbOptions dbOptions = new DbOptions();
            ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
            writeOptions = new WriteOptions();
            BlockBasedTableOptions tableConfig = new BlockBasedTableOptions();

            RocksDbOptions rocksDbOptions = new RocksDbOptions(dbOptions, columnFamilyOptions);

            tableConfig.SetBlockCache(RocksDbSharp.Cache.CreateLru(BLOCK_CACHE_SIZE));
            tableConfig.SetBlockSize(BLOCK_SIZE);
            tableConfig.SetFilterPolicy(BloomFilterPolicy.Create());

            rocksDbOptions.SetOptimizeFiltersForHits(1);
            rocksDbOptions.SetBlockBasedTableFactory(tableConfig);
            rocksDbOptions.SetCompression(COMPRESSION_TYPE);
            rocksDbOptions.SetWriteBufferSize(WRITE_BUFFER_SIZE);
            rocksDbOptions.SetCompactionStyle(COMPACTION_STYLE);
            rocksDbOptions.SetMaxWriteBufferNumber(MAX_WRITE_BUFFERS);
            rocksDbOptions.SetCreateIfMissing(true);
            rocksDbOptions.SetErrorIfExists(false);
            rocksDbOptions.SetInfoLogLevel(RocksLogLevel.ERROR);
            // this is the recommended way to increase parallelism in RocksDb
            // note that the current implementation of setIncreaseParallelism affects the number
            // of compaction threads but not flush threads (the latter remains one). Also
            // the parallelism value needs to be at least two because of the code in
            // https://github.com/facebook/rocksdb/blob/62ad0a9b19f0be4cefa70b6b32876e764b7f3c11/util/options.cc#L580
            // subtracts one from the value passed to determine the number of compaction threads
            // (this could be a bug in the RocksDB code and their devs have been contacted).
            rocksDbOptions.IncreaseParallelism(Math.Max(Environment.ProcessorCount, 2));
            
            writeOptions.DisableWal(1);

            context.Configuration.RocksDbConfigHandler?.Invoke(Name, rocksDbOptions);

            DbDir = new DirectoryInfo(Path.Combine(context.StateDir, parentDir, Name));

            Directory.CreateDirectory(DbDir.FullName);

            OpenRocksDB(dbOptions, columnFamilyOptions);

            IsOpen = true;
        }

        private void OpenRocksDB(DbOptions dbOptions, ColumnFamilyOptions columnFamilyOptions)
        {
            int maxRetries = 5;
            int i = 0;
            bool open = false;
            RocksDbException rocksDbException = null;

            var columnFamilyDescriptors = new ColumnFamilies(columnFamilyOptions);

            while (!open && i < maxRetries)
            {
                try
                {
                    Db = RocksDbSharp.RocksDb.Open(
                        dbOptions,
                        DbDir.FullName,
                        columnFamilyDescriptors);

                    var columnFamilyHandle = Db.GetDefaultColumnFamily();
                    DbAdapter = new SingleColumnFamilyAdapter(
                        Name,
                        Db,
                        writeOptions,
                        KeyComparator,
                        columnFamilyHandle);
                    open = true;
                }
                catch (RocksDbException e)
                {
                    ++i;
                    rocksDbException = e;
                }
            }

            if(!open)
                throw new ProcessorStateException("Error opening store " + Name + " at location " + DbDir.ToString(), rocksDbException);
        }

        private void CheckStateStoreOpen()
        {
            if (!IsOpen)
            {
                throw new InvalidStateStoreException($"Store {Name} is currently closed");
            }
        }

        private IKeyValueEnumerator<Bytes, byte[]> Range(Bytes from, Bytes to, bool forward)
        {
            if (KeyComparator.Invoke(from.Get, to.Get) > 0)
            {
                log.LogWarning("Returning empty iterator for fetch with invalid key range: from > to. "
                            + "This may be due to range arguments set in the wrong order, " +
                            "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                            "Note that the built-in numerical serdes do not follow this for negative numbers");
                return new EmptyKeyValueEnumerator<Bytes, byte[]>();
            }

            CheckStateStoreOpen();

            var rocksEnumerator = DbAdapter.Range(from, to, forward);
            var wrapped = new WrappedRocksRbKeyValueEnumerator(rocksEnumerator, openIterators.Remove);
            openIterators.Add(wrapped);
            return wrapped;
        }
        
        private IEnumerable<KeyValuePair<Bytes, byte[]>> All(bool forward)
        {
            var enumerator = DbAdapter.All(forward);
            var wrapped = new WrappedRocksRbKeyValueEnumerator(enumerator, openIterators.Remove);
            openIterators.Add(wrapped);
            return new RocksDbEnumerable(Name, wrapped);
        }

        #endregion

        /// <summary>
        /// Use to RocksDbRangeEnumerator to compare two keys
        /// </summary>
        /// <param name="key1">From key</param>
        /// <param name="key2">To key</param>
        /// <returns></returns>
        protected int CompareKey(byte[] key1, byte[] key2)
            => BytesComparer.Compare(key1, key2);
    }
}