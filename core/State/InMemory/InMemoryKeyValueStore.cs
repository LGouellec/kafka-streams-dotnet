using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.InMemory.Internal;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.State.InMemory
{
    /// <summary>
    /// <see cref="InMemoryKeyValueStore"/> implements <see cref="IKeyValueStore{K, V}"/>. 
    /// This store can be used for development phase. It's not persistent, so be carefull.
    /// </summary>
    public class InMemoryKeyValueStore : IKeyValueStore<Bytes, byte[]>
    {
        private static readonly ILogger log = Logger.GetLogger(typeof(InMemoryKeyValueStore));
        private BytesComparer bytesComparer = new BytesComparer();
        private int size = 0;
        private readonly IDictionary<Bytes, byte[]> map = new Dictionary<Bytes, byte[]>(new BytesComparer());

        /// <summary>
        /// Constructor with the store name
        /// </summary>
        /// <param name="name">Store name</param>
        public InMemoryKeyValueStore(string name)
        {
            Name = name;
        }

        /// <summary>
        /// Name of the store
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Return always false in <see cref="InMemoryKeyValueStore"/>
        /// </summary>
        public bool Persistent => false;

        /// <summary>
        /// Is open
        /// </summary>
        public bool IsOpen { get; private set; } = false;

        /// <summary>
        /// Calculate approximate the number of entries in the state store
        /// </summary>
        /// <returns></returns>
        public long ApproximateNumEntries() => size;

        /// <summary>
        /// Close the storage engine.
        /// Note that this function needs to be idempotent since it may be called
        /// several times on the same state store.
        /// Users only need to implement this function but should NEVER need to call this api explicitly
        /// as it will be called by the library automatically when necessary
        /// </summary>
        public void Close()
        {
            map.Clear();
            size = 0;
            IsOpen = false;
        }

        /// <summary>
        /// Delete the value from the store (if there is one).
        /// </summary>
        /// <param name="key">The key</param>
        /// <returns>Return old value or null if key not found</returns>
        public byte[] Delete(Bytes key)
        {
            byte[] v = map.ContainsKey(key) ? map[key] : null;
            size -= map.Remove(key) ? 1 : 0;
            return v;
        }

        /// <summary>
        /// Flush any cached data. Nothing for moment in <see cref="InMemoryKeyValueStore"/>
        /// </summary>
        public void Flush() { /* Nothing => IN MEMORY */ }

        /// <summary>
        /// Get the value corresponding to this key.
        /// </summary>
        /// <param name="key">The key to fetch</param>
        /// <returns>The value or null if no value is found</returns>
        public byte[] Get(Bytes key) => map.ContainsKey(key) ? map[key] : null;

        /// <summary>
        /// Return an iterator over all keys in this store. No ordering guarantees are provided.
        /// </summary>
        /// <returns>An iterator of all key/value pairs in the store.</returns>
        public IEnumerable<KeyValuePair<Bytes, byte[]>> All()
            => All(true);

        /// <summary>
        /// Return a reverse enumerator over all keys in this store. No ordering guarantees are provided.
        /// </summary>
        /// <returns>A reverse enumerator of all key/value pairs in the store.</returns>
        /// <exception cref="InvalidStateStoreException">if the store is not initialized</exception>
        public IEnumerable<KeyValuePair<Bytes, byte[]>> ReverseAll()
            => All(false);

        /// <summary>
        /// Get an enumerator over a given range of keys. This enumerator must be closed after use.
        /// Order is not guaranteed as bytes lexicographical ordering might not represent key order.
        /// </summary>
        /// <param name="from">The first key that could be in the range, where iteration starts from.</param>
        /// <param name="to">The last key that could be in the range, where iteration ends.</param>
        /// <returns>The enumerator for this range, from smallest to largest bytes.</returns>
        public IKeyValueEnumerator<Bytes, byte[]> Range(Bytes from, Bytes to)
            => Range(from, to, true);

        /// <summary>
        /// Get a reverser enumerator over a given range of keys. This enumerator must be closed after use.
        /// Order is not guaranteed as bytes lexicographical ordering might not represent key order.
        /// </summary>
        /// <param name="from">The first key that could be in the range, where iteration starts from.</param>
        /// <param name="to">The last key that could be in the range, where iteration ends.</param>
        /// <returns>The reverse enumerator for this range, from smallest to largest bytes.</returns>
        /// <exception cref="InvalidStateStoreException">if the store is not initialized</exception>
        public IKeyValueEnumerator<Bytes, byte[]> ReverseRange(Bytes from, Bytes to)
            => Range(from, to, false);

        /// <summary>
        /// Initialize state store.
        /// </summary>
        /// <param name="context">Processor context, used for register this store via <see cref="ProcessorContext.Register(IStateStore, Processors.Internal.StateRestoreCallback)"/></param>
        /// <param name="root">Root store</param>
        public void Init(ProcessorContext context, IStateStore root)
        {
            size = 0;
            if (root != null)
            {
                // register the store
                context.Register(root, (key, value) => Put(key, value));
            }

            IsOpen = true;
        }

        /// <summary>
        /// Update the value associated with this key.
        /// </summary>
        /// <param name="key">The key to associate the value to</param>
        /// <param name="value">The value to update, it can be null.if the serialized bytes are also null it is interpreted as deletes</param>
        public void Put(Bytes key, byte[] value)
        {
            if (value == null)
                size -= map.Remove(key) ? 1 : 0;
            else
                size += map.AddOrUpdate(key, value) ? 1 : 0;
        }

        /// <summary>
        /// Update all the given key/value pairs.
        /// </summary>
        /// <param name="entries">A list of entries to put into the store</param>
        public void PutAll(IEnumerable<KeyValuePair<Bytes, byte[]>> entries)
        {
            foreach (var kp in entries)
                Put(kp.Key, kp.Value);
        }

        /// <summary>
        /// Update the value associated with this key, unless a value is already associated with the key.
        /// </summary>
        /// <param name="key">The key to associate the value to</param>
        /// <param name="value">The value to update, it can be null.if the serialized bytes are also null it is interpreted as deletes</param>
        /// <returns>The old value or null if there is no such key.</returns>
        public byte[] PutIfAbsent(Bytes key, byte[] value)
        {
            var old = Get(key);
            if (old == null)
                Put(key, value);
            
            return old;
        }

        private IKeyValueEnumerator<Bytes, byte[]> Range(Bytes from, Bytes to, bool forward)
        {
            if (bytesComparer.Compare(from, to) > 0)
            {
                log.LogWarning("Returning empty iterator for fetch with invalid key range: from > to. " +
                            "This may be due to range arguments set in the wrong order, " +
                            "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                            "Note that the built-in numerical serdes do not follow this for negative numbers");
                return new EmptyKeyValueEnumerator<Bytes, byte[]>();
            }

            var submap = (new SortedDictionary<Bytes, byte[]>(map, new BytesComparer())).SubMap(from, to, true, true);

            return new InMemoryKeyValueEnumerator(submap, forward);
        }
    
        private IEnumerable<KeyValuePair<Bytes, byte[]>> All(bool forward)
        {
            var enumerator = forward ? map.GetEnumerator() : map.Reverse().GetEnumerator();
            while (enumerator.MoveNext())
                yield return enumerator.Current;
        }
    }
}