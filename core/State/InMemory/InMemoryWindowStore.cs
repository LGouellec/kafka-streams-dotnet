using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.State.Helper;

namespace Streamiz.Kafka.Net.State.InMemory
{
    #region InMemory Iterator

    internal abstract class InMemoryWindowStoreEnumeratorWrapper
    {
        private readonly List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> iterator;
        private List<KeyValuePair<Bytes, byte[]>> valueIterator;
        private readonly Bytes keyFrom;
        private readonly Bytes keyTo;
        private readonly Func<InMemoryWindowStoreEnumeratorWrapper, bool> closingCallback;
        protected readonly bool retainDuplicate;
        private readonly bool allKeys;

        protected int indexIt = 0;
        protected int valueIndexIt = 0;
        protected KeyValuePair<Bytes, byte[]>? next;

        protected bool disposed = false;

        public long CurrentTime { get; private set; }

        public bool HasNext
        {
            get
            {
                if (disposed)
                    throw new ObjectDisposedException("Enumerator was disposed");


                if (valueIterator != null && valueIndexIt >= valueIterator.Count && indexIt >= iterator.Count)
                {
                    next = null;
                    return false;
                }

                next = CalculateNext();
                if (next == null)
                    return false;

                if (allKeys || !retainDuplicate)
                    return true;

                var key = GetKey(next.Value.Key);
                if(KeyWithInRange(key))
                    return true;
                else
                {
                    next = null;
                    return HasNext;
                }
            }
        }

        private bool KeyWithInRange(Bytes key)
        {
            // split all cases for readability and avoid BooleanExpressionComplexity checkstyle warning
            if (keyFrom == null && keyTo == null) {
                // fetch all
                return true;
            } else if (keyFrom == null) {
                // start from the beginning
                return key.CompareTo(GetKey(keyTo)) <= 0;
            } else if (keyTo == null) {
                // end to the last
                return key.CompareTo(GetKey(keyFrom)) >= 0;
            } else {
                // key is within the range
                return key.CompareTo(GetKey(keyFrom)) >= 0 && key.CompareTo(GetKey(keyTo)) <= 0;
            }
        }

        protected KeyValuePair<Bytes, byte[]>? CalculateNext()
        {
            if (disposed)
                throw new ObjectDisposedException("Enumerator was disposed");


            while (valueIterator == null || valueIndexIt >= valueIterator.Count)
            {
                valueIterator = SetRecordIterator();
                if (valueIterator == null)
                    return null;
                else
                    valueIndexIt = 0;
            }

            var e = valueIterator[valueIndexIt];
            ++valueIndexIt;
            return e;
        }

        public InMemoryWindowStoreEnumeratorWrapper(
            Bytes keyFrom,
            Bytes keyTo,
            List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> dataIterator,
            Func<InMemoryWindowStoreEnumeratorWrapper, bool> closingCallback,
            bool retainDuplicate)
        {
            this.keyFrom = keyFrom;
            this.keyTo = keyTo;
            allKeys = keyFrom == null && keyTo == null;
            iterator = dataIterator;
            this.closingCallback = closingCallback;
            this.retainDuplicate = retainDuplicate;
        }

        public void Close()
        {
            if (!disposed)
            {
                iterator.Clear();
                valueIterator?.Clear();
                closingCallback?.Invoke(this);
                disposed = true;
            }
        }

        public void RemoveExpiredData(long time)
        {
            iterator.RemoveAll((k) => k.Key < time);
        }

        private List<KeyValuePair<Bytes, byte[]>> SetRecordIterator()
        {
            if (indexIt >= iterator.Count)
                return null;

            var current = iterator[indexIt];
            ++indexIt;
            CurrentTime = current.Key;

            if (allKeys)
                return new List<KeyValuePair<Bytes, byte[]>>(current.Value);
            
            return new List<KeyValuePair<Bytes, byte[]>>(
                current.Value.Where(kv => GetKey(kv.Key).CompareTo(GetKey(keyFrom)) >= 0 && GetKey(kv.Key).CompareTo(GetKey(keyTo)) <= 0));
        }

        protected void Reset()
        {
            if(disposed)
                throw new ObjectDisposedException("Enumerator was disposed");

            indexIt = 0;
            valueIndexIt = 0;
            next = null;
            valueIterator = null;
        }

        protected Bytes GetKey(Bytes wrappedKey)
        {
            if (retainDuplicate)
            {
                var buffer = ByteBuffer.Build(wrappedKey.Get, true);
                return Bytes.Wrap(buffer.GetBytes(0, wrappedKey.Get.Length - sizeof(Int32)));
            }

            return wrappedKey;
        }
    }

    internal class WrappedInMemoryWindowStoreEnumerator : InMemoryWindowStoreEnumeratorWrapper, IWindowStoreEnumerator<byte[]>
    {
        public WrappedInMemoryWindowStoreEnumerator(Bytes keyFrom, Bytes keyTo, List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> dataIterator, Func<InMemoryWindowStoreEnumeratorWrapper, bool> closingCallback, bool retainDuplicate)
            : base(keyFrom, keyTo, dataIterator, closingCallback, retainDuplicate)
        {
        }

        public KeyValuePair<long, byte[]>? Current
        {
            get
            {
                if (disposed)
                    throw new ObjectDisposedException("Enumerator was disposed");

                if (next.HasValue)
                    return new KeyValuePair<long, byte[]>(CurrentTime, next.Value.Value);
                else
                    return null;
            }
        }

        object IEnumerator.Current => Current;

        public void Dispose() => base.Close();

        public bool MoveNext() => HasNext;

        public long PeekNextKey()
        {
            throw new NotImplementedException();
        }

        public new void Reset() => base.Reset();
    }

    internal class WrappedWindowedKeyValueEnumerator : InMemoryWindowStoreEnumeratorWrapper, IKeyValueEnumerator<Windowed<Bytes>, byte[]>
    {
        private readonly long windowSize;

        public WrappedWindowedKeyValueEnumerator(Bytes keyFrom, Bytes keyTo, List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> dataIterator, Func<InMemoryWindowStoreEnumeratorWrapper, bool> closingCallback, long windowSize, bool retainDuplicate)
            : base(keyFrom, keyTo, dataIterator, closingCallback, retainDuplicate)
        {
            this.windowSize = windowSize;
        }

        public KeyValuePair<Windowed<Bytes>, byte[]>? Current
        {
            get
            {
                if (disposed)
                    throw new ObjectDisposedException("Enumerator was disposed");

                if (next.HasValue)
                {
                    return new KeyValuePair<Windowed<Bytes>, byte[]>(GetWindowedKey(), next.Value.Value);
                }
                else
                {
                    return null;
                }
            }
        }

        private Windowed<Bytes> GetWindowedKey()
        {
            TimeWindow timeWindow = new TimeWindow(CurrentTime, CurrentTime + windowSize);
            return new Windowed<Bytes>(retainDuplicate ? GetKey(next.Value.Key) : next.Value.Key, timeWindow);
        }

        object IEnumerator.Current => Current;

        public void Dispose() => base.Close();

        public bool MoveNext() => HasNext;

        public Windowed<Bytes> PeekNextKey()
        {
            throw new NotImplementedException();
        }

        public new void Reset() => base.Reset();
    }

    #endregion

    internal class InMemoryWindowStore : IWindowStore<Bytes, byte[]>
    {
        private readonly TimeSpan retention;
        private readonly long size;
        private readonly bool retainDuplicates;
        private Sensor expiredRecordSensor;
        private ProcessorContext context;

        private long observedStreamTime = -1;
        private int seqnum = 0;

        private readonly ConcurrentDictionary<long, ConcurrentDictionary<Bytes, byte[]>> map = new();
        
        private readonly ISet<InMemoryWindowStoreEnumeratorWrapper> openIterators = new HashSet<InMemoryWindowStoreEnumeratorWrapper>();

        private readonly ILogger logger = Logger.GetLogger(typeof(InMemoryWindowStore));

        public InMemoryWindowStore(string storeName, TimeSpan retention, long size, bool retainDuplicates)
        {
            Name = storeName;
            this.retention = retention;
            this.size = size;
            this.retainDuplicates = retainDuplicates;
        }

        public string Name { get; }

        public bool Persistent => false;
        public bool IsLocally => true;

        public bool IsOpen { get; private set; } = false;

        private void UpdateSeqNumber()
        {
            if(retainDuplicates)
                seqnum = (seqnum + 1) & 0x7FFFFFFF;
        }
        
        public virtual IKeyValueEnumerator<Windowed<Bytes>, byte[]> All()
        {
            RemoveExpiredData();
            long minTime = observedStreamTime - (long)retention.TotalMilliseconds;
            return CreateNewWindowedKeyValueEnumerator(null, null, Tail(minTime));
        }

        public void Close()
        {
            if (openIterators.Count != 0)
            {
                logger.LogWarning("Closing {OpenIteratorCount} open iterators for store {Name}", openIterators.Count, Name);
                for (int i = 0; i< openIterators.Count; ++i)
                    openIterators.ElementAt(i).Close();
            }

            map.Clear();
            IsOpen = false;
        }

        public virtual byte[] Fetch(Bytes key, long time)
        {
            RemoveExpiredData();

            if (time <= observedStreamTime - retention.TotalMilliseconds)
                return null;

            if (map.ContainsKey(time))
            {
                var keyFrom = retainDuplicates ? WrapWithSeq(key, 0) : key;
                var keyTo = retainDuplicates ? WrapWithSeq(key, Int32.MaxValue) : key;
                return map[time]
                    .FirstOrDefault(kv => kv.Key.CompareTo(keyFrom) >= 0 && kv.Key.CompareTo(keyTo) <= 0).Value;
            }
            else
                return null;
        }

        public virtual IWindowStoreEnumerator<byte[]> Fetch(Bytes key, DateTime from, DateTime to)
            => Fetch(key, from.GetMilliseconds(), to.GetMilliseconds());

        public virtual IWindowStoreEnumerator<byte[]> Fetch(Bytes key, long from, long to)
        {
            RemoveExpiredData();

            long minTime = Math.Max(from, observedStreamTime - (long)retention.TotalMilliseconds + 1);


            if (to < minTime)
            {
                return new EmptyWindowStoreEnumerator<byte[]>();
            }

            return CreateNewWindowStoreEnumerator(key, SubMap(minTime, to));
        }

        public virtual IKeyValueEnumerator<Windowed<Bytes>, byte[]> FetchAll(DateTime from, DateTime to)
        {
            RemoveExpiredData();

            long minTime = Math.Max(from.GetMilliseconds(), observedStreamTime - (long)retention.TotalMilliseconds + 1);

            if (to.GetMilliseconds() < minTime)
            { 
                return new EmptyKeyValueEnumerator<Windowed<Bytes>, byte[]>();
            }

            return CreateNewWindowedKeyValueEnumerator(null, null, SubMap(minTime, to.GetMilliseconds()));
        }

        public virtual void Flush()
        {
        }

        public void Init(ProcessorContext context, IStateStore root)
        {
            expiredRecordSensor = TaskMetrics.DroppedRecordsSensor(
                Thread.CurrentThread.Name,
                context.Id,
                context.Metrics);
            this.context = context;
            
            if (root != null)
            {
                // register the store
                context.Register(root,
                    (key, value, timestamp) =>
                    {
                        Put(
                            Bytes.Wrap(WindowKeyHelper.ExtractStoreKeyBytes(key.Get)),
                            value,
                            WindowKeyHelper.ExtractStoreTimestamp(key.Get));
                    });
            }

            IsOpen = true;
        }

        public virtual void Put(Bytes key, byte[] value, long windowStartTimestamp)
        {
            observedStreamTime = Math.Max(observedStreamTime, windowStartTimestamp);

            if (windowStartTimestamp <= observedStreamTime - retention.TotalMilliseconds)
            {
                expiredRecordSensor.Record(1.0, context.Timestamp);
                logger.LogWarning("Skipping record for expired segment");
            }
            else
            {
                if (value != null)
                {
                    UpdateSeqNumber();
                    var keyBytes = retainDuplicates ? WrapWithSeq(key, seqnum) : key;
                    map.AddOrUpdate(windowStartTimestamp,
                        (k) =>
                        {
                            var dic = new ConcurrentDictionary<Bytes, byte[]>();
                            dic.AddOrUpdate(keyBytes, (b) => value, (k, d) => value);
                            return dic;
                        },
                        (k, d) =>
                        {
                            d.AddOrUpdate(keyBytes, (b) => value, (k, d) => value);
                            return d;
                        });
                }
                else if(!retainDuplicates)
                {
                    // Skip if value is null and duplicates are allowed since this delete is a no-op
                    if (map.ContainsKey(windowStartTimestamp))
                    {
                        ConcurrentDictionary<Bytes, byte[]> tmp = null;
                        byte[] d = null;
                        map[windowStartTimestamp].TryRemove(key, out d);
                        if (map[windowStartTimestamp].Count == 0)
                            map.TryRemove(windowStartTimestamp, out tmp);
                    }
                }
            }
        }

        #region Private

        private void RemoveExpiredData()
        {
            ConcurrentDictionary<Bytes, byte[]> tmp;
            long minLiveTime = Math.Max(0L, observedStreamTime - (long)retention.TotalMilliseconds + 1);
            foreach (var it in openIterators)
            {
                minLiveTime = Math.Min(minLiveTime, it.CurrentTime);
                it.RemoveExpiredData(minLiveTime);
            }

            foreach (var k in map.Keys.ToList())
            {
                if (k < minLiveTime)
                    map.TryRemove(k, out tmp);
            }
        }

        private List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> SubMap(long time1, long time2)
            => map.Where(kv => time1 <= kv.Key && time2 >= kv.Key).OrderBy(kp => kp.Key).ToList();

        private List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> Tail(long time)
            => map.Where(kv => kv.Key > time).OrderBy(kp => kp.Key).ToList();

        private IWindowStoreEnumerator<byte[]> CreateNewWindowStoreEnumerator(Bytes key, List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> enumerator)
        {
            var keyFrom = retainDuplicates ? WrapWithSeq(key, 0) : key;
            var keyTo = retainDuplicates ? WrapWithSeq(key, Int32.MaxValue) : key;
            
            var it = new WrappedInMemoryWindowStoreEnumerator(keyFrom, keyTo, enumerator, openIterators.Remove, retainDuplicates);
            openIterators.Add(it);
            return it;
        }

        private WrappedWindowedKeyValueEnumerator CreateNewWindowedKeyValueEnumerator(Bytes keyFrom, Bytes keyTo, List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> enumerator)
        {
            var from = retainDuplicates && keyFrom != null ? WrapWithSeq(keyFrom, 0) : keyFrom;
            var to = retainDuplicates && keyTo != null ? WrapWithSeq(keyTo, Int32.MaxValue) : keyTo;
            
            var it =
                  new WrappedWindowedKeyValueEnumerator(keyFrom,
                                                      keyTo,
                                                      enumerator,
                                                      openIterators.Remove,
                                                      size, 
                                                      retainDuplicates);
            openIterators.Add(it);
            return it;
        }

        private Bytes WrapWithSeq(Bytes key, int seq)
        {
            var buffer = ByteBuffer.Build(key.Get.Length + sizeof(Int32), true);
            buffer.Put(key.Get);
            buffer.PutInt(seq);
            return Bytes.Wrap(buffer.ToArray());
        }
        
        #endregion
    }
}