using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;

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

                if ((keyFrom == null && keyTo == null) || (next.Value.Key.Equals(keyFrom) || next.Value.Key.Equals(keyTo)))
                    return true;
                else
                {
                    next = null;
                    return HasNext;
                }
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
            Func<InMemoryWindowStoreEnumeratorWrapper, bool> closingCallback)
        {
            this.keyFrom = keyFrom;
            this.keyTo = keyTo;
            allKeys = keyFrom == null && keyTo == null;
            iterator = dataIterator;
            this.closingCallback = closingCallback;
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
            else
                throw new ObjectDisposedException("Enumerator was disposed");
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
            else
                return new List<KeyValuePair<Bytes, byte[]>>(current.Value.Where(kv => kv.Key.Equals(keyFrom) || kv.Key.Equals(keyTo)));
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
    }

    internal class WrappedInMemoryWindowStoreEnumerator : InMemoryWindowStoreEnumeratorWrapper, IWindowStoreEnumerator<byte[]>
    {
        public WrappedInMemoryWindowStoreEnumerator(Bytes keyFrom, Bytes keyTo, List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> dataIterator, Func<InMemoryWindowStoreEnumeratorWrapper, bool> closingCallback)
            : base(keyFrom, keyTo, dataIterator, closingCallback)
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

        public WrappedWindowedKeyValueEnumerator(Bytes keyFrom, Bytes keyTo, List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> dataIterator, Func<InMemoryWindowStoreEnumeratorWrapper, bool> closingCallback, long windowSize)
            : base(keyFrom, keyTo, dataIterator, closingCallback)
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
            return new Windowed<Bytes>(next.Value.Key, timeWindow);
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

        private long observedStreamTime = -1;

        private readonly ConcurrentDictionary<long, ConcurrentDictionary<Bytes, byte[]>> map =
            new ConcurrentDictionary<long, ConcurrentDictionary<Bytes, byte[]>>();
        
        private readonly ISet<InMemoryWindowStoreEnumeratorWrapper> openIterators = new HashSet<InMemoryWindowStoreEnumeratorWrapper>();

        private readonly ILogger logger = Logger.GetLogger(typeof(InMemoryWindowStore));

        public InMemoryWindowStore(string storeName, TimeSpan retention, long size)
        {
            Name = storeName;
            this.retention = retention;
            this.size = size;
        }

        public string Name { get; }

        public bool Persistent => false;

        public bool IsOpen { get; private set; } = false;

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> All()
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

        public byte[] Fetch(Bytes key, long time)
        {
            RemoveExpiredData();

            if (time <= observedStreamTime - retention.TotalMilliseconds)
                return null;

            if (map.ContainsKey(time))
                return map[time].Get(key);
            else
                return null;
        }

        public IWindowStoreEnumerator<byte[]> Fetch(Bytes key, DateTime from, DateTime to)
            => Fetch(key, from.GetMilliseconds(), to.GetMilliseconds());

        public IWindowStoreEnumerator<byte[]> Fetch(Bytes key, long from, long to)
        {
            RemoveExpiredData();

            long minTime = Math.Max(from, observedStreamTime - (long)retention.TotalMilliseconds + 1);


            if (to < minTime)
            {
                return new EmptyWindowStoreEnumerator<byte[]>();
            }

            return CreateNewWindowStoreEnumerator(key, SubMap(minTime, to));
        }

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> FetchAll(DateTime from, DateTime to)
        {
            RemoveExpiredData();

            long minTime = Math.Max(from.GetMilliseconds(), observedStreamTime - (long)retention.TotalMilliseconds + 1);

            if (to.GetMilliseconds() < minTime)
            { 
                return new EmptyKeyValueEnumerator<Windowed<Bytes>, byte[]>();
            }

            return CreateNewWindowedKeyValueEnumerator(null, null, SubMap(minTime, to.GetMilliseconds()));
        }

        public void Flush()
        {
        }

        public void Init(ProcessorContext context, IStateStore root)
        {
            if (root != null)
            {
                // register the store
                context.Register(root,
                    (key, value) => Put(key, value, context.Timestamp));
            }

            IsOpen = true;
        }

        public void Put(Bytes key, byte[] value, long windowStartTimestamp)
        {
            RemoveExpiredData();

            observedStreamTime = Math.Max(observedStreamTime, windowStartTimestamp);

            if (windowStartTimestamp <= observedStreamTime - retention.TotalMilliseconds)
            {
                logger.LogWarning("Skipping record for expired segment");
            }
            else
            {
                if (value != null)
                {
                    map.AddOrUpdate(windowStartTimestamp,
                        (k) =>
                        {
                            var dic = new ConcurrentDictionary<Bytes, byte[]>();
                            dic.AddOrUpdate(key, (b) => value, (k, d) => value);
                            return dic;
                        },
                        (k, d) =>
                        {
                            d.AddOrUpdate(key, (b) => value, (k, d) => value);
                            return d;
                        });
                }
                else
                {
                    if (map.ContainsKey(windowStartTimestamp))
                    {
                        ConcurrentDictionary<Bytes, byte[]> tmp = null;
                        byte[] d = null;
                        map[windowStartTimestamp].Remove(key, out d);
                        if (map[windowStartTimestamp].Count == 0)
                            map.Remove(windowStartTimestamp, out tmp);
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
                    map.Remove(k, out tmp);
            }
        }

        private List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> SubMap(long time1, long time2)
            => map.Where(kv => time1 <= kv.Key && time2 >= kv.Key).OrderBy(kp => kp.Key).ToList();

        private List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> Tail(long time)
            => map.Where(kv => kv.Key > time).OrderBy(kp => kp.Key).ToList();

        private IWindowStoreEnumerator<byte[]> CreateNewWindowStoreEnumerator(Bytes key, List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> enumerator)
        {
            var it = new WrappedInMemoryWindowStoreEnumerator(key, key, enumerator, openIterators.Remove);
            openIterators.Add(it);
            return it;
        }

        private WrappedWindowedKeyValueEnumerator CreateNewWindowedKeyValueEnumerator(Bytes keyFrom, Bytes keyTo, List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> enumerator)
        {
            var it =
                  new WrappedWindowedKeyValueEnumerator(keyFrom,
                                                      keyTo,
                                                      enumerator,
                                                      openIterators.Remove,
                                                      size);
            openIterators.Add(it);
            return it;
        }

        #endregion
    }
}