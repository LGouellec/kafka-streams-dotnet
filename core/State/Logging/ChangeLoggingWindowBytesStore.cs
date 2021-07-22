using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Helper;
using Streamiz.Kafka.Net.State.Internal;
using System;

namespace Streamiz.Kafka.Net.State.Logging
{
    internal class ChangeLoggingWindowBytesStore :
        WrappedStateStore<IWindowStore<Bytes, byte[]>>,
        IWindowStore<Bytes, byte[]>
    {
        public ChangeLoggingWindowBytesStore(IWindowStore<Bytes, byte[]> wrapped) 
            : base(wrapped)
        {
        }

        protected virtual void Publish(Bytes key, byte[] value)
            => context.Log(Name, key, value, context.RecordContext.Timestamp);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> All()
            => wrapped.All();

        public byte[] Fetch(Bytes key, long time)
            => wrapped.Fetch(key, time);

        public IWindowStoreEnumerator<byte[]> Fetch(Bytes key, DateTime from, DateTime to)
            => wrapped.Fetch(key, from, to);

        public IWindowStoreEnumerator<byte[]> Fetch(Bytes key, long from, long to)
            => wrapped.Fetch(key, from, to);

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> FetchAll(DateTime from, DateTime to)
            => wrapped.FetchAll(from, to);

        public void Put(Bytes key, byte[] value, long windowStartTimestamp)
        {
            wrapped.Put(key, value, windowStartTimestamp);
            Publish(WindowKeyHelper.ToStoreKeyBinary(key, windowStartTimestamp, 0), value);
        }
    }
}