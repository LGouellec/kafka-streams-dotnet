using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Helper;
using Streamiz.Kafka.Net.State.Internal;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.State.RocksDb.Internal
{
    /// <summary>
    /// Like JAVA Implementation, no advantages to rewrite
    /// </summary>
    internal class RocksDbWindowKeySchema : IKeySchema
    {
        private static readonly IComparer<Bytes> bytesComparer = new BytesComparer();

        public Func<IKeyValueEnumerator<Bytes, byte[]>, bool> HasNextCondition(Bytes binaryKeyFrom, Bytes binaryKeyTo, long from, long to)
        {
            return (enumerator) =>
            {
                while (enumerator.MoveNext())
                {
                    var bytes = enumerator.PeekNextKey();
                    Bytes keyBytes = Bytes.Wrap(WindowKeyHelper.ExtractStoreKeyBytes(bytes.Get));
                    long time = WindowKeyHelper.ExtractStoreTimestamp(bytes.Get);
                    if ((binaryKeyFrom == null || bytesComparer.Compare(keyBytes, binaryKeyFrom) >= 0)
                        && (binaryKeyTo == null || bytesComparer.Compare(keyBytes, binaryKeyTo) <= 0)
                        && time >= from
                        && time <= to)
                        return true;
                        
                }
                return false;
            };
        }

        public Bytes LowerRange(Bytes key, long from)
            => OrderedBytes.LowerRange(key, WindowKeyHelper.MIN_SUFFIX);

        public Bytes LowerRangeFixedSize(Bytes key, long from)
            => WindowKeyHelper.ToStoreKeyBinary(key, Math.Max(0, from), 0);

        public IList<S> SegmentsToSearch<S>(ISegments<S> segments, long from, long to, bool forward) where S : ISegment
            => segments.Segments(from, to, forward).ToList();

        public long SegmentTimestamp(Bytes key)
            => WindowKeyHelper.ExtractStoreTimestamp(key.Get);

        public Bytes UpperRange(Bytes key, long to)
        {
            byte[] maxSuffix = ByteBuffer.Build(WindowKeyHelper.SUFFIX_SIZE)
            .PutLong(to)
            .PutInt(int.MaxValue)
            .ToArray();

            return OrderedBytes.UpperRange(key, maxSuffix);
        }

        public Bytes UpperRangeFixedSize(Bytes key, long to)
            => WindowKeyHelper.ToStoreKeyBinary(key, to, Int32.MaxValue);
    }
}