using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Helper;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State.RocksDb
{
    internal class WindowKeyBytesComparer : 
        IEqualityComparer<WindowKeyBytes>,
        IComparer<WindowKeyBytes>,
        IComparer<Bytes>
    {
        public bool Equals(WindowKeyBytes x, WindowKeyBytes y)
        {
            return x.Equals(y);
            
        }

        public int GetHashCode(WindowKeyBytes obj)
        {
            return obj.GetHashCode();
        }

        public int Compare(WindowKeyBytes x, WindowKeyBytes y)
        {
            var bytes1 = x.Get;
            var bytes2 = y.Get;
            using (var buffer1 = ByteBuffer.Build(bytes1))
            {
                using(var buffer2 = ByteBuffer.Build(bytes2))
                {
                    var key1 = buffer1.GetBytes(0, bytes1.Length - WindowKeyHelper.SUFFIX_SIZE);
                    var key2 = buffer2.GetBytes(0, bytes2.Length - WindowKeyHelper.SUFFIX_SIZE);
                    int compareKey = BytesComparer.Compare(key1, key2);
                    if (compareKey == 0)
                    {
                        long ts1 = buffer1.GetLong(bytes1.Length - WindowKeyHelper.SUFFIX_SIZE);
                        long ts2 = buffer2.GetLong(bytes2.Length - WindowKeyHelper.SUFFIX_SIZE);
                        int compareTs = ts1.CompareTo(ts2);
                        if (compareTs == 0)
                        {
                            int seq1 = buffer1.GetInt(bytes1.Length - WindowKeyHelper.TIMESTAMP_SIZE);
                            int seq2 = buffer2.GetInt(bytes2.Length - WindowKeyHelper.TIMESTAMP_SIZE);
                            return seq1.CompareTo(seq2);
                        }
                        else
                            return compareTs;
                    }
                    else
                        return compareKey;
                }
            }
        }

        public int Compare(Bytes x, Bytes y)
            => Compare(WindowKeyBytes.Wrap(x.Get), WindowKeyBytes.Wrap(y.Get));
    }

    public class WindowKeyBytes : Bytes
    {
        private WindowKeyBytes(byte[] bytes)
            : base(bytes)
        {

        }

        //public static WindowKeyBytes Wrap(byte[] key, long time, int seqnum)
        //{
        //    int totalSize = key.Length + sizeof(long) + sizeof(int);
        //    using (var buffer = ByteBuffer.Build(totalSize))
        //    {
        //        buffer.PutInt(key.Length);
        //        buffer.Put(key);
        //        buffer.PutLong(time);
        //        buffer.PutInt(seqnum);
        //        return new WindowKeyBytes(buffer.ToArray());
        //    }
        //}

        public static WindowKeyBytes Wrap(byte[] key)
        {
            if (key == null)
                return null;
            return new WindowKeyBytes(key);
        }
    }
}
