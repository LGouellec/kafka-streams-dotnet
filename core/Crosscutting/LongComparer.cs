using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Crosscutting
{
    internal class LongComparer : IComparer<long>
    {
        public int Compare(long x, long y)
            => x.CompareTo(y);
    }
}
