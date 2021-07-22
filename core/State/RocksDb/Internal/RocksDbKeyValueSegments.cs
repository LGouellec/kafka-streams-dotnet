using Streamiz.Kafka.Net.State.Internal;

namespace Streamiz.Kafka.Net.State.RocksDb.Internal
{
    internal class RocksDbKeyValueSegments : AbstractSegments<RocksDbKeyValueSegment>
    {
        public RocksDbKeyValueSegments(string name, long retention, long segmentInterval)
            : base(name, retention, segmentInterval)
        {

        }

        public override RocksDbKeyValueSegment GetOrCreateSegment(long segmentId, ProcessorContext context)
        {
            if (segments.ContainsKey(segmentId))
                return segments[segmentId];
            else
            {
                var segment = new RocksDbKeyValueSegment(
                    SegmentName(segmentId),
                    name,
                    segmentId);

                segments.Add(segmentId, segment);
                segment.OpenDB(context);
                return segment;
            }
        }
    }
}