using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal interface ISegments<S>
        where S : ISegment
    {
        long SegmentId(long timestamp);
        string SegmentName(long segmentId);
        S GetSegmentForTimestamp(long timestamp);
        S GetOrCreateSegmentIfLive(long segmentId, ProcessorContext context, long streamTime);
        S GetOrCreateSegment(long segmentId, ProcessorContext context);
        void OpenExisting(ProcessorContext context, long streamTime);
        IEnumerable<S> Segments(long timeFrom, long timeTo, bool forward);
        IEnumerable<S> AllSegments(bool forward);
        void Flush();
        void Close();
    }
}
