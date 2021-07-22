using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal abstract class AbstractSegments<S> : ISegments<S>
        where S : ISegment
    {
        protected readonly ILogger logger;
        protected readonly SortedDictionary<long, S> segments = new SortedDictionary<long, S>(new LongComparer());

        protected readonly string name;
        private readonly long segmentInterval;
        private readonly long retentionPeriod;

        protected AbstractSegments(string name, long retentionPeriod, long segmentInterval)
        {
            logger = Logger.GetLogger(GetType());
            this.name = name;
            this.segmentInterval = segmentInterval;
            this.retentionPeriod = retentionPeriod;
        }

        public abstract S GetOrCreateSegment(long segmentId, ProcessorContext context);

        #region ISegments Impl

        public IEnumerable<S> AllSegments(bool forward)
        {
            var _enumerable = forward ?
                segments.Values.AsEnumerable() :
                segments.Values.Reverse().AsEnumerable();

            return _enumerable.Where(v => v.IsOpen);
        }

        public void Close()
        {
            foreach (var s in segments.Values)
            {
                s.Close();
            }

            segments.Clear();
        }

        public void Flush()
        {
            foreach (var s in segments.Values)
            {
                s.Flush();
            }
        }
        public S GetOrCreateSegmentIfLive(long segmentId, ProcessorContext context, long streamTime)
        {
            long minLiveTimestamp = streamTime - retentionPeriod;
            long minLiveSegment = SegmentId(minLiveTimestamp);

            S toReturn;
            if (segmentId >= minLiveSegment)
            {
                // The segment is live. get it, ensure it's open, and return it.
                toReturn = GetOrCreateSegment(segmentId, context);
            }
            else
            {
                toReturn = default(S);
            }

            CleanupEarlierThan(minLiveSegment);
            return toReturn;
        }

        public S GetSegmentForTimestamp(long timestamp)
        {
            long key = SegmentId(timestamp);
            return segments.ContainsKey(key) ? segments[key] : default(S);
        }

        public void OpenExisting(ProcessorContext context, long streamTime)
        {
            try
            {
                var directory = Directory.CreateDirectory(Path.Combine(context.StateDir, name));
                directory
                    .GetFiles()
                    .Select(s => SegmentIdFromSegmentName(s.Name))
                    .OrderBy(l => l)
                    .ToList()
                    .ForEach(segId => GetOrCreateSegment(segId, context));
            }
            catch (Exception ex) {
                throw new ProcessorStateException($"{Path.Combine(context.StateDir, name)}  doesn't exist and cannot be created for segments {name}");
            }

            long minLiveSegment = SegmentId(streamTime - retentionPeriod);
            CleanupEarlierThan(minLiveSegment);
        }

        public long SegmentId(long timestamp)
            => timestamp / segmentInterval;

        public string SegmentName(long segmentId)
            => $"{name}.{segmentId * segmentInterval}";

        public IEnumerable<S> Segments(long timeFrom, long timeTo, bool forward)
        {
            var _segs = segments.SubMap(SegmentId(timeFrom), SegmentId(timeTo), true, true);
            _segs = forward ? _segs : _segs.Reverse();
            return _segs.Select(s => s.Value).Where(s => s.IsOpen);
        }
        
        #endregion

        private void CleanupEarlierThan(long minLiveSegment)
        {
            var _segs = segments.HeadMap(minLiveSegment, false).ToList();

            foreach(var kv in _segs)
            {
                segments.Remove(kv.Key);
                try
                {
                    kv.Value.Close();
                    kv.Value.Destroy();
                }catch (Exception e)
                {
                    logger.LogError(e, "Error destroying state store {Name}", kv.Value.Name);
                }
            }
        } 

        private long SegmentIdFromSegmentName(string segmentName)
        {
            int segmentSeparatorIndex = name.Length;
            char segmentSeparator = segmentName[segmentSeparatorIndex];
            string segmentIdString = segmentName.Substring(segmentSeparatorIndex + 1);
            long segmentId;

            try
            {
                segmentId = long.Parse(segmentIdString) / segmentInterval;
            }
            catch (FormatException)
            {
                throw new ProcessorStateException($"Unable to parse segment id as long from segmentName: {segmentName}");
            }

            return segmentId;
        }
    }
}