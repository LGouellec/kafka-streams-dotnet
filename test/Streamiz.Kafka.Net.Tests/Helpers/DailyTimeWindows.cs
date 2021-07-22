using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.Tests.Helpers
{
    public class DailyTimeWindows : WindowOptions<TimeWindow>
    {
        private readonly TimeZoneInfo zone;
        private readonly int startHour;

        public override long Size => (long)TimeSpan.FromDays(1).TotalMilliseconds;

        public override long GracePeriodMs { get; }


        public DailyTimeWindows(TimeZoneInfo zone, int startHour, TimeSpan grace)
        {
            this.zone = zone;
            this.startHour = startHour;
            this.GracePeriodMs = (long)grace.TotalMilliseconds;
        }

        public override IDictionary<long, TimeWindow> WindowsFor(long timestamp)
        {
            var zoneDate = TimeZoneInfo.ConvertTime(timestamp.FromMilliseconds(), zone);
            var startTime = zoneDate.Hour >= startHour ?
                zoneDate.TruncateUnitDays().AddHours(startHour) :
                zoneDate.TruncateUnitDays().AddDays(-1).AddHours(startHour);
            var endTime = startTime.AddDays(1);

            var windows = new Dictionary<long, TimeWindow>();
            windows.Add(startTime.GetMilliseconds(), new TimeWindow(startTime.GetMilliseconds(), endTime.GetMilliseconds()));
            return windows;
        }
    }
}
