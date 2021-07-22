using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Tests.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using TimeZoneConverter;
using System.Runtime.InteropServices;

namespace Streamiz.Kafka.Net.Tests.Public
{
    internal class MySerDes : TimeWindowedSerDes<int>
    {
        public MySerDes()
            : base(new Int32SerDes(), (long)TimeSpan.FromDays(1).TotalMilliseconds)
        {
        }
    }

    /// <summary>
    /// Unit test inspired by @jeanlouisboudart
    /// See : https://github.com/jeanlouisboudart/kafka-streams-examples/blob/timestamp-sync-edge-case/src/test/java/io/confluent/examples/streams/window/CustomWindowTest.java
    /// </summary>
    /// TODO : Refactor when supress processor will be done
    public class CustomWindowTests
    {
        private static readonly string inputTopic = "inputTopic";
        private static readonly string outputTopic = "outputTopic";
        private static readonly TimeZoneInfo zone = TimeZoneInfo.Utc;
        private static readonly int windowStartHour = 18;


        [Test]
        public void ShouldSumNumbersOnSameDay()
        {
            var inputRecords = new List<TestRecord<string, int>>{
                new TestRecord<string, int>(null, 1, TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 1, 16, 29, 0, DateTimeKind.Utc), zone)),
                new TestRecord<string, int>(null, 2, TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 1, 16, 30, 0, DateTimeKind.Utc), zone)),
                new TestRecord<string, int>(null, 7, TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 1, 16, 31, 0, DateTimeKind.Utc), zone))
            };

            var expected = new List<(Windowed<int>, int)>
            {
                (ToWindowed(1, TimeZoneInfo.ConvertTime(new DateTime(2018, 12, 31, 18, 0, 0, DateTimeKind.Utc), zone), TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 1, 18, 0, 0, DateTimeKind.Utc), zone)), 1),
                (ToWindowed(1, TimeZoneInfo.ConvertTime(new DateTime(2018, 12, 31, 18, 0, 0, DateTimeKind.Utc), zone), TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 1, 18, 0, 0, DateTimeKind.Utc), zone)), 3),
                (ToWindowed(1, TimeZoneInfo.ConvertTime(new DateTime(2018, 12, 31, 18, 0, 0, DateTimeKind.Utc), zone), TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 1, 18, 0, 0, DateTimeKind.Utc), zone)), 10),
            };

            Verify(inputRecords, expected, zone);
        }

        [Test]
        public void ShouldSumNumbersWithTwoWindows()
        {
            var inputRecords = new List<TestRecord<string, int>>{
                new TestRecord<string, int>(null, 1, TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 1, 16, 29, 0, DateTimeKind.Utc), zone)),
                new TestRecord<string, int>(null, 2, TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 1, 16, 30, 0, DateTimeKind.Utc), zone)),
                new TestRecord<string, int>(null, 7, TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 1, 19, 31, 0, DateTimeKind.Utc), zone))
            };

            var expected = new List<(Windowed<int>, int)>
            {
                (ToWindowed(1, TimeZoneInfo.ConvertTime(new DateTime(2018, 12, 31, 18, 0, 0, DateTimeKind.Utc), zone), TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 1, 18, 0, 0, DateTimeKind.Utc), zone)), 1),
                (ToWindowed(1, TimeZoneInfo.ConvertTime(new DateTime(2018, 12, 31, 18, 0, 0, DateTimeKind.Utc), zone), TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 1, 18, 0, 0, DateTimeKind.Utc), zone)), 3),
                (ToWindowed(1, TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 1, 18, 0, 0, DateTimeKind.Utc), zone), TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 2, 18, 0, 0, DateTimeKind.Utc), zone)), 7)
            };

            Verify(inputRecords, expected, zone);
        }

        [Test]
        public void ShouldSumNumbersWithTwoWindowsAndLateArrival()
        {
            var inputRecords = new List<TestRecord<string, int>>{
                new TestRecord<string, int>(null, 1, TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 1, 16, 29, 0, DateTimeKind.Utc), zone)),
                new TestRecord<string, int>(null, 2, TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 1, 16, 30, 0, DateTimeKind.Utc), zone)),
                new TestRecord<string, int>(null, 2, TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 1, 18, 1, 0, DateTimeKind.Utc), zone)),
                //Out-of-order arrival message
                new TestRecord<string, int>(null, 7, TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 1, 16, 31, 0, DateTimeKind.Utc), zone)),
                new TestRecord<string, int>(null, 40, TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 1, 18, 31, 0, DateTimeKind.Utc), zone)),
                //this late arrival event should be ignored as it happens after a message that was outside of grace period 18h (end of window) + 90min (grace period)
                new TestRecord<string, int>(null, 42, TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 1, 16, 35, 0, DateTimeKind.Utc), zone))
            };

            var expected = new List<(Windowed<int>, int)>
            {
                (ToWindowed(1, TimeZoneInfo.ConvertTime(new DateTime(2018, 12, 31, 18, 0, 0, DateTimeKind.Utc), zone), TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 1, 18, 0, 0, DateTimeKind.Utc), zone)), 1),
                (ToWindowed(1, TimeZoneInfo.ConvertTime(new DateTime(2018, 12, 31, 18, 0, 0, DateTimeKind.Utc), zone), TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 1, 18, 0, 0, DateTimeKind.Utc), zone)), 3),
                (ToWindowed(1, TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 1, 18, 0, 0, DateTimeKind.Utc), zone), TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 2, 18, 0, 0, DateTimeKind.Utc), zone)), 2),
                (ToWindowed(1, TimeZoneInfo.ConvertTime(new DateTime(2018, 12, 31, 18, 0, 0, DateTimeKind.Utc), zone), TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 1, 18, 0, 0, DateTimeKind.Utc), zone)), 10),
                (ToWindowed(1, TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 1, 18, 0, 0, DateTimeKind.Utc), zone), TimeZoneInfo.ConvertTime(new DateTime(2019, 1, 2, 18, 0, 0, DateTimeKind.Utc), zone)), 42)
            };

            Verify(inputRecords, expected, zone);
        }

        [Test]
        public void ShouldSumNumbersWithTwoWindowsAndNoDSTTimezone()
        {
            var inputRecords = new List<TestRecord<string, int>>{
                new TestRecord<string, int>(null, 1, TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 30, 1, 39, 0, DateTimeKind.Utc), zone)),
                new TestRecord<string, int>(null, 2, TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 30, 3, 0, 0, DateTimeKind.Utc), zone)),
                new TestRecord<string, int>(null, 7, TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 30, 3, 10, 0, DateTimeKind.Utc), zone)),
                new TestRecord<string, int>(null, 1, TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 31, 1, 39, 0, DateTimeKind.Utc), zone)),
                new TestRecord<string, int>(null, 2, TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 31, 3, 0, 0, DateTimeKind.Utc), zone)),
                new TestRecord<string, int>(null, 7, TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 31, 3, 10, 0, DateTimeKind.Utc), zone)),
            };

            var expected = new List<(Windowed<int>, int)>
            {
                (ToWindowed(1, TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 29, 18, 0, 0, DateTimeKind.Utc), zone), TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 30, 18, 0, 0, DateTimeKind.Utc), zone)), 1),
                (ToWindowed(1, TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 29, 18, 0, 0, DateTimeKind.Utc), zone), TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 30, 18, 0, 0, DateTimeKind.Utc), zone)), 3),
                (ToWindowed(1, TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 29, 18, 0, 0, DateTimeKind.Utc), zone), TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 30, 18, 0, 0, DateTimeKind.Utc), zone)), 10),
                (ToWindowed(1, TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 30, 18, 0, 0, DateTimeKind.Utc), zone), TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 31, 18, 0, 0, DateTimeKind.Utc), zone)), 1),
                (ToWindowed(1, TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 30, 18, 0, 0, DateTimeKind.Utc), zone), TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 31, 18, 0, 0, DateTimeKind.Utc), zone)), 3),
                (ToWindowed(1, TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 30, 18, 0, 0, DateTimeKind.Utc), zone), TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 31, 18, 0, 0, DateTimeKind.Utc), zone)), 10)
            };

            Verify(inputRecords, expected, zone);
        }

        [Test]
        public void ShouldSumNumbersWithTwoWindowsAndDSTTimezone()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                // This test illustrate problems with daylight savings
                //Some timezone have daylight savings time (DST) resulting in two days in year that have either 23 or 25 hours.
                //Kafka streams currently support only fixed period for the moment.
                TimeZoneInfo zoneWithDST = TZConvert.GetTimeZoneInfo("Europe/Paris");

                var inputRecords = new List<TestRecord<string, int>>{
                new TestRecord<string, int>(null, 1, TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 30, 1, 39, 0, DateTimeKind.Unspecified), zoneWithDST)),
                new TestRecord<string, int>(null, 2, TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 30, 3, 0, 0, DateTimeKind.Unspecified), zoneWithDST)),
                new TestRecord<string, int>(null, 7, TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 30, 3, 10, 0, DateTimeKind.Unspecified), zoneWithDST)),
                new TestRecord<string, int>(null, 1, TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 31, 1, 39, 0, DateTimeKind.Unspecified), zoneWithDST)),
                new TestRecord<string, int>(null, 2, TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 31, 3, 0, 0, DateTimeKind.Unspecified), zoneWithDST)),
                new TestRecord<string, int>(null, 7, TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 31, 3, 10, 0, DateTimeKind.Unspecified), zoneWithDST)),
            };

                var expected = new List<(Windowed<int>, int)>
            {
                (ToWindowed(1, TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 29, 18, 0, 0, DateTimeKind.Unspecified), zoneWithDST), TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 30, 18, 0, 0, DateTimeKind.Unspecified), zoneWithDST)), 1),
                (ToWindowed(1, TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 29, 18, 0, 0, DateTimeKind.Unspecified), zoneWithDST), TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 30, 18, 0, 0, DateTimeKind.Unspecified), zoneWithDST)), 3),
                (ToWindowed(1, TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 29, 18, 0, 0, DateTimeKind.Unspecified), zoneWithDST), TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 30, 18, 0, 0, DateTimeKind.Unspecified), zoneWithDST)), 10),
                (ToWindowed(1, TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 30, 18, 0, 0, DateTimeKind.Unspecified), zoneWithDST), TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 31, 19, 0, 0, DateTimeKind.Unspecified), zoneWithDST)), 1),
                (ToWindowed(1, TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 30, 18, 0, 0, DateTimeKind.Unspecified), zoneWithDST), TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 31, 19, 0, 0, DateTimeKind.Unspecified), zoneWithDST)), 3),
                (ToWindowed(1, TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 30, 18, 0, 0, DateTimeKind.Unspecified), zoneWithDST), TimeZoneInfo.ConvertTime(new DateTime(2019, 3, 31, 19, 0, 0, DateTimeKind.Unspecified), zoneWithDST)), 10)
            };

                Verify(inputRecords, expected, zoneWithDST);
            }
        }

        #region Helpers

        private void Verify(List<TestRecord<string, int>> inputRecords, List<(Windowed<int>, int)> expectedValues, TimeZoneInfo zone)
        {
            var streamConfiguration = new StreamConfig<StringSerDes, Int32SerDes>();
            streamConfiguration.ApplicationId = "custom-window-test";

            var topo = GetTopo(zone);
            using (var driver = new TopologyTestDriver(topo, streamConfiguration))
            {
                var input = driver.CreateInputTopic<string, int>(inputTopic);
                var output = driver.CreateOuputTopic<Windowed<int>, int, MySerDes, Int32SerDes>(outputTopic);
                input.PipeInputs(inputRecords);
                var items = output.ReadKeyValueList().Select(c => (c.Message.Key, c.Message.Value)).ToList();
                Assert.AreEqual(expectedValues, items);
            }
        }

        private Topology GetTopo(TimeZoneInfo zone)
        {
            var builder = new StreamBuilder();
            var grace = TimeSpan.FromMinutes(30);

            var stream = builder.Stream<string, int>(inputTopic);
            var sumStream = stream
                .SelectKey((k, v) => 1)
                .GroupByKey()
                .WindowedBy(new DailyTimeWindows(zone, windowStartHour, grace))
                .Reduce(
                    (v1, v2) => v1 + v2,
                    InMemoryWindows<int, int>
                        .Create<Int32SerDes, Int32SerDes>()
                        .WithRetention(TimeSpan.FromDays(1) + grace))
                .ToStream();

            sumStream.To<MySerDes, Int32SerDes>(outputTopic);
            return builder.Build();
        }

        private Windowed<int> ToWindowed(int key,
                                         DateTime start,
                                         DateTime end)
        {
            return new Windowed<int>(key, new TimeWindow(start.GetMilliseconds(), end.GetMilliseconds()));
        }

        #endregion
    }
}
