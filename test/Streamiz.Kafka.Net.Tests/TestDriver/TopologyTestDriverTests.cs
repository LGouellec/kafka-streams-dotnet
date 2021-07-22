using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;

namespace Streamiz.Kafka.Net.Tests.TestDriver
{
    public class TopologyTestDriverTests
    {
        [Test]
        public void TestGetWindowStoreDoesntNotExist()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-window-stream";

            var builder = new StreamBuilder();

            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromSeconds(5)))
                .Count(InMemoryWindows<string, long>.As("count-store"));

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                DateTime dt = DateTime.Now;
                var input = driver.CreateInputTopic<string, string>("topic");
                input.PipeInput("test", "1", dt);
                var store = driver.GetWindowStore<string, long>("store");
                Assert.IsNull(store);
            }
        }

        [Test]
        public void TestGetWindowStoreIncorrectType()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-window-stream";

            var builder = new StreamBuilder();

            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromSeconds(5)))
                .Count(InMemoryWindows<string, long>.As("count-store"));

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                DateTime dt = DateTime.Now;
                var input = driver.CreateInputTopic<string, string>("topic");
                input.PipeInput("test", "1", dt);
                var store = driver.GetWindowStore<int, long>("count-store");
                Assert.IsNull(store);
            }
        }

        [Test]
        public void TestGetWindowStoreKeyValueStore()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-window-stream";

            var builder = new StreamBuilder();

            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .Count(InMemory<string, long>.As("count-store"));

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                DateTime dt = DateTime.Now;
                var input = driver.CreateInputTopic<string, string>("topic");
                input.PipeInput("test", "1", dt);
                var store = driver.GetWindowStore<string, long>("count-store");
                Assert.IsNull(store);
            }
        }
    }
}
