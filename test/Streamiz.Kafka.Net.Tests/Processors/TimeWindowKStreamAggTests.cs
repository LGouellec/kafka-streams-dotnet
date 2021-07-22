using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Collections.Generic;
using System.Linq;


namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class TimeWindowKStreamAggTests
    {
        internal class StringTimeWindowedSerDes : TimeWindowedSerDes<string>
        {
            public StringTimeWindowedSerDes()
                : base(new StringSerDes(), (long)TimeSpan.FromSeconds(10).TotalMilliseconds)
            {

            }
        }

        [Test]
        public void WithNullMaterialize()
        {
            // CERTIFIED THAT SAME IF Materialize is null, a state store exist for count processor with a generated namestore
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            var serdes = new StringSerDes();

            config.ApplicationId = "test-window-count";

            var builder = new StreamBuilder();
            Materialized<string, int, IWindowStore<Bytes, byte[]>> m = null;

            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(2000))
                .Aggregate(
                        () => 0,
                        (k, v, agg) => Math.Max(v.Length, agg),
                        m);

            var topology = builder.Build();
            TaskId id = new TaskId { Id = 0, Partition = 0 };
            var processorTopology = topology.Builder.BuildTopology(id);

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(config.ToConsumerConfig(), null);

            
            var part = new TopicPartition("topic", 0);
            StreamTask task = new StreamTask(
                "thread-0",
                id,
                new List<TopicPartition> { part },
                processorTopology,
                consumer,
                config,
                supplier,
                null);
            task.GroupMetadata = consumer as SyncConsumer;
            Assert.Throws<StreamsException>(() => task.InitializeStateStores());
        }

        [Test]
        public void WithNullValueSerDes()
        {
            // WITH VALUE NULL SERDES, in running KeySerdes must be StringSerdes, and ValueSerdes Int64SerDes
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-window-count";

            var builder = new StreamBuilder();

            Materialized<string, int, IWindowStore<Bytes, byte[]>> m =
                Materialized<string, int, IWindowStore<Bytes, byte[]>>
                    .Create("count-store")
                    .With(null, null);

            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(2000))
                .Aggregate(
                        () => 0,
                        (k, v, agg) => Math.Max(v.Length, agg),
                        m)
                .ToStream()
                .To("output-topic");

            var topology = builder.Build();
            Assert.Throws<StreamsException>(() =>
            {
                using (var driver = new TopologyTestDriver(topology, config))
                {
                    var input = driver.CreateInputTopic<string, string>("topic");
                    var output = driver.CreateOuputTopic("output-topic", TimeSpan.FromSeconds(1), new StringTimeWindowedSerDes(), new Int64SerDes());
                    input.PipeInput("test", "1");
                }
            });
        }

        [Test]
        public void TimeWindowingAgg()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-window-stream";

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(20000))
                .Aggregate<int, Int32SerDes>(
                        () => 0,
                        (k, v, agg) => Math.Max(v.Length, agg))
                .ToStream()
                .To<StringTimeWindowedSerDes, Int32SerDes>("output");

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                var input = driver.CreateInputTopic<string, string>("topic");
                var output = driver.CreateOuputTopic("output", TimeSpan.FromSeconds(1), new StringTimeWindowedSerDes(), new Int32SerDes());
                input.PipeInput("test", "1");
                input.PipeInput("test", "230");
                input.PipeInput("test", "32");
                var elements = output.ReadKeyValueList().ToList();
                Assert.AreEqual(3, elements.Count);
                Assert.AreEqual("test", elements[0].Message.Key.Key);
                Assert.AreEqual((long)TimeSpan.FromSeconds(10).TotalMilliseconds, elements[0].Message.Key.Window.EndMs - elements[0].Message.Key.Window.StartMs);
                Assert.AreEqual(1, elements[0].Message.Value);
                Assert.AreEqual("test", elements[1].Message.Key.Key);
                Assert.AreEqual(elements[0].Message.Key.Window, elements[1].Message.Key.Window);
                Assert.AreEqual(3, elements[1].Message.Value);
                Assert.AreEqual("test", elements[2].Message.Key.Key);
                Assert.AreEqual(elements[0].Message.Key.Window, elements[2].Message.Key.Window);
                Assert.AreEqual(3, elements[2].Message.Value);
            }
        }

        [Test]
        public void TimeWindowingAggWithMaterialize()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-window-stream";

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(2000))
                .Aggregate(
                        () => 0,
                        (k, v, agg) => Math.Max(v.Length, agg),
                        InMemoryWindows<string, int>.As("store").WithValueSerdes<Int32SerDes>())
                .ToStream()
                .To<StringTimeWindowedSerDes, Int32SerDes>("output");

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                var input = driver.CreateInputTopic<string, string>("topic");
                var output = driver.CreateOuputTopic("output", TimeSpan.FromSeconds(1), new StringTimeWindowedSerDes(), new Int32SerDes());
                input.PipeInput("test", "1");
                input.PipeInput("test", "230");
                input.PipeInput("test", "32");
                var elements = output.ReadKeyValueList().ToList();
                Assert.AreEqual(3, elements.Count);
                Assert.AreEqual("test", elements[0].Message.Key.Key);
                Assert.AreEqual((long)TimeSpan.FromSeconds(10).TotalMilliseconds, elements[0].Message.Key.Window.EndMs - elements[0].Message.Key.Window.StartMs);
                Assert.AreEqual(1, elements[0].Message.Value);
                Assert.AreEqual("test", elements[1].Message.Key.Key);
                Assert.AreEqual(elements[0].Message.Key.Window, elements[1].Message.Key.Window);
                Assert.AreEqual(3, elements[1].Message.Value);
                Assert.AreEqual("test", elements[2].Message.Key.Key);
                Assert.AreEqual(elements[0].Message.Key.Window, elements[2].Message.Key.Window);
                Assert.AreEqual(3, elements[2].Message.Value);
            }
        }

        [Test]
        public void TimeWindowingAggKeySerdesUnknow()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-window-stream";

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(2000))
                .Aggregate(
                        () => 0,
                        (k, v, agg) => Math.Max(v.Length, agg),
                        InMemoryWindows<string, int>.As("store").WithValueSerdes<Int32SerDes>())
                .ToStream()
                .To("output");

            var topology = builder.Build();
            Assert.Throws<StreamsException>(() =>
            {
                using (var driver = new TopologyTestDriver(topology, config))
                {
                    var input = driver.CreateInputTopic<string, string>("topic");
                    input.PipeInput("test", "1");
                }
            });
        }

        [Test]
        public void TimeWindowingAggNothing()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-window-stream";

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(2000))
                .Aggregate(
                        () => 0,
                        (k, v, agg) => Math.Max(v.Length, agg),
                        InMemoryWindows<string, int>.As("store").WithValueSerdes<Int32SerDes>())
                .ToStream()
                .To<StringTimeWindowedSerDes, Int32SerDes>("output");

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                var input = driver.CreateInputTopic<string, string>("topic");
                var output = driver.CreateOuputTopic("output", TimeSpan.FromSeconds(1), new StringTimeWindowedSerDes(), new Int64SerDes());
                var elements = output.ReadKeyValueList().ToList();
                Assert.AreEqual(0, elements.Count);
            }
        }

        [Test]
        public void TimeWindowingQueryStoreAll()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-window-stream";
            
            var builder = new StreamBuilder();

            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(2000))
                .Aggregate(
                        () => 0,
                        (k, v, agg) => Math.Max(v.Length, agg),
                        InMemoryWindows<string, int>.As("store").WithValueSerdes<Int32SerDes>());

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                var input = driver.CreateInputTopic<string, string>("topic");
                input.PipeInput("test", "1");
                input.PipeInput("test", "2567");
                input.PipeInput("test", "32");
                var store = driver.GetWindowStore<string, int>("store");
                var elements = store.All().ToList();
                Assert.AreEqual(1, elements.Count);
                Assert.AreEqual("test", elements[0].Key.Key);
                Assert.AreEqual((long)TimeSpan.FromSeconds(2).TotalMilliseconds, elements[0].Key.Window.EndMs - elements[0].Key.Window.StartMs);
                Assert.AreEqual(4, elements[0].Value);
            }
        }

        [Test]
        public void TimeWindowingQueryStore2Window()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-window-stream";

            var builder = new StreamBuilder();

            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(2000))
                .Aggregate(
                        () => 0,
                        (k, v, agg) => Math.Max(v.Length, agg),
                        InMemoryWindows<string, int>.As("store").WithValueSerdes<Int32SerDes>());

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                DateTime dt = DateTime.Now;
                var input = driver.CreateInputTopic<string, string>("topic");
                input.PipeInput("test", "1", dt);
                input.PipeInput("test", "2300", dt);
                input.PipeInput("test", "3", dt.AddMinutes(1));
                var store = driver.GetWindowStore<string, int>("store");
                var elements = store.All().ToList();
                Assert.AreEqual(2, elements.Count);
                Assert.AreEqual("test", elements[0].Key.Key);
                Assert.AreEqual((long)TimeSpan.FromSeconds(2).TotalMilliseconds, elements[0].Key.Window.EndMs - elements[0].Key.Window.StartMs);
                Assert.AreEqual(4, elements[0].Value);
                Assert.AreEqual("test", elements[1].Key.Key);
                Assert.AreEqual((long)TimeSpan.FromSeconds(2).TotalMilliseconds, elements[1].Key.Window.EndMs - elements[1].Key.Window.StartMs);
                Assert.AreEqual(1, elements[1].Value);
            }
        }

    }
}
