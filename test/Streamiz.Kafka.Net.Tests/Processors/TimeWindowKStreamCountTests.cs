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
    public class TimeWindowKStreamCountTests
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
            Materialized<string, long, IWindowStore<Bytes, byte[]>> m = null;

            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(2000))
                .Count(m);

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
            task.InitializeStateStores();
            task.InitializeTopology();

            Assert.AreEqual(1, task.Context.States.StateStoreNames.Count());
            var nameStore = task.Context.States.StateStoreNames.ElementAt(0);
            Assert.IsNotNull(nameStore);
            Assert.AreNotEqual(string.Empty, nameStore);
            var store = task.GetStore(nameStore);
            Assert.IsInstanceOf<ITimestampedWindowStore<string, long>>(store);
            Assert.AreEqual(0, (store as ITimestampedWindowStore<string, long>).All().ToList().Count);
        }

        [Test]
        public void WithNullValueSerDes()
        {
            // WITH VALUE NULL SERDES, in running KeySerdes must be StringSerdes, and ValueSerdes Int64SerDes
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-window-count";

            var builder = new StreamBuilder();

            Materialized<string, long, IWindowStore<Bytes, byte[]>> m =
                Materialized<string, long, IWindowStore<Bytes, byte[]>>
                    .Create("count-store")
                    .With(null, null);

            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromSeconds(5)))
                .Count(m)
                .ToStream()
                .To<StringTimeWindowedSerDes, Int64SerDes>("output-topic");

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                var input = driver.CreateInputTopic<string, string>("topic");
                var output = driver.CreateOuputTopic("output-topic", TimeSpan.FromSeconds(1), new StringTimeWindowedSerDes(), new Int64SerDes());
                input.PipeInput("test", "1");
                input.PipeInput("test-test", "30");
                var records = output.ReadKeyValueList().ToList();
                Assert.AreEqual(2, records.Count);
                Assert.AreEqual("test", records[0].Message.Key.Key);
                Assert.AreEqual(1, records[0].Message.Value);
                Assert.AreEqual("test-test", records[1].Message.Key.Key);
                Assert.AreEqual(1, records[1].Message.Value);
                Assert.AreEqual(records[0].Message.Key.Window, records[1].Message.Key.Window);
            }
        }

        [Test]
        public void TimeWindowingCount()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-window-stream";

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromSeconds(10)))
                .Count()
                .ToStream()
                .To<StringTimeWindowedSerDes, Int64SerDes>("output");

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                var input = driver.CreateInputTopic<string, string>("topic");
                var output = driver.CreateOuputTopic("output", TimeSpan.FromSeconds(1), new StringTimeWindowedSerDes(), new Int64SerDes());
                input.PipeInput("test", "1");
                input.PipeInput("test", "2");
                input.PipeInput("test", "3");
                var elements = output.ReadKeyValueList().ToList();
                Assert.AreEqual(3, elements.Count);
                Assert.AreEqual("test", elements[0].Message.Key.Key);
                Assert.AreEqual((long)TimeSpan.FromSeconds(10).TotalMilliseconds, elements[0].Message.Key.Window.EndMs - elements[0].Message.Key.Window.StartMs);
                Assert.AreEqual(1, elements[0].Message.Value);
                Assert.AreEqual("test", elements[1].Message.Key.Key);
                Assert.AreEqual(elements[0].Message.Key.Window, elements[1].Message.Key.Window);
                Assert.AreEqual(2, elements[1].Message.Value);
                Assert.AreEqual("test", elements[2].Message.Key.Key);
                Assert.AreEqual(elements[0].Message.Key.Window, elements[2].Message.Key.Window);
                Assert.AreEqual(3, elements[2].Message.Value);
            }
        }

        [Test]
        public void TimeWindowingCountWithName()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-window-stream";

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromSeconds(10)))
                .Count("count-01")
                .ToStream()
                .To<StringTimeWindowedSerDes, Int64SerDes>("output");

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                var input = driver.CreateInputTopic<string, string>("topic");
                var output = driver.CreateOuputTopic("output", TimeSpan.FromSeconds(1), new StringTimeWindowedSerDes(), new Int64SerDes());
                input.PipeInput("test", "1");
                input.PipeInput("test", "2");
                input.PipeInput("test", "3");
                var elements = output.ReadKeyValueList().ToList();
                Assert.AreEqual(3, elements.Count);
                Assert.AreEqual("test", elements[0].Message.Key.Key);
                Assert.AreEqual((long)TimeSpan.FromSeconds(10).TotalMilliseconds, elements[0].Message.Key.Window.EndMs - elements[0].Message.Key.Window.StartMs);
                Assert.AreEqual(1, elements[0].Message.Value);
                Assert.AreEqual("test", elements[1].Message.Key.Key);
                Assert.AreEqual(elements[0].Message.Key.Window, elements[1].Message.Key.Window);
                Assert.AreEqual(2, elements[1].Message.Value);
                Assert.AreEqual("test", elements[2].Message.Key.Key);
                Assert.AreEqual(elements[0].Message.Key.Window, elements[2].Message.Key.Window);
                Assert.AreEqual(3, elements[2].Message.Value);
            }
        }

        [Test]
        public void TimeWindowingCountWithMaterialize()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-window-stream";

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromSeconds(10)))
                .Count(Materialized<string, long, IWindowStore<Bytes, byte[]>>.Create("count-store"))
                .ToStream()
                .To<StringTimeWindowedSerDes, Int64SerDes>("output");

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                var input = driver.CreateInputTopic<string, string>("topic");
                var output = driver.CreateOuputTopic("output", TimeSpan.FromSeconds(1), new StringTimeWindowedSerDes(), new Int64SerDes());
                input.PipeInput("test", "1");
                input.PipeInput("test", "2");
                input.PipeInput("test", "3");
                var elements = output.ReadKeyValueList().ToList();
                Assert.AreEqual(3, elements.Count);
                Assert.AreEqual("test", elements[0].Message.Key.Key);
                Assert.AreEqual((long)TimeSpan.FromSeconds(10).TotalMilliseconds, elements[0].Message.Key.Window.EndMs - elements[0].Message.Key.Window.StartMs);
                Assert.AreEqual(1, elements[0].Message.Value);
                Assert.AreEqual("test", elements[1].Message.Key.Key);
                Assert.AreEqual(elements[0].Message.Key.Window, elements[1].Message.Key.Window);
                Assert.AreEqual(2, elements[1].Message.Value);
                Assert.AreEqual("test", elements[2].Message.Key.Key);
                Assert.AreEqual(elements[0].Message.Key.Window, elements[2].Message.Key.Window);
                Assert.AreEqual(3, elements[2].Message.Value);
            }
        }

        [Test]
        public void TimeWindowingCountKeySerdesUnknow()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-window-stream";

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromSeconds(10)))
                .Count(Materialized<string, long, IWindowStore<Bytes, byte[]>>.Create("count-store"))
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
        public void TimeWindowingCountNothing()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-window-stream";

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromSeconds(1)))
                .Count()
                .ToStream()
                .To<StringTimeWindowedSerDes, Int64SerDes>("output");

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
                .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromSeconds(10)))
                .Count(InMemoryWindows<string, long>.As("count-store"));

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                var input = driver.CreateInputTopic<string, string>("topic");
                input.PipeInput("test", "1");
                input.PipeInput("test", "2");
                input.PipeInput("test", "3");
                var store = driver.GetWindowStore<string, long>("count-store");
                var elements = store.All().ToList();
                Assert.AreEqual(1, elements.Count);
                Assert.AreEqual("test", elements[0].Key.Key);
                Assert.AreEqual((long)TimeSpan.FromSeconds(10).TotalMilliseconds, elements[0].Key.Window.EndMs - elements[0].Key.Window.StartMs);
                Assert.AreEqual(3, elements[0].Value);
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
                .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromSeconds(5)))
                .Count(InMemoryWindows<string, long>.As("count-store"));

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                DateTime dt = DateTime.Now;
                var input = driver.CreateInputTopic<string, string>("topic");
                input.PipeInput("test", "1", dt);
                input.PipeInput("test", "2", dt);
                input.PipeInput("test", "3", dt.AddMinutes(1));
                var store = driver.GetWindowStore<string, long>("count-store");
                var elements = store.All().ToList();
                Assert.AreEqual(2, elements.Count);
                Assert.AreEqual("test", elements[0].Key.Key);
                Assert.AreEqual((long)TimeSpan.FromSeconds(5).TotalMilliseconds, elements[0].Key.Window.EndMs - elements[0].Key.Window.StartMs);
                Assert.AreEqual(2, elements[0].Value);
                Assert.AreEqual("test", elements[1].Key.Key);
                Assert.AreEqual((long)TimeSpan.FromSeconds(5).TotalMilliseconds, elements[1].Key.Window.EndMs - elements[1].Key.Window.StartMs);
                Assert.AreEqual(1, elements[1].Value);
            }
        }
    }
}
