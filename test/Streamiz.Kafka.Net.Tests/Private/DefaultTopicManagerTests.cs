using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class DefaultTopicManagerTests
    {
        private SyncKafkaSupplier kafkaSupplier;

        [SetUp]
        public void Begin()
        {
            kafkaSupplier = new SyncKafkaSupplier();
        }

        [TearDown]
        public void Dispose()
        {
            kafkaSupplier = null;
        }

        [Test]
        public void ApplyInternalChangelogTopics()
        {
            AdminClientConfig config = new AdminClientConfig();
            config.BootstrapServers = "localhost:9092";

            StreamConfig config2 = new StreamConfig();

            DefaultTopicManager manager = new DefaultTopicManager(config2, kafkaSupplier.GetAdmin(config));

            IDictionary<string, InternalTopicConfig> topics = new Dictionary<string, InternalTopicConfig>();
            topics.Add("topic", new UnwindowedChangelogTopicConfig
            {
                Name = "topic",
                NumberPartitions = 1
            });
            topics.Add("topic1", new UnwindowedChangelogTopicConfig
            {
                Name = "topic1",
                NumberPartitions = 1
            });

            var r = manager.ApplyAsync(topics).GetAwaiter().GetResult().ToList();

            Assert.AreEqual(2, r.Count);
            Assert.AreEqual("topic", r[0]);
            Assert.AreEqual("topic1", r[1]);
        }

        [Test]
        public void ApplyInternalChangelogTopics2()
        {
            AdminClientConfig config = new AdminClientConfig();
            config.BootstrapServers = "localhost:9092";

            StreamConfig config2 = new StreamConfig();

            DefaultTopicManager manager = new DefaultTopicManager(config2, kafkaSupplier.GetAdmin(config));

            IDictionary<string, InternalTopicConfig> topics = new Dictionary<string, InternalTopicConfig>();
            topics.Add("topic", new WindowedChangelogTopicConfig
            {
                Name = "topic",
                NumberPartitions = 1
            });
            topics.Add("topic1", new WindowedChangelogTopicConfig
            {
                Name = "topic1",
                NumberPartitions = 1
            });

            var r = manager.ApplyAsync(topics).GetAwaiter().GetResult().ToList();

            Assert.AreEqual(2, r.Count);
            Assert.AreEqual("topic", r[0]);
            Assert.AreEqual("topic1", r[1]);
        }

        [Test]
        public void ApplyInternalChangelogTopicsWithExistingTopics()
        {
            AdminClientConfig config = new AdminClientConfig();
            config.BootstrapServers = "localhost:9092";

            StreamConfig config2 = new StreamConfig();

            DefaultTopicManager manager = new DefaultTopicManager(config2, kafkaSupplier.GetAdmin(config));

            ((SyncProducer)kafkaSupplier.GetProducer(new ProducerConfig())).CreateTopic("topic");

            IDictionary<string, InternalTopicConfig> topics = new Dictionary<string, InternalTopicConfig>();
            topics.Add("topic", new UnwindowedChangelogTopicConfig
            {
                Name = "topic",
                NumberPartitions = 1
            });
            topics.Add("topic1", new UnwindowedChangelogTopicConfig
            {
                Name = "topic1",
                NumberPartitions = 1
            });

            var r = manager.ApplyAsync(topics).GetAwaiter().GetResult().ToList();

            Assert.AreEqual(1, r.Count);
            Assert.AreEqual("topic1", r[0]);
        }

        [Test]
        public void ApplyInternalChangelogTopicsInvalidDifferentPartition()
        {
            AdminClientConfig config = new AdminClientConfig();
            config.BootstrapServers = "localhost:9092";

            StreamConfig config2 = new StreamConfig();

            DefaultTopicManager manager = new DefaultTopicManager(config2, kafkaSupplier.GetAdmin(config));

            // Create topic with just one partition
            ((SyncProducer)kafkaSupplier.GetProducer(new ProducerConfig())).CreateTopic("topic");

            IDictionary<string, InternalTopicConfig> topics = new Dictionary<string, InternalTopicConfig>();
            topics.Add("topic", new UnwindowedChangelogTopicConfig
            {
                Name = "topic",
                NumberPartitions = 4
            });

            Assert.Throws<StreamsException>(() => manager.ApplyAsync(topics).GetAwaiter().GetResult());
        }

        [Test]
        public void ApplyInternalChangelogTopicsParrallel()
        {
            AdminClientConfig config = new AdminClientConfig();
            config.BootstrapServers = "localhost:9092";

            StreamConfig config2 = new StreamConfig();

            DefaultTopicManager manager = new DefaultTopicManager(config2, kafkaSupplier.GetAdmin(config));

            IDictionary<string, InternalTopicConfig> topics = new Dictionary<string, InternalTopicConfig>();
            topics.Add("topic", new UnwindowedChangelogTopicConfig
            {
                Name = "topic",
                NumberPartitions = 1
            });

            var r = Parallel.ForEach(new List<int> { 1, 2, 3, 4 }, (i) => manager.ApplyAsync(topics).GetAwaiter().GetResult());
            Assert.IsTrue(r.IsCompleted);
        }

        // WAIT dotnet testcontainers

        //[Test]
        //public void test()
        //{
        //    StreamBuilder builder = new StreamBuilder();
        //    var options = InMemory<string, long>
        //        .As("count-store")
        //        .WithLoggingEnabled(null)
        //        .WithKeySerdes(new StringSerDes())
        //        .WithValueSerdes(new Int64SerDes());

        //    builder
        //        .Stream<string, string>("source")
        //        .GroupByKey()
        //        .Count(options);

        //    var topology = builder.Build();
        //    topology.Builder.BuildTopology();

        //    AdminClientConfig config = new AdminClientConfig();
        //    config.BootstrapServers = "localhost:9092";
        //    AdminClientBuilder adminClientBuilder = new AdminClientBuilder(config);

        //    StreamConfig config2 = new StreamConfig();

        //    DefaultTopicManager manager = new DefaultTopicManager(config2, adminClientBuilder.Build());

        //    InternalTopicManagerUtils.CreateChangelogTopicsAsync(manager, topology.Builder)
        //        .GetAwaiter().GetResult();

        //    manager.Dispose();
        //}

        //    [Test]
        //    public void test1()
        //    {
        //        AdminClientConfig config = new AdminClientConfig();
        //        config.BootstrapServers = "localhost:9092";

        //        AdminClientBuilder builder = new AdminClientBuilder(config);
        //        StreamConfig config2 = new StreamConfig();

        //        DefaultTopicManager manager = new DefaultTopicManager(config2, builder.Build());


        //        IDictionary<string, InternalTopicConfig> topics = new Dictionary<string, InternalTopicConfig>();
        //        topics.Add("topic4", new UnwindowedChangelogTopicConfig
        //        {
        //            Name = "topic4",
        //            NumberPartitions = 10
        //        });
        //        topics.Add("topic5", new UnwindowedChangelogTopicConfig
        //        {
        //            Name = "topic5",
        //            NumberPartitions = 10
        //        });
        //        topics.Add("topic6", new UnwindowedChangelogTopicConfig
        //        {
        //            Name = "topic6",
        //            NumberPartitions = 10
        //        });

        //        var r = Parallel.ForEach(new List<int> { 1, 2, 3, 4 }, (i) => manager.ApplyAsync(topics).GetAwaiter().GetResult());
        //    }
    }
}