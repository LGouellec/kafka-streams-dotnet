using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class PauseResumeTests
    {
        [Test]
        public void WorkflowCompleteMaxTaskIdleTest()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-group";
            config.MaxTaskIdleMs = (long)TimeSpan.FromSeconds(100).TotalMilliseconds;

            var builder = new StreamBuilder();

            var stream1 = builder.Stream<string, string>("topic1");
            var stream2 = builder.Stream<string, string>("topic2");

            stream1
                .LeftJoin(stream2, (v1, v2) => $"{v1}-{v2}", JoinWindowOptions.Of(TimeSpan.FromSeconds(10)))
                .To("output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic1 = driver.CreateInputTopic<string, string>("topic1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("topic2");
                var outputTopic = driver.CreateOuputTopic<string, string>("output");
                inputTopic1.PipeInput("key", "i1");
                inputTopic1.PipeInput("key", "i2");
                inputTopic1.PipeInput("key", "i3");
                var items = outputTopic.ReadKeyValueList().ToList();
                Assert.AreEqual(0, items.Count);
            }
        }

        //[Test]
        // TODO : fix that
        public void WorkflowCompleteBufferedRecordsTest()
        {
            int maxBuffered = 10;
            var token = new System.Threading.CancellationTokenSource();
            var serdes = new StringSerDes();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-group";
            config.MaxTaskIdleMs = (long)TimeSpan.FromSeconds(100).TotalMilliseconds;
            config.BufferedRecordsPerPartition = maxBuffered;
            config.PollMs = 10;

            var builder = new StreamBuilder();

            var stream1 = builder.Stream<string, string>("topic1");
            var stream2 = builder.Stream<string, string>("topic2");

            stream1
                .Join(stream2, (v1, v2) => $"{v1}-{v2}", JoinWindowOptions.Of(TimeSpan.FromSeconds(10)))
                .To("output");

            var topo = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(config.ToConsumerConfig("test-consum"), null);
            consumer.Subscribe("output");
            var thread = StreamThread.Create(
                "thread-0", "c0",
                topo.Builder, config,
                supplier, supplier.GetAdmin(config.ToAdminConfig("admin")),
                0) as StreamThread;

            thread.Start(token.Token);

            for (int i = 0; i < maxBuffered + 1; ++i)
            {
                producer.Produce("topic1", new Confluent.Kafka.Message<byte[], byte[]>
                {
                    Key = serdes.Serialize("key", new SerializationContext()),
                    Value = serdes.Serialize($"coucou{i}", new SerializationContext())
                });
            }
            // CONSUME PAUSE AFTER maxBuffered + 1 messages
            System.Threading.Thread.Sleep(50);

            // Add one message more with consumer in stream thread in pause
            producer.Produce("topic1", new Confluent.Kafka.Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key", new SerializationContext()),
                Value = serdes.Serialize($"coucou{maxBuffered+1}", new SerializationContext())
            });

            Assert.AreEqual(1, thread.ActiveTasks.Count());
            var task = thread.ActiveTasks.ToArray()[0];
            Assert.IsNotNull(task.Grouper);
            Assert.IsFalse(task.Grouper.AllPartitionsBuffered);
            Assert.AreEqual(maxBuffered+1, task.Grouper.NumBuffered());
            Assert.AreEqual(maxBuffered+1, task.Grouper.NumBuffered(new TopicPartition("topic1", 0)));
            Assert.AreEqual(0, task.Grouper.NumBuffered(new TopicPartition("topic2", 0)));

            producer.Produce("topic2", new Confluent.Kafka.Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key", new SerializationContext()),
                Value = serdes.Serialize($"test", new SerializationContext())
            });

            List<ConsumeResult<byte[], byte[]>> records = new List<ConsumeResult<byte[], byte[]>>();
            do
            {
                records.AddRange(consumer.ConsumeRecords(TimeSpan.FromMilliseconds(100)).ToList());
            } while (records.Count() <= 12);

            Assert.AreEqual(maxBuffered + 2, records.Count());
            for (int i = 0; i < maxBuffered + 2; ++i)
            {
                var message = records.ToArray()[i];
                Assert.AreEqual("key", serdes.Deserialize(message.Message.Key, new SerializationContext()));
                Assert.IsTrue(serdes.Deserialize(message.Message.Value, new SerializationContext()).Contains($"coucou{i}-"));
            }

            token.Cancel();
            thread.Dispose();
        }
    }
}
