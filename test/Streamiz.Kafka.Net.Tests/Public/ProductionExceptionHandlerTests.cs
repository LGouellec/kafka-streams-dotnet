using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Tests.Helpers;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Streamiz.Kafka.Net.Tests.Public
{
    public class ProductionExceptionHandlerTests
    {
        [Test]
        public async Task KafkaStreamProductionExceptionHandlerContinueTest()
        {
            var _return = new List<KeyValuePair<string, string>>();

            var timeout = TimeSpan.FromSeconds(10);

            bool isRunningState = false;
            DateTime dt = DateTime.Now;

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;
            config.ProductionExceptionHandler += (r) => ExceptionHandlerResponse.CONTINUE;

            var supplier = new ProducerSyncException();
            var producer = supplier.GetProducer(config.ToProducerConfig());

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("test")
                .To("test-output");

            builder.Stream<string, string>("test-output")
                .Peek((k, v) => _return.Add(KeyValuePair.Create(k, v)));

            var t = builder.Build();
            var stream = new KafkaStream(t, config, supplier);

            stream.StateChanged += (old, @new) =>
            {
                if (@new.Equals(KafkaStream.State.RUNNING))
                {
                    isRunningState = true;
                }
            };
            await stream.StartAsync();
            while (!isRunningState)
            {
                Thread.Sleep(250);
                if (DateTime.Now > dt + timeout)
                {
                    break;
                }
            }
            Assert.IsTrue(isRunningState);

            if (isRunningState)
            {
                var serdes = new StringSerDes();
                dt = DateTime.Now;
                producer.Produce("test",
                    new Confluent.Kafka.Message<byte[], byte[]>
                    {
                        Key = serdes.Serialize("k", new SerializationContext()),
                        Value = serdes.Serialize("test", new SerializationContext()),
                        Timestamp = new Confluent.Kafka.Timestamp(dt)
                    });
                Thread.Sleep(1000);
                var expected = new List<KeyValuePair<string, string>>();

                Assert.AreEqual(expected, _return);
            }

            stream.Dispose();
        }

        [Test]
        public void KafkaStreamProductionExceptionHandlerFailTest()
        {
            bool errorState = false;
            var _return = new List<KeyValuePair<string, string>>();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            var dt = DateTime.Now;
            var timeout = TimeSpan.FromSeconds(10);

            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;
            config.ProductionExceptionHandler += (r) => ExceptionHandlerResponse.FAIL;

            var supplier = new ProducerSyncException();

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("test")
                .To("test-output");

            builder.Stream<string, string>("test-output")
                .Peek((k, v) => _return.Add(KeyValuePair.Create(k, v)));

            var t = builder.Build();

            using (var driver = new TopologyTestDriver(t.Builder, config, TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY, supplier))
            {
                var inputtopic = driver.CreateInputTopic<string, string>("test");
                inputtopic.PipeInput("coucou");
                while (!errorState)
                {
                    errorState = driver.IsError;
                    Thread.Sleep(10);
                    if (DateTime.Now > dt + timeout)
                    {
                        break;
                    }
                }
                Assert.IsTrue(driver.IsError);
            }

            Assert.AreEqual(new List<KeyValuePair<string, string>>(), _return);
        }
    }
}
