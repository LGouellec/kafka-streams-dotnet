using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Crosscutting;
using System;
using System.Collections.Generic;
using System.Linq;
using Streamiz.Kafka.Net.Errors;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamPassThoughTests
    {

        private class MyITimestampExtractor : ITimestampExtractor
        {
            public long Extract(ConsumeResult<object, object> record, long partitionTime)
            {
                return DateTime.Now.GetMilliseconds();
            }
        }

        [Test]
        public void NoSerdesCompatible()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-passto";
            var builder = new StreamBuilder();

            builder
                .Stream<string, string>("topic")
                .MapValues((k, v) => k.ToCharArray()[0])
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
        public void ShouldNotAllowNullOrEmptyTopic()
        {
            var builder = new StreamBuilder();
            Assert.Throws<ArgumentException>(() => builder.Stream<string, string>(null));
            Assert.Throws<ArgumentException>(() => builder.Stream<string, string>(""));
        }

        [Test]
        public void ShouldNotAllowNullTopicDest()
        {
            var builder = new StreamBuilder();
            var stream = builder.Stream<string, string>("topic");
            string topicDes = null;
            Assert.Throws<ArgumentNullException>(() => stream.To(topicDes));
        }

        [Test]
        public void ShouldNotAllowEmptyTopicDest()
        {
            var builder = new StreamBuilder();
            var stream = builder.Stream<string, string>("topic");
            string topicDes = "";
            Assert.Throws<ArgumentException>(() => stream.To(topicDes));
        }

        [Test]
        public void PassThoughElements()
        {
            var builder = new StreamBuilder();

            builder.Stream<string, string>("topic").To("topic2");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-passthrough";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOuputTopic<string, string>("topic2");
                var expected = new List<KeyValuePair<string, string>>();
                for (int i = 0; i < 42; i++)
                {
                    string key = i.ToString();
                    string value = $"V{i}";
                    inputTopic.PipeInput(key, value);
                    expected.Add(KeyValuePair.Create(key, value));
                }

                var list = outputTopic.ReadKeyValueList().Select(r => KeyValuePair.Create(r.Message.Key, r.Message.Value)).ToList();

                Assert.AreEqual(expected, list);
            }
        }

        [Test]
        public void PassThoughElements2()
        {
            var builder = new StreamBuilder();

            builder
                .Stream<string, string>("topic")
                .To((k, v, c) => "topic2");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-passthrough";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOuputTopic<string, string>("topic2");
                var expected = new List<KeyValuePair<string, string>>();
                for (int i = 0; i < 42; i++)
                {
                    string key = i.ToString();
                    string value = $"V{i}";
                    inputTopic.PipeInput(key, value);
                    expected.Add(KeyValuePair.Create(key, value));
                }

                var list = outputTopic.ReadKeyValueList().Select(r => KeyValuePair.Create(r.Message.Key, r.Message.Value)).ToList();

                Assert.AreEqual(expected, list);
            }
        }

        [Test]
        public void PassThoughElements3()
        {
            var builder = new StreamBuilder();

            builder
                .Stream<string, string>("topic")
                .To<StringSerDes, StringSerDes>("topic2");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-passthrough";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOuputTopic<string, string>("topic2");
                var expected = new List<KeyValuePair<string, string>>();
                for (int i = 0; i < 42; i++)
                {
                    string key = i.ToString();
                    string value = $"V{i}";
                    inputTopic.PipeInput(key, value);
                    expected.Add(KeyValuePair.Create(key, value));
                }

                var list = outputTopic.ReadKeyValueList().Select(r => KeyValuePair.Create(r.Message.Key, r.Message.Value)).ToList();

                Assert.AreEqual(expected, list);
            }
        }

        [Test]
        public void PassThoughElements4()
        {
            var builder = new StreamBuilder();

            builder
                .Stream<string, string, StringSerDes, StringSerDes>("topic")
                .To("topic2");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-passthrough";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOuputTopic<string, string>("topic2");
                var expected = new List<KeyValuePair<string, string>>();
                for (int i = 0; i < 42; i++)
                {
                    string key = i.ToString();
                    string value = $"V{i}";
                    inputTopic.PipeInput(key, value);
                    expected.Add(KeyValuePair.Create(key, value));
                }

                var list = outputTopic.ReadKeyValueList().Select(r => KeyValuePair.Create(r.Message.Key, r.Message.Value)).ToList();

                Assert.AreEqual(expected, list);
            }
        }

        [Test]
        public void PassThoughElements5()
        {
            var builder = new StreamBuilder();

            builder
                .Stream<string, string, StringSerDes, StringSerDes>("topic", new MyITimestampExtractor())
                .To("topic2");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-passthrough";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOuputTopic<string, string>("topic2");
                var expected = new List<KeyValuePair<string, string>>();
                for (int i = 0; i < 42; i++)
                {
                    string key = i.ToString();
                    string value = $"V{i}";
                    inputTopic.PipeInput(key, value);
                    expected.Add(KeyValuePair.Create(key, value));
                }

                var list = outputTopic.ReadKeyValueList().Select(r => KeyValuePair.Create(r.Message.Key, r.Message.Value)).ToList();

                Assert.AreEqual(expected, list);
            }
        }

        [Test]
        public void PassThoughElements6()
        {
            var builder = new StreamBuilder();

            builder
                .Stream<string, string, StringSerDes, StringSerDes>("topic", "source", new MyITimestampExtractor())
                .To("topic2");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-passthrough";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOuputTopic<string, string>("topic2");
                var expected = new List<KeyValuePair<string, string>>();
                for (int i = 0; i < 42; i++)
                {
                    string key = i.ToString();
                    string value = $"V{i}";
                    inputTopic.PipeInput(key, value);
                    expected.Add(KeyValuePair.Create(key, value));
                }

                var list = outputTopic.ReadKeyValueList().Select(r => KeyValuePair.Create(r.Message.Key, r.Message.Value)).ToList();

                Assert.AreEqual(expected, list);
            }
        }
    }
}