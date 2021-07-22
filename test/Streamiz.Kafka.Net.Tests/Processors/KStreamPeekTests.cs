using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamPeekTests
    {
        [Test]
        public void ShouldObserveStreamElements()
        {
            var builder = new StreamBuilder();
            var peekObserved = new List<KeyValuePair<string, string>>();
            var streamObserved = new List<KeyValuePair<string, string>>();

            builder.Stream<string, string>("topic")
                .Peek((k, v) => peekObserved.Add(KeyValuePair.Create(k, v)))
                .Foreach((k, v) => streamObserved.Add(KeyValuePair.Create(k, v)));

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-peek";
            
            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var expected = new List<KeyValuePair<string, string>>();
                for (int i = 0; i < 32; i++)
                {
                    string key = i.ToString();
                    string value = $"V{i}";
                    inputTopic.PipeInput(key, value);
                    expected.Add(KeyValuePair.Create(key, value));
                }
                
                Assert.AreEqual(expected, peekObserved);
                Assert.AreEqual(expected, streamObserved);
            }
        }

        [Test]
        public void ShouldNotAllowNullAction()
        {
            var builder = new StreamBuilder();
            IKStream<string, string> stream = builder.Stream<string, string>("topic");

            Assert.Throws<ArgumentNullException>(() => stream.Peek(null));
        }
    }
}