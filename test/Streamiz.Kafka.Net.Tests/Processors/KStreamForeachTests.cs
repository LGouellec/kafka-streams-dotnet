using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamForeachTests
    {
        [Test]
        public void ShouldNotAllowNullAction()
        {
            var builder = new StreamBuilder();
            IKStream<string, string> stream = builder.Stream<string, string>("topic");
            Assert.Throws<ArgumentNullException>(() => stream.Foreach(null));
        }

        [Test]
        public void ForeachAction()
        {
            var builder = new StreamBuilder();
            var foreachObserved = new List<KeyValuePair<string, string>>();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "123456"));

            builder.Stream<string, string>("topic")
                .Foreach((k, v) => foreachObserved.Add(KeyValuePair.Create(k, v)));

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-foreach";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");

                inputTopic.PipeInputs(data);

                Assert.AreEqual(data, foreachObserved);
            }
        }
    }
}
