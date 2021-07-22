using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using System;
using System.Collections.Generic;


namespace Streamiz.Kafka.Net.Tests.Private
{
    public class ProcessorStateManagerTests
    {
        private class TestStateStore : IStateStore
        {
            public TestStateStore(string name)
            {
                Name = name;
            }

            public string Name { get; }

            public bool Persistent => false;

            public bool IsOpen => true;

            public void Close()
            {
                
            }

            public void Flush()
            {
                
            }

            public void Init(ProcessorContext context, IStateStore root)
            {
                
            }
        }

        [Test]
        public void RegisterStateStoreAlreadyRegistered()
        {
            TaskId id = new TaskId { Id = 1, Partition = 0 };
            TopicPartition partition = new TopicPartition("topic", 0);
            ProcessorStateManager stateMgt = new ProcessorStateManager(id, new List<TopicPartition> { partition }, null);

            stateMgt.Register(new TestStateStore("state-store1"), null);
            Assert.Throws<ArgumentException>(() => stateMgt.Register(new TestStateStore("state-store1"), null));
        }

        [Test]
        public void RegisterStateStoreAndGetIt()
        {
            TaskId id = new TaskId { Id = 1, Partition = 0 };
            TopicPartition partition = new TopicPartition("topic", 0);
            ProcessorStateManager stateMgt = new ProcessorStateManager(id, new List<TopicPartition> { partition }, null);

            stateMgt.Register(new TestStateStore("state-store1"), null);
            var store = stateMgt.GetStore("state-store1");
            Assert.IsNotNull(store);
            Assert.AreEqual("state-store1", store.Name);
        }

        [Test]
        public void RegisterStateStoreAndGetUnknown()
        {
            TaskId id = new TaskId { Id = 1, Partition = 0 };
            TopicPartition partition = new TopicPartition("topic", 0);
            ProcessorStateManager stateMgt = new ProcessorStateManager(id, new List<TopicPartition> { partition }, null);

            stateMgt.Register(new TestStateStore("state-store1"), null);
            var store = stateMgt.GetStore("sdfdfre1");
            Assert.IsNull(store);
        }

        [Test]
        public void GetChangelogTopicPartition()
        {
            TaskId id = new TaskId { Id = 0, Partition = 2 };
            TopicPartition partition = new TopicPartition("topic", 0);
            ProcessorStateManager stateMgt = new ProcessorStateManager(id, new List<TopicPartition> { partition }, new Dictionary<string, string> { { "state-store1", "state-store1-cl" } });

            stateMgt.Register(new TestStateStore("state-store1"), null);
            var tp = stateMgt.GetRegisteredChangelogPartitionFor("state-store1");

            Assert.AreEqual(2, tp.Partition.Value);
            Assert.AreEqual("state-store1-cl", tp.Topic);
        }
    }
}
