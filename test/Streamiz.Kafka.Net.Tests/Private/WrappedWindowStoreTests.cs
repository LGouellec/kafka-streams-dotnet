using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.InMemory;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Crosscutting;
using System;
using Streamiz.Kafka.Net.State;
using System.Text;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class WrappedWindowStoreTests
    {
        private WrappedWindowStore<string, int> wrapped = null;
        private InMemoryWindowStore inmemorystore = null;

        [SetUp]
        public void Setup()
        {
            inmemorystore = new InMemoryWindowStore("store", TimeSpan.FromMinutes(20), 1000 * 2);
            wrapped = new WrappedWindowStore<string, int>(inmemorystore, 1000 * 2, new StringSerDes(),  new Int32SerDes());
        }

        [Test]
        public void TestWithUnknwonSerdes()
        { 
            wrapped = new WrappedWindowStore<string, int>(inmemorystore, 1000 * 2, null,  null);
            var id = new TaskId { Id = 0,  Partition = 0 };
            var stateManager = new ProcessorStateManager(id, new List<Confluent.Kafka.TopicPartition> { new Confluent.Kafka.TopicPartition("test", 0) }, null);
            var context = new ProcessorContext(null, new StreamConfig(), stateManager);
            wrapped.Init(context, inmemorystore);
            Assert.Throws<StreamsException>(() => wrapped.Put("coucou", 120, 1300));
        }

        [Test]
        public void TestWithUnknwonSerdes2()
        {
            wrapped = new WrappedWindowStore<string, int>(inmemorystore, 1000 * 2, null, null);
            var id = new TaskId { Id = 0,  Partition = 0 };
            var stateManager = new ProcessorStateManager(id, new List<Confluent.Kafka.TopicPartition> { new Confluent.Kafka.TopicPartition("test", 0) }, null);
            var context = new ProcessorContext(null, new StreamConfig(), stateManager);
            wrapped.Init(context, inmemorystore);
            inmemorystore.Put(new Bytes(Encoding.UTF8.GetBytes("test")), BitConverter.GetBytes(100), 300);
            Assert.Throws<StreamsException>(() => wrapped.Fetch("test", 300));
        }

        [Test]
        public void TestFetch()
        {
            var id = new TaskId { Id = 0,  Partition = 0 };
            var stateManager = new ProcessorStateManager(id, new List<Confluent.Kafka.TopicPartition> { new Confluent.Kafka.TopicPartition("test", 0) }, null);
            var context = new ProcessorContext(null, new StreamConfig(), stateManager);
            wrapped.Init(context, inmemorystore);
            wrapped.Put("coucou", 120, 1300);
            Assert.AreEqual(120, wrapped.Fetch("coucou", 1300));
        }

        [Test]
        public void TestFetchWithDate()
        {
            var dt = DateTime.Now;
            var id = new TaskId { Id = 0,  Partition = 0 };
            var stateManager = new ProcessorStateManager(id, new List<Confluent.Kafka.TopicPartition> { new Confluent.Kafka.TopicPartition("test", 0) }, null);
            var context = new ProcessorContext(null, new StreamConfig(), stateManager);
            wrapped.Init(context, inmemorystore);
            wrapped.Put("coucou", 120, dt.GetMilliseconds());
            var list = wrapped.Fetch("coucou", dt.AddSeconds(-1), dt.AddSeconds(2)).ToList();
            Assert.AreEqual(1, list.Count);
            Assert.AreEqual(dt.GetMilliseconds(), list[0].Key);
            Assert.AreEqual(120, list[0].Value);
        }

        [Test]
        public void TestFetchAll()
        {
            var dt = DateTime.Now;
            var id = new TaskId { Id = 0,  Partition = 0 };
            var stateManager = new ProcessorStateManager(id, new List<Confluent.Kafka.TopicPartition> { new Confluent.Kafka.TopicPartition("test", 0) }, null);
            var context = new ProcessorContext(null, new StreamConfig(), stateManager);
            wrapped.Init(context, inmemorystore);
            wrapped.Put("coucou", 120, dt.GetMilliseconds());
            var list = wrapped.FetchAll(dt.AddSeconds(-1), dt.AddSeconds(2)).ToList();
            Assert.AreEqual(1, list.Count);
            Assert.AreEqual(dt.GetMilliseconds(), list[0].Key.Window.StartMs);
            Assert.AreEqual("coucou", list[0].Key.Key);
            Assert.AreEqual(120, list[0].Value);
        }

        [Test]
        public void TestAll()
        {
            var dt = DateTime.Now;
            var id = new TaskId { Id = 0,  Partition = 0 };
            var stateManager = new ProcessorStateManager(id, new List<Confluent.Kafka.TopicPartition> { new Confluent.Kafka.TopicPartition("test", 0) }, null);
            var context = new ProcessorContext(null, new StreamConfig(), stateManager);
            wrapped.Init(context, inmemorystore);
            wrapped.Put("coucou", 120, dt.GetMilliseconds());
            wrapped.Put("coucou-toto", 5, dt.GetMilliseconds());
            var list = wrapped.All().ToList();
            Assert.AreEqual(2, list.Count);
            Assert.AreEqual(dt.GetMilliseconds(), list[0].Key.Window.StartMs);
            Assert.AreEqual(dt.GetMilliseconds(), list[1].Key.Window.StartMs);
            Assert.AreEqual("coucou", list[0].Key.Key);
            Assert.AreEqual("coucou-toto", list[1].Key.Key);
            Assert.AreEqual(120, list[0].Value);
            Assert.AreEqual(5, list[1].Value);
        }
    }
}
