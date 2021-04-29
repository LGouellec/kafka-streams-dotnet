﻿using Confluent.Kafka;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.RocksDb;
using Streamiz.Kafka.Net.State.RocksDb.Internal;
using Streamiz.Kafka.Net.Tests.Helpers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace Streamiz.Kafka.Net.Tests.Stores
{
    public class RocksDbWindowStoreTests
    {
        private static readonly TimeSpan defaultRetention = TimeSpan.FromMinutes(1);
        private static readonly TimeSpan defaultSize = TimeSpan.FromSeconds(10);

        private StreamConfig config = null;
        private RocksDbWindowStore store = null;
        private ProcessorContext context = null;
        private TaskId id = null;
        private TopicPartition partition = null;
        private ProcessorStateManager stateManager = null;
        private Mock<AbstractTask> task = null;

        [SetUp]
        public void Begin()
        {
            config = new StreamConfig();
            config.ApplicationId = $"unit-test-rocksdb-w";
            config.UseRandomRocksDbConfigForTest();

            id = new TaskId { Id = 0, Partition = 0 };
            partition = new TopicPartition("source", 0);
            stateManager = new ProcessorStateManager(id, new List<TopicPartition> { partition });

            task = new Mock<AbstractTask>();
            task.Setup(k => k.Id).Returns(id);

            context = new ProcessorContext(task.Object, config, stateManager);

            store = new RocksDbWindowStore(
                new RocksDbSegmentedBytesStore("test-w-store", (long)defaultRetention.TotalMilliseconds, 5000, new RocksDbWindowKeySchema()),
                (long)defaultSize.TotalMilliseconds);

            store.Init(context, store);
        }

        [TearDown]
        public void End()
        {
            store.Flush();
            stateManager.Close();
            Directory.Delete(Path.Combine(config.StateDir, config.ApplicationId), true);
        }

        [Test]
        public void CreateInMemoryWindowStore()
        {
            Assert.IsTrue(store.Persistent);
            Assert.AreEqual("test-w-store", store.Name);
            Assert.AreEqual(0, store.All().ToList().Count);
        }

        [Test]
        public void PutOneElement()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("test-key"));
            store.Put(key, BitConverter.GetBytes(100), date.GetMilliseconds());
            var r = store.Fetch(key, date.GetMilliseconds());
            Assert.IsNotNull(r);
            Assert.AreEqual(BitConverter.GetBytes(100), r);
        }

        [Test]
        public void PutTwoElementSameKeyDifferentTime()
        {
            var date = DateTime.Now;
            var dt2 = date.AddSeconds(1);
            var key = new Bytes(Encoding.UTF8.GetBytes("test-key"));
            store.Put(key, BitConverter.GetBytes(100), date.GetMilliseconds());
            store.Put(key, BitConverter.GetBytes(150), dt2.GetMilliseconds());
            var r = store.Fetch(key, date.GetMilliseconds());
            Assert.IsNotNull(r);
            Assert.AreEqual(BitConverter.GetBytes(100), r);

            r = store.Fetch(key, dt2.GetMilliseconds());
            Assert.IsNotNull(r);
            Assert.AreEqual(BitConverter.GetBytes(150), r);
        }

        [Test]
        public void PutTwoElementSameKeySameTime()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("test-key"));
            store.Put(key, BitConverter.GetBytes(100), date.GetMilliseconds());
            store.Put(key, BitConverter.GetBytes(300), date.GetMilliseconds());
            var r = store.Fetch(key, date.GetMilliseconds());
            Assert.IsNotNull(r);
            Assert.AreEqual(BitConverter.GetBytes(300), r);
        }

        [Test]
        public void PutTwoElementDifferentKeyDifferentTime()
        {
            var date = DateTime.Now;
            var dt2 = date.AddSeconds(1);
            var key = new Bytes(Encoding.UTF8.GetBytes("test-key"));
            var key2 = new Bytes(Encoding.UTF8.GetBytes("coucou-key"));
            store.Put(key, BitConverter.GetBytes(100), date.GetMilliseconds());
            store.Put(key2, BitConverter.GetBytes(300), dt2.GetMilliseconds());
            var r = store.Fetch(key, date.GetMilliseconds());
            Assert.IsNotNull(r);
            Assert.AreEqual(BitConverter.GetBytes(100), r);

            r = store.Fetch(key, dt2.GetMilliseconds());
            Assert.IsNull(r);

            r = store.Fetch(key2, dt2.GetMilliseconds());
            Assert.IsNotNull(r);
            Assert.AreEqual(BitConverter.GetBytes(300), r);

            r = store.Fetch(key2, date.GetMilliseconds());
            Assert.IsNull(r);
        }

        [Test]
        public void PutTwoElementDifferentKeySameTime()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("test-key"));
            var key2 = new Bytes(Encoding.UTF8.GetBytes("coucou-key"));
            store.Put(key, BitConverter.GetBytes(100), date.GetMilliseconds());
            store.Put(key2, BitConverter.GetBytes(300), date.GetMilliseconds());
            var r = store.Fetch(key, date.GetMilliseconds());
            Assert.IsNotNull(r);
            Assert.AreEqual(BitConverter.GetBytes(100), r);
            r = store.Fetch(key2, date.GetMilliseconds());
            Assert.IsNotNull(r);
            Assert.AreEqual(BitConverter.GetBytes(300), r);
        }

        [Test]
        public void PutElementsAndFetch()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("test-key"));
            var key2 = new Bytes(Encoding.UTF8.GetBytes("coucou-key"));
            store.Put(key, BitConverter.GetBytes(100), date.GetMilliseconds());
            var d1 = date.AddSeconds(1).GetMilliseconds();
            store.Put(key2, BitConverter.GetBytes(300), d1);
            var r = store.FetchAll(date.AddSeconds(-10), date.AddSeconds(20))
                .ToList()
                .OrderBy(kv => kv.Key.Window.StartMs, new LongComparer()).ToList();
            Assert.AreEqual(2, r.Count);
            Assert.AreEqual(key, r[0].Key.Key);
            Assert.AreEqual(BitConverter.GetBytes(100), r[0].Value);
            Assert.AreEqual(defaultSize, r[0].Key.Window.TotalTime);
            Assert.AreEqual(key2, r[1].Key.Key);
            Assert.AreEqual(BitConverter.GetBytes(300), r[1].Value);
            Assert.AreEqual(defaultSize, r[1].Key.Window.TotalTime);
        }

        [Test]
        public void PutElementsWithNullValue()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("test-key"));
            store.Put(key, null, date.GetMilliseconds());
            var r = store.All().ToList();
            Assert.AreEqual(0, r.Count);
        }

        [Test]
        public void PutElementsAndUpdateNullValueSameWindow()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("test-key"));
            var value = Encoding.UTF8.GetBytes("test");
            store.Put(key, value, date.GetMilliseconds());
            store.Put(key, null, date.GetMilliseconds());
            var r = store.All().ToList();
            Assert.AreEqual(0, r.Count);
        }

        [Test]
        public void PutElementsAndUpdateNullValueDifferentWindow()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("test-key"));
            var value = Encoding.UTF8.GetBytes("test");
            store.Put(key, value, date.GetMilliseconds());
            store.Put(key, null, date.AddSeconds(1).GetMilliseconds());
            var r = store.All().ToList();
            Assert.AreEqual(1, r.Count);
            Assert.AreEqual(value, store.Fetch(key, date.GetMilliseconds()));
            Assert.IsNull(store.Fetch(key, date.AddSeconds(1).GetMilliseconds()));
        }

        [Test]
        public void FetchKeyDoesNotExist()
        {
            var date = DateTime.Now;
            Assert.IsNull(store.Fetch(new Bytes(new byte[0]), 100));
        }

        [Test]
        public void FetchRangeDoesNotExist()
        {
            var date = DateTime.Now;
            var it = store.FetchAll(date.AddDays(-1), date.AddDays(1));
            Assert.AreEqual(null, it.Current);
            Assert.IsFalse(it.MoveNext());
            Assert.AreEqual(null, it.Current);
        }

        [Test]
        public void TestRetention()
        {
            var date = DateTime.Now.AddDays(-1);
            store.Put(new Bytes(new byte[1] { 13 }), new byte[0], date.GetMilliseconds());
            Assert.AreEqual(0, store.All().ToList().Count);
        }

        [Test]
        public void TestRetentionWithOpenIt()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("test-key"));
            var value = Encoding.UTF8.GetBytes("test");
            store.Put(key, value, date.GetMilliseconds());
            var it = store.All();
            it.MoveNext();
            Thread.Sleep(2000);
            store.Put(key, value, date.AddSeconds(4).GetMilliseconds());
            var r = it.ToList().Count;
            Assert.AreEqual(0, r);
        }

        [Test]
        public void EmptyKeyValueIteratorTest()
        {
            var dt = DateTime.Now;
            var enumerator = store.FetchAll(dt.AddDays(1), dt);
            Assert.IsFalse(enumerator.MoveNext());
            enumerator.Reset();
            Assert.AreEqual(0, enumerator.ToList().Count);
        }

        [Test]
        public void EmptyWindowStoreIteratorTest()
        {
            var dt = DateTime.Now;
            var enumerator = store.Fetch(new Bytes(null), dt.AddDays(1), dt);
            Assert.IsFalse(enumerator.MoveNext());
            enumerator.Reset();
            Assert.AreEqual(0, enumerator.ToList().Count);
        }
    }
}