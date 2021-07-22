using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Tests.Helpers;
using System;

namespace Streamiz.Kafka.Net.Tests.Stores
{
    public class RocksDbWindowBytesStoreSupplierTests
    {
        [Test]
        public void Windowing()
        {
            var builder = new StreamBuilder();

            builder.Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(1000))
                .Count(RocksDbWindows<string, long>.As<StringSerDes, Int64SerDes>("rocksdb-w-store"));

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-rocksdb-window-store";
            config.UseRandomRocksDbConfigForTest();

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                DateTime dt = DateTime.Now;
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                inputTopic.PipeInput("key1", "1", dt);
                inputTopic.PipeInput("key2", "2", dt);

                var store = driver.GetWindowStore<string, long>("rocksdb-w-store");
                Assert.IsNotNull(store);
                var k1 = store.FetchAll(dt.AddMinutes(-10), dt.AddMinutes(10)).ToList();

                Assert.AreEqual(2, k1.Count);
                Assert.AreEqual(1L, k1[0].Value);
                Assert.AreEqual(1L, k1[1].Value);
            }
            config.RemoveRocksDbFolderForTest();
        }

        [Test]
        public void StoresPersistentKeyValueStoreTest()
        {
            var builder = new StreamBuilder();

            builder.Stream<string, string>("topic")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(1000))
                .Count(
                      Materialized<string, long, IWindowStore<Bytes, byte[]>>.Create(
                    Streamiz.Kafka.Net.State.Stores.PersistentWindowStore(
                        "rocksdb-w-store",
                        TimeSpan.FromDays(1),
                        TimeSpan.FromSeconds(1))));

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-rocksdb-window-store";
            config.UseRandomRocksDbConfigForTest();
            
            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                DateTime dt = DateTime.Now;
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                inputTopic.PipeInput("abc", "1", dt);
                inputTopic.PipeInput("key1", "1", dt);
                inputTopic.PipeInput("test", "1", dt);

                var store = driver.GetWindowStore<string, long>("rocksdb-w-store");
                Assert.IsNotNull(store);
                var k1 = store.FetchAll(dt.AddMinutes(-10), dt.AddMinutes(10)).ToList();

                Assert.AreEqual(3, k1.Count);
            }
            config.RemoveRocksDbFolderForTest();
        }
    }
}
