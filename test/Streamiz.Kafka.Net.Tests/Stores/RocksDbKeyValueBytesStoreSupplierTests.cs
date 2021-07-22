using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Stores
{
    public class RocksDbKeyValueBytesStoreSupplierTests
    {
        [Test]
        public void KTableSource()
        {
            var builder = new StreamBuilder();

            builder.Table("table-topic", RocksDb<string, string>.As<StringSerDes, StringSerDes>("table-topic-store"));

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-map";
            config.UseRandomRocksDbConfigForTest();

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("table-topic");
                inputTopic.PipeInput("key1", "1");
                inputTopic.PipeInput("key2", "2");

                var store = driver.GetKeyValueStore<string, string>("table-topic-store");
                Assert.IsNotNull(store);
                var resultK1 = store.Get("key1");
                var resultK2 = store.Get("key2");

                Assert.AreEqual("1", resultK1);
                Assert.AreEqual("2", resultK2);
            }
            config.RemoveRocksDbFolderForTest();
        }
    
        [Test]
        public void StoresPersistentKeyValueStoreTest()
        {
            var builder = new StreamBuilder();

            // Same like that : 
            // builder.Table("table-topic", RocksDb<string, string>.As("table-topic-store"));
            builder.Table("table-topic",
                Materialized<string, string, IKeyValueStore<Bytes, byte[]>>.Create(
                    Streamiz.Kafka.Net.State.Stores.PersistentKeyValueStore("table-topic-store")));

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-map";
            config.UseRandomRocksDbConfigForTest();

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("table-topic");
                inputTopic.PipeInput("key1", "1");
                inputTopic.PipeInput("key2", "2");

                var store = driver.GetKeyValueStore<string, string>("table-topic-store");
                Assert.IsNotNull(store);
                var resultK1 = store.Get("key1");
                var resultK2 = store.Get("key2");

                Assert.AreEqual("1", resultK1);
                Assert.AreEqual("2", resultK2);
            }
            config.RemoveRocksDbFolderForTest();
        }
    }
}
