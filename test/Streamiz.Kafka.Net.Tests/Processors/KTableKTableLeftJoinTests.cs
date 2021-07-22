using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KTableKTableLeftJoinTests
    {
        internal class ValueJoiner : IValueJoiner<string, string, string>
        {
            public string Apply(string value1, string value2)
                => $"{value1}-{value2}";
        }

        [Test]
        public void TableTableLeftJoin()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-table-table-left-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table1 = builder.Table("table1", InMemory<string, string>.As("store1"));
            var table2 = builder.Table("table2", InMemory<string, string>.As("store2"));

            var tableJoin = table1.LeftJoin(table2, (v1, v2) => $"{v1}-{v2}");

            tableJoin.ToStream().To("topic-output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic1 = driver.CreateInputTopic<string, string>("table1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("table2");
                var outputTopic = driver.CreateOuputTopic<string, string>("topic-output");
                inputTopic1.PipeInput("test", "test");
                inputTopic2.PipeInput("test", "coucou");
                inputTopic1.PipeInput("test2", "test2");
                var records = outputTopic.ReadKeyValuesToMap();
                Assert.IsNotNull(records);
                Assert.AreEqual(2, records.Count);
                Assert.AreEqual("test-coucou", records["test"]);
                Assert.AreEqual("test2-", records["test2"]);
            }
        }

        [Test]
        public void TableTableLeftJoinValueJoiner()
        {

            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-table-table-left-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table1 = builder.Table("table1", InMemory<string, string>.As("store1"));
            var table2 = builder.Table("table2", InMemory<string, string>.As("store2"));

            var tableJoin = table1.LeftJoin(table2, new ValueJoiner());

            tableJoin.ToStream().To("topic-output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic1 = driver.CreateInputTopic<string, string>("table1");
                var outputTopic = driver.CreateOuputTopic<string, string>("topic-output");
                inputTopic1.PipeInput("test", "test");
                var record = outputTopic.ReadKeyValuesToMap();
                Assert.IsNotNull(record);
                Assert.AreEqual(1, record.Count);
                Assert.AreEqual("test-", record["test"]);
            }
        }

        [Test]
        public void TableTableLeftJoinStateStore()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-table-table-left-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table1 = builder.Table("table1", InMemory<string, string>.As("store1"));
            var table2 = builder.Table("table2", InMemory<string, string>.As("store2"));

            var tableJoin = table1.LeftJoin(table2, (v1, v2) => $"{v1}-{v2}", InMemory<string, string>.As("merge-store"));

            tableJoin.ToStream().To("topic-output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic1 = driver.CreateInputTopic<string, string>("table1");
                var outputTopic = driver.CreateOuputTopic<string, string>("topic-output");

                inputTopic1.PipeInput("test", "test");

                var st1 = driver.GetKeyValueStore<string, string>("store1");
                var st2 = driver.GetKeyValueStore<string, string>("store2");
                var mergeStore = driver.GetKeyValueStore<string, string>("merge-store");

                Assert.AreEqual(1, st1.ApproximateNumEntries());
                Assert.AreEqual(0, st2.ApproximateNumEntries());
                Assert.AreEqual(1, mergeStore.ApproximateNumEntries());

                Assert.AreEqual("test", st1.Get("test"));
                Assert.IsNull(st2.Get("test"));
                Assert.AreEqual("test-", mergeStore.Get("test"));
            }
        }

        [Test]
        public void TableTableLeftJoinWithtoutStateStore()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-table-table-left-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table1 = builder.Table("table1", InMemory<string, string>.As("store1"));
            var table2 = builder.Table("table2", InMemory<string, string>.As("store2"));

            var tableJoin = table1.LeftJoin(table2, (v1, v2) => $"{v1}-{v2}");

            tableJoin.ToStream().To("topic-output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic1 = driver.CreateInputTopic<string, string>("table1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("table2");
                var outputTopic = driver.CreateOuputTopic<string, string>("topic-output");
                inputTopic1.PipeInput("test", "test");
                inputTopic2.PipeInput("test", "coucou");
                inputTopic1.PipeInput("test2", "test2");
                var records = outputTopic.ReadKeyValuesToMap();
                Assert.IsNotNull(records);
                Assert.AreEqual(2, records.Count);
                Assert.AreEqual("test-coucou", records["test"]);
                Assert.AreEqual("test2-", records["test2"]);
            }
        }

        [Test]
        public void TableTableLeftJoinNullKey()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-table-table-left-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table1 = builder.Table("table1", InMemory<string, string>.As("store1"));
            var table2 = builder.Table("table2", InMemory<string, string>.As("store2"));

            var tableJoin = table1.LeftJoin(table2, (v1, v2) => $"{v1}-{v2}");

            tableJoin.ToStream().To("topic-output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic1 = driver.CreateInputTopic<string, string>("table1");
                var inputTopic2 = driver.CreateInputTopic<string, string>("table2");
                var outputTopic = driver.CreateOuputTopic<string, string>("topic-output");
                inputTopic1.PipeInput("test", "test");
                inputTopic1.PipeInput(null, "test");
                inputTopic2.PipeInput("test", "coucou");
                var records = outputTopic.ReadKeyValuesToMap();
                Assert.IsNotNull(records);
                Assert.AreEqual(1, records.Count);
                Assert.AreEqual("test-coucou", records["test"]);
            }
        }

        [Test]
        public void TableTableLeftJoinGetterSupplier()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-table-table-left-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table1 = builder.Table("users", InMemory<string, string>.As("store-users"));
            var table2 = builder.Table("regions", InMemory<string, string>.As("store-regions"));
            var stream = builder.Stream<string, string>("orders");

            var tableJoin = table1.LeftJoin(table2, (v1, v2) => $"{v1}-{v2 ?? "?"}");

            stream
                .Join(tableJoin, 
                (order, ur) => $"Order:{order}|UserRegion:{ur}")
                .To("topic-output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic1 = driver.CreateInputTopic<string, string>("users");
                var inputTopic2 = driver.CreateInputTopic<string, string>("regions");
                var inputTopic3 = driver.CreateInputTopic<string, string>("orders");
                var outputTopic = driver.CreateOuputTopic<string, string>("topic-output");

                inputTopic1.PipeInput("sylvain", "sylvain");
                inputTopic1.PipeInput("lise", "lise");
                inputTopic2.PipeInput("sylvain", "France");
                inputTopic3.PipeInput("sylvain", "iPhone12Pro");
                inputTopic3.PipeInput("lise", "PixelA4");

                var records = outputTopic.ReadKeyValuesToMap();
                Assert.IsNotNull(records);
                Assert.AreEqual(2, records.Count);
                Assert.AreEqual("Order:iPhone12Pro|UserRegion:sylvain-France", records["sylvain"]);
                Assert.AreEqual("Order:PixelA4|UserRegion:lise-?", records["lise"]);
            }
        }


    }
}
