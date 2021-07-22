using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class TableTableMergeJoinTests
    {
        [Test]
        public void TableTableMergeJoinSendOldValues()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-table-table-left-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table1 = builder.Table("users", InMemory<string, string>.As("store-users"));
            var table2 = builder.Table("regions", InMemory<string, string>.As("store-regions"));
            var table3 = builder.Table("country", InMemory<string, string>.As("store-country"));
            var stream = builder.Stream<string, string>("orders");

            var tableJoin = table1.LeftJoin(table2, (v1, v2) => $"{v1}-{v2 ?? "?"}");

            var tableJoin2 = tableJoin.LeftJoin(table3, (v1, v2) => $"{v1}-{v2 ?? "?"}");

            stream
                .Join(tableJoin2,
                (order, ur) => $"Order:{order}|UserRegionCountry:{ur}")
                .To("topic-output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic1 = driver.CreateInputTopic<string, string>("users");
                var inputTopic2 = driver.CreateInputTopic<string, string>("regions");
                var inputTopic3 = driver.CreateInputTopic<string, string>("orders");
                var inputTopic4 = driver.CreateInputTopic<string, string>("country");
                var outputTopic = driver.CreateOuputTopic<string, string>("topic-output");

                inputTopic1.PipeInput("sylvain", "sylvain");
                inputTopic2.PipeInput("sylvain", "Europe");
                inputTopic4.PipeInput("sylvain", "France");
                inputTopic3.PipeInput("sylvain", "iPhone12Pro");

                var records = outputTopic.ReadKeyValuesToMap();
                Assert.IsNotNull(records);
                Assert.AreEqual(1, records.Count);
                Assert.AreEqual("Order:iPhone12Pro|UserRegionCountry:sylvain-Europe-France", records["sylvain"]);
            }
        }


        [Test]
        public void TableTableMergeJoinForwardSendOldValues()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-table-table-left-join"
            };

            StreamBuilder builder = new StreamBuilder();

            var table1 = builder.Table("users", InMemory<string, string>.As("store-users"));
            var table2 = builder.Table("regions", InMemory<string, string>.As("store-regions"));
            var table3 = builder.Table("country", InMemory<string, string>.As("store-country"));
            var stream = builder.Stream<string, string>("orders");

            var tableJoin = table1.LeftJoin(table2, (v1, v2) => $"{v1}-{v2 ?? "?"}", InMemory<string, string>.As("merge1"));

            var tableJoin2 = tableJoin.LeftJoin(table3, (v1, v2) => $"{v1}-{v2 ?? "?"}");

            stream
                .Join(tableJoin2,
                (order, ur) => $"Order:{order}|UserRegionCountry:{ur}")
                .To("topic-output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic1 = driver.CreateInputTopic<string, string>("users");
                var inputTopic2 = driver.CreateInputTopic<string, string>("regions");
                var inputTopic3 = driver.CreateInputTopic<string, string>("orders");
                var inputTopic4 = driver.CreateInputTopic<string, string>("country");
                var outputTopic = driver.CreateOuputTopic<string, string>("topic-output");

                inputTopic1.PipeInput("sylvain", "sylvain");
                inputTopic2.PipeInput("sylvain", "Europe");
                inputTopic4.PipeInput("sylvain", "France");
                inputTopic3.PipeInput("sylvain", "iPhone12Pro");

                var records = outputTopic.ReadKeyValuesToMap();
                Assert.IsNotNull(records);
                Assert.AreEqual(1, records.Count);
                Assert.AreEqual("Order:iPhone12Pro|UserRegionCountry:sylvain-Europe-France", records["sylvain"]);
            }
        }

    }
}