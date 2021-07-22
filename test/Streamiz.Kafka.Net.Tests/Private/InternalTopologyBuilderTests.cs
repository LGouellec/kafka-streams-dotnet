using NUnit.Framework;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class InternalTopologyBuilderTests
    {
        [Test]
        public void MakeInternalTopicGroupsTest()
        {
            var config = new StreamConfig();
            config.ApplicationId = "MakeInternalTopicGroupsTest";

            var builder = new StreamBuilder();

            var inmemory = InMemory<string, string>.As("table-source");
            inmemory.WithLoggingEnabled(null);
            builder.Table("source", inmemory);

            var topology = builder.Build();
            
            topology.Builder.RewriteTopology(config);
            topology.Builder.BuildTopology();

            var topicsGroups = topology.Builder.MakeInternalTopicGroups();
        }
    }
}
