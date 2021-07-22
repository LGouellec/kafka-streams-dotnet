using Confluent.Kafka.Admin;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal static class InternalTopicManagerUtils
    {
        internal static ILogger log = Logger.GetLogger(typeof(InternalTopicManagerUtils));
        private static readonly TimeSpan timeout = TimeSpan.FromSeconds(10);
        private static readonly ConfigResource brokerConfigResource;

        static InternalTopicManagerUtils()
        {
            brokerConfigResource = new ConfigResource
            {
                Type = ResourceType.Broker
            };
        }

        internal static async Task CreateChangelogTopicsAsync(
            ITopicManager topicManager,
            InternalTopologyBuilder builder)
        {
            var clusterMetadata = topicManager.AdminClient.GetMetadata(timeout);
            brokerConfigResource.Name = clusterMetadata.Brokers[0].BrokerId.ToString();
            var resultsConf = await topicManager.AdminClient.DescribeConfigsAsync(new List<ConfigResource> { brokerConfigResource });
            var internalTopicsGroups = builder.MakeInternalTopicGroups();
            
            foreach (var entry in internalTopicsGroups)
            {
                var topic = clusterMetadata.Topics.FirstOrDefault(t => t.Topic.Equals(entry.Value.SourceTopics.First()));
                if (topic != null)
                {
                    entry
                        .Value
                        .ChangelogTopics
                        .Values
                        .ForEach(c => c.NumberPartitions = topic.Partitions.Count);
                }
                else
                {
                    entry
                        .Value
                        .ChangelogTopics
                        .Values
                        .ForEach(c => c.NumberPartitions = DefaultPartitionNumber(resultsConf));
                }

                await topicManager.ApplyAsync(entry.Value.ChangelogTopics);
            }
        }

        internal static int DefaultPartitionNumber(List<DescribeConfigsResult> configsResults)
        {
            string numPartitionsCst = "num.partitions";
            if (configsResults.First().Entries.ContainsKey(numPartitionsCst))
                return int.Parse(configsResults.First().Entries[numPartitionsCst].Value);
            else
                throw new StreamsException($"Default number partitions unavailable !");
        }
    }
}
