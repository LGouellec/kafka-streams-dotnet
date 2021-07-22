using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.Processors
{
    public class DefaultTopicManager : ITopicManager
    {
        private readonly ILogger log = Logger.GetLogger(typeof(DefaultTopicManager));
        private readonly IStreamConfig config;
        private readonly TimeSpan timeout = TimeSpan.FromSeconds(10);

        public DefaultTopicManager(IStreamConfig config, IAdminClient adminClient)
        {
            this.config = config;
            AdminClient = adminClient;
        }

        public IAdminClient AdminClient { get; private set; }

        /// <summary>
        /// <para>
        /// Prepares a set of given internal topics.
        /// </para>
        /// <para>
        /// If a topic does not exist creates a new topic.
        /// If a topic with the correct number of partitions exists ignores it.
        /// If a topic exists already but has different number of partitions we fail and throw exception requesting user to reset the app before restarting again.
        /// </para>
        /// </summary>
        /// <param name="topics">internal topics of topology</param>
        /// <returns>the list of topics which had to be newly created</returns>
        public async Task<IEnumerable<string>> ApplyAsync(IDictionary<string, InternalTopicConfig> topics)
        {
            int maxRetry = 10, i = 0;

            #region inner method

            async Task<IEnumerable<string>> Run()
            {
                log.LogDebug($"Starting to apply internal topics in topic manager (try: {i + 1}, max retry : {maxRetry}).");
                var defaultConfig = new Dictionary<string, string>();
                var topicsNewCreated = new List<string>();
                var topicsToCreate = new List<string>();
                var metadata = AdminClient.GetMetadata(timeout);

                // 1. get source topic partition
                // 2. check if changelog exist, :
                //   2.1 - if yes and partition number exactly same; continue;
                //   2.2 - if yes, and partition number !=; throw Exception
                // 3. if changelog doesn't exist, create it with partition number and configuration
                foreach (var t in topics)
                {
                    var numberPartitions = GetNumberPartitionForTopic(metadata, t.Key);
                    if (numberPartitions == 0)
                    {
                        // Topic need to create
                        topicsToCreate.Add(t.Key);
                    }
                    else
                    {
                        if (numberPartitions == t.Value.NumberPartitions)
                        {
                            continue;
                        }
                        else
                        {
                            string msg = $"Existing internal topic {t.Key} with invalid partitions: expected {t.Value.NumberPartitions}, actual: {numberPartitions}. Please clean up invalid topics before processing.";
                            log.LogError(msg);
                            throw new StreamsException(msg);
                        }
                    }
                }

                if (topicsToCreate.Any())
                {
                    var topicSpecifications = topicsToCreate
                        .Select(t => topics[t])
                        .Select(t => new TopicSpecification()
                        {
                            Name = t.Name,
                            NumPartitions = t.NumberPartitions,
                            ReplicationFactor = (short)config.ReplicationFactor,
                            Configs = new Dictionary<string, string>(t.GetProperties(defaultConfig, config.WindowStoreChangelogAdditionalRetentionMs))
                        });

                    await AdminClient.CreateTopicsAsync(topicSpecifications);

                    // Check if topics has been created

                    var metadata2 = AdminClient.GetMetadata(timeout);
                    var intersectCount = metadata2.Topics.Select(t => t.Topic).Intersect(topicsToCreate).Count();
                    if (intersectCount != topicsToCreate.Count)
                    {
                        throw new StreamsException($"All topics has not been created. Please retry to create all topics.");
                    }
                    else
                    {
                        topicsNewCreated.AddRange(topicsToCreate);
                        log.LogDebug("Internal topics has been created : {Topics}",
                            string.Join(", ", topicsNewCreated));
                    }
                }

                log.LogDebug("Complete to apply internal topics in topic manager");
                return topicsNewCreated;
            }

            #endregion
            
            Exception _e = null;
            while (i < maxRetry)
            {
                try
                {
                    return await Run();
                }
                catch (Exception e)
                {
                    ++i;
                    _e = e;
                    log.LogDebug(
                        "Impossible to create all internal topics: {Message}. Maybe an another instance of your application just created them. (try: {Try}, max retry : {MaxTry})",
                        e.Message, i + 1, maxRetry);
                }
            }

            throw new StreamsException(_e);
        }

        public void Dispose()
            => AdminClient.Dispose();

        private int GetNumberPartitionForTopic(Metadata metadata, string topicName)
        {
            var topicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic.Equals(topicName));
            return topicMetadata != null ? topicMetadata.Partitions.Count : 0;
        }

    }
}
