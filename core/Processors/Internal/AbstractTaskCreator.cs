using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal abstract class AbstractTaskCreator<T> where T : ITask
    {
        protected readonly ILogger log;

        protected AbstractTaskCreator()
        {
            log = Logger.GetLogger(GetType());
        }

        public ICollection<T> CreateTasks(IConsumer<byte[], byte[]> consumer, IDictionary<TaskId, IList<TopicPartition>> tasksToBeCreated)
        {
            List<T> createdTasks = new List<T>();
            foreach (var newTaskAndPartitions in tasksToBeCreated)
            {
                T task = CreateTask(consumer, newTaskAndPartitions.Key, newTaskAndPartitions.Value);
                if (task != null)
                {
                    log.LogDebug("Created task {NewTaskAndPartitionsKey} with assigned partition {PartitionValues}", newTaskAndPartitions.Key,
                        string.Join(",", newTaskAndPartitions.Value));
                    createdTasks.Add(task);
                }

            }
            return createdTasks;
        }

        public abstract T CreateTask(IConsumer<byte[], byte[]> consumer, TaskId id, IEnumerable<TopicPartition> partition);
    }
}