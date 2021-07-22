using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.Stream.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.Processors
{
    internal abstract class AbstractTask : ITask
    {
        protected readonly IStreamConfig configuration;
        protected IConsumer<byte[], byte[]> consumer;
        protected bool taskInitialized;
        protected bool commitNeeded = false;
        protected bool commitRequested = false;
        protected IStateManager stateMgr;
        protected ILogger log;
        protected readonly string logPrefix = "";

        // For testing
        internal AbstractTask() { }

        protected AbstractTask(TaskId id, IEnumerable<TopicPartition> partition, ProcessorTopology topology, IConsumer<byte[], byte[]> consumer, IStreamConfig config)
        {
            log = Logger.GetLogger(GetType());
            logPrefix = $"stream-task[{id.Id}|{id.Partition}] ";

            Partition = partition;
            Id = id;
            Topology = topology;

            this.consumer = consumer;
            configuration = config;

            stateMgr = new ProcessorStateManager(
                id,
                partition,
                topology.StoresToTopics);
        }

        public ProcessorTopology Topology { get; }

        public ProcessorContext Context { get; protected set; }

        public virtual TaskId Id { get; }

        public IEnumerable<TopicPartition> Partition { get; }

        public ICollection<TopicPartition> ChangelogPartitions { get; internal set; }

        public bool HasStateStores => !(Topology.StateStores.Count == 0);

        public string ApplicationId => configuration.ApplicationId;

        public bool CommitNeeded => commitNeeded;
        public bool CommitRequested => commitRequested;

        public bool IsClosed { get; protected set; }

        public void RequestCommit() => commitRequested = true;

        public bool IsPersistent
             => stateMgr.StateStoreNames
                    .Select(n => stateMgr.GetStore(n))
                    .Any((s) => s.Persistent);

        #region Abstract

        public abstract PartitionGrouper Grouper { get; }
        public abstract bool CanProcess(long now);
        public abstract void Close();
        public abstract void Commit();
        public abstract IStateStore GetStore(string name);
        public abstract void InitializeTopology();
        public abstract bool InitializeStateStores();
        public abstract void Resume();
        public abstract void Suspend();

        #endregion

        protected void RegisterStateStores()
        {
            if (!Topology.StateStores.Any() && !Topology.GlobalStateStores.Any())
            {
                return;
            }

            log.LogDebug("{LogPrefix}Initializing state stores", logPrefix);

            foreach (var kv in Topology.StateStores)
            {
                var store = kv.Value;
                log.LogDebug("{LogPrefix}Initializing store {Key}", logPrefix, kv.Key);
                store.Init(Context, store);
            }

            foreach (var kv in Topology.GlobalStateStores.Where(k => !Topology.StateStores.ContainsKey(k.Key)))
            {
                var store = kv.Value;
                log.LogDebug("{LogPrefix}Initializing store {Key}", logPrefix, kv.Key);
                store.Init(Context, store);
            }
        }

        protected virtual void FlushState()
        {
            try
            {
                stateMgr.Flush();
            }
            catch (Exception e)
            {
                log.LogError(e, "{LogPrefix}Error during flush state store with exception :", logPrefix);
                throw;
            }
        }

        protected void CloseStateManager()
        {
            log.LogDebug("{LogPrefix}Closing state manager", logPrefix);
            try
            {
                stateMgr.Close();
            }
            catch (Exception e)
            {
                log.LogError(e, "{LogPrefix}Error during closing state store with exception :", logPrefix);
                throw;
            }
        }
    }
}
