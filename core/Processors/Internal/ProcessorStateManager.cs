using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Internal;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class ProcessorStateManager : IStateManager
    {
        internal class StateStoreMetadata
        {
            internal IStateStore Store { get; set; }
            internal TopicPartition ChangelogTopicPartition { get; set; }
            internal Action<ConsumeResult<byte[], byte[]>>  RestoreCallback { get; set; }
            internal Func<ConsumeResult<byte[], byte[]>, ConsumeResult<byte[], byte[]>> RecordConverter { get; set; }

            /// <summary>
            /// indicating the current snapshot of the store as the offset of last changelog record that has been restore in local state store
            /// offset upsated in three ways :
            /// 1 - when loading checkpoint file
            /// 2 - updating with restore records
            /// 3 - when checkpointing the given written offsets from record collector
            /// </summary>
            internal long? Offset;
        }
        internal readonly TaskId taskId;
        
        private static readonly string STATE_CHANGELOG_TOPIC_SUFFIX = "-changelog";

        private readonly ILogger log;
        private readonly string logPrefix;
        private readonly IDictionary<string, StateStoreMetadata> registeredStores = new Dictionary<string, StateStoreMetadata>();
        private readonly IDictionary<string, string> changelogTopics;
        private readonly IOffsetCheckpointManager offsetCheckpointManager;
        private readonly IChangelogRegister changelogRegister;
        private IDictionary<string, IStateStore> globalStateStores = new Dictionary<string, IStateStore>();

        public IEnumerable<TopicPartition> Partition { get; private set; }

        public IEnumerable<string> StateStoreNames => registeredStores.Keys;

        public ICollection<TopicPartition> ChangelogPartitions => ChangelogOffsets.Keys.ToList();

        public IDictionary<TopicPartition, long> ChangelogOffsets =>
            registeredStores.Values
                .Where(s => s.ChangelogTopicPartition != null && s.Store.IsLocally)
                .ToDictionary(s => s.ChangelogTopicPartition, s =>
                {
                    if (s.Offset.HasValue)
                        return s.Offset.Value + 1;
                    return 0L;
                });

        public ProcessorStateManager(
            TaskId taskId,
            IEnumerable<TopicPartition> partition,
            IDictionary<string, string> changelogTopics,
            IChangelogRegister changelogReader,
            IOffsetCheckpointManager offsetCheckpointManager)
        {
            log = Logger.GetLogger(typeof(ProcessorStateManager));
            logPrefix = $"stream-task[{taskId.Id}|{taskId.Partition}] ";
            this.taskId = taskId;
            Partition = partition;

            this.changelogTopics = changelogTopics ?? new Dictionary<string, string>();
            changelogRegister = changelogReader;
            this.offsetCheckpointManager = offsetCheckpointManager;
        }

        public static string StoreChangelogTopic(string applicationId, String storeName)
        {
            return $"{applicationId}-{storeName}{STATE_CHANGELOG_TOPIC_SUFFIX}";
        }

        private bool IsChangelogStateStore(string storeName)
            => changelogTopics.ContainsKey(storeName);

        private TopicPartition GetStorePartition(string storeName) 
            => new(changelogTopics[storeName], taskId.Partition);

        #region State Manager IMPL

        public void Flush()
        {
            log.LogDebug("{LogPrefix}Flushing all stores registered in the state manager", logPrefix);

            foreach (var state in registeredStores)
            {
                log.LogDebug("{LogPrefix}Flushing store {StateKey}", logPrefix, state.Key);
                state.Value.Store.Flush();
            }
        }

        public void Register(IStateStore store, Action<ConsumeResult<byte[], byte[]>>  callback)
        {
            string storeName = store.Name;
            log.LogDebug("{LogPrefix}Registering state store {StoreName} to its state manager", logPrefix, storeName);

            if (registeredStores.ContainsKey(storeName))
            {
                throw new ArgumentException($"{logPrefix} Store {storeName} has already been registered.");
            }
            
            
            var metadata = IsChangelogStateStore(storeName) ?
                new StateStoreMetadata
                {
                    Store = store,
                    ChangelogTopicPartition = GetStorePartition(storeName),
                    RestoreCallback = callback,
                    RecordConverter = StateManagerTools.ConverterForStore(store),
                    Offset = null
                } :
                new StateStoreMetadata
                {
                    Store = store,
                    Offset = null
                };

            registeredStores.Add(storeName, metadata);

            if (IsChangelogStateStore(storeName))
                changelogRegister.Register(GetStorePartition(storeName), this);

            log.LogDebug($"{logPrefix}Registered state store {storeName} to its state manager");
        }

        public void Close()
        {
            log.LogDebug("{LogPrefix}Closing its state manager and all the registered state stores", logPrefix);

            changelogRegister.Unregister(registeredStores.Values.Where(m => m.ChangelogTopicPartition != null).Select(m => m.ChangelogTopicPartition));

            foreach( var state in registeredStores)
            {
                log.LogDebug("{LogPrefix}Closing storage engine {StoreKey}", logPrefix, state.Key);
                state.Value.Store.Close();
            }

            registeredStores.Clear();
        }

        public IStateStore GetStore(string name)
        {
            if (registeredStores.ContainsKey(name))
                return registeredStores[name].Store;

            if (globalStateStores.ContainsKey(name))
                return globalStateStores[name];

            return null;
        }

        public void RegisterGlobalStateStores(IDictionary<string, IStateStore> globalStateStores)
        {
            this.globalStateStores = globalStateStores;
        }

        public TopicPartition GetRegisteredChangelogPartitionFor(string storeName)
        {
            if (registeredStores.ContainsKey(storeName))
            {
                var metadata = registeredStores[storeName];
                if (metadata.ChangelogTopicPartition != null)
                    return metadata.ChangelogTopicPartition;
                throw new IllegalStateException(
                    @$"Registered state store {storeName} does not have a registered 
                        changelog partition. 
                        This may happen if logging is disabled for 
                        the state store.");
            }

            throw new IllegalStateException(
                @$"State store {storeName} for which the registered
                    changelog partition should be retrieved has not
                    been registered");
        }

        public void UpdateChangelogOffsets(IDictionary<TopicPartition, long> writtenOffsets)
        {
            foreach(var kv in writtenOffsets)
            {
                var storeMetadata = GetStoreMetadata(kv.Key);
                if(storeMetadata != null)
                {
                    storeMetadata.Offset = kv.Value;
                    log.LogDebug($"State store {storeMetadata.Store.Name} updated to written offset {kv.Value} at changelog {kv.Key}");
                }
            }
        }

        public void Checkpoint()
        {
            IDictionary<TopicPartition, long> checkpointOffsets = new Dictionary<TopicPartition, long>();
            foreach(var store in registeredStores)
            {
                if (store.Value.ChangelogTopicPartition != null 
                    && store.Value.Store.Persistent
                    && store.Value.Store.IsLocally)
                {
                    checkpointOffsets.Add(store.Value.ChangelogTopicPartition, store.Value.Offset.HasValue ? store.Value.Offset.Value : OffsetCheckpointFile.OFFSET_UNKNOWN);
                }
            }

            log.LogDebug($"{logPrefix}Writting checkpoint");
            try {
                offsetCheckpointManager.Write(taskId, checkpointOffsets);
            }catch(Exception e)
            {
                log.LogWarning($"{logPrefix}Failed to write offset checkpoint. Exception: {e.Message}{Environment.NewLine}{e.StackTrace}");
            }
         }

        public void InitializeOffsetsFromCheckpoint()
        {
            var loadedCheckpoints = offsetCheckpointManager.Read(taskId);

            log.LogDebug($"Loaded offsets from checkpoint manager: {string.Join(",", loadedCheckpoints.Select(c => $"[{c.Key.Topic}-{c.Key.Partition}]-{c.Value}"))}");

            foreach(var kvStore in registeredStores)
            {
                if(kvStore.Value.ChangelogTopicPartition == null || !kvStore.Value.Store.IsLocally)
                {
                    kvStore.Value.ChangelogTopicPartition = null;
                    log.LogInformation($"State store {kvStore.Value.Store.Name} is not logged and hence would not be restored");
                }
                else if (!kvStore.Value.Store.Persistent)
                {
                    log.LogInformation($"Initializing to the starting offset for changelog {kvStore.Value.ChangelogTopicPartition} of in-memory state store {kvStore.Value.Store.Name}");
                }
                else if(kvStore.Value.Offset == null)
                {
                    if (loadedCheckpoints.ContainsKey(kvStore.Value.ChangelogTopicPartition))
                    {
                        long offset = loadedCheckpoints[kvStore.Value.ChangelogTopicPartition];
                        kvStore.Value.Offset = offset != OffsetCheckpointFile.OFFSET_UNKNOWN ? offset : null;
                        log.LogDebug($"State store {kvStore.Value.Store.Name} initialized from checkpoint with offset {offset} at changelog {kvStore.Value.ChangelogTopicPartition}");
                        loadedCheckpoints.Remove(kvStore.Value.ChangelogTopicPartition);
                    }
                    else
                    {
                        log.LogInformation($"State store {kvStore.Value.Store.Name} did not find checkpoint offset, hence would " +
                                $"default to the starting offset at changelog {kvStore.Value.ChangelogTopicPartition}");
                    }
                }
                else
                {
                    loadedCheckpoints.Remove(kvStore.Value.ChangelogTopicPartition);
                    log.LogDebug($"Skipping re-initialization of offset from checkpoint for recycled store {kvStore.Value.Store.Name}");
                }
            }
            
            if (loadedCheckpoints.Any())
            {
                log.LogWarning($"Some loaded checkpoint offsets cannot find their corresponding state stores: {string.Join(",", loadedCheckpoints.Select(c => $"[{c.Key.Topic}-{c.Key.Partition}]-{c.Value}"))}");
            }
        }

        public string ChangelogFor(string storeName)
            => changelogTopics.Get(storeName);
        
        #endregion

        internal StateStoreMetadata GetStoreMetadata(TopicPartition topicPartition)
        {
            foreach (var store in registeredStores)
                if (store.Value.ChangelogTopicPartition != null && store.Value.ChangelogTopicPartition.Equals(topicPartition))
                    return store.Value;
            return null;
        }

        internal void Restore(StateStoreMetadata storeMetadata, IEnumerable<ConsumeResult<byte[], byte[]>> records)
        {
            if (!registeredStores.ContainsKey(storeMetadata.Store.Name))
                throw new IllegalStateException($"Restoring {storeMetadata.Store.Name} store which is not registered in this state manager, this should not happen");

            if (records.Any())
            {
                var listRecords = records.ToList();
                long endOffset = listRecords[listRecords.Count - 1].Offset.Value;
                var convertedRecords = records.Select(r => storeMetadata.RecordConverter(r));

                // TODO : bach restoration behavior
                foreach (var _record in convertedRecords)
                    storeMetadata.RestoreCallback(_record);

                storeMetadata.Offset = endOffset;
            }
        }
    }
}