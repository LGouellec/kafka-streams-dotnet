using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Stream.Internal;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class InternalTopologyBuilder
    {
        internal class TopologyTopicsInfo
        {
            public IEnumerable<string> SinkTopics { get; internal set; }
            public IEnumerable<string> SourceTopics { get; internal set; }
            public IDictionary<string, InternalTopicConfig> ChangelogTopics { get; internal set; }
        }

        private readonly IDictionary<string, NodeFactory> nodeFactories = new Dictionary<string, NodeFactory>();
        private readonly IDictionary<string, StateStoreFactory> stateFactories = new Dictionary<string, StateStoreFactory>();
        private readonly IDictionary<string, StoreBuilder> globalStateBuilders = new Dictionary<string, StoreBuilder>();
        private readonly IList<string> sourceTopics = new List<string>();
        private readonly ISet<string> globalTopics = new HashSet<string>();
        private readonly QuickUnion<string> nodeGrouper = new QuickUnion<string>();
        private IDictionary<int, ISet<string>> nodeGroups = new Dictionary<int, ISet<string>>();
        private string applicationId;
        private readonly IList<ISet<string>> copartitionSourceGroups = new List<ISet<string>>();

        // map from state store names to this state store's corresponding changelog topic if possible
        private readonly IDictionary<string, string> storesToTopics = new Dictionary<string, string>();
        // map from changelog topic name to its corresponding state store.
        private readonly IDictionary<string, string> topicsToStores = new Dictionary<string, string>();

        internal InternalTopologyBuilder()
        {
        }

        internal IEnumerable<string> GetSourceTopics() => sourceTopics;

        internal IEnumerable<string> GetGlobalTopics() => globalTopics;

        internal IDictionary<string, IStateStore> GlobalStateStores { get; } = new Dictionary<string, IStateStore>();

        internal bool HasNoNonGlobalTopology => !sourceTopics.Any();

        #region Connect

        internal void CopartitionSources(ISet<string> allSourceNodes)
        {
            copartitionSourceGroups.Add(allSourceNodes);
        }

        internal void ConnectProcessorAndStateStore(string processorName, params string[] stateStoreNames)
        {
            foreach (var stateStoreName in stateStoreNames)
            {
                if (globalStateBuilders.ContainsKey(stateStoreName))
                {
                    throw new TopologyException($"Global StateStore {stateStoreName} can be used by a " +
                        $"Processor without being specified; it should not be explicitly passed.");
                }
                if (!stateFactories.ContainsKey(stateStoreName))
                {
                    throw new TopologyException("StateStore " + stateStoreName + " is not added yet.");
                }
                if (!nodeFactories.ContainsKey(processorName))
                {
                    throw new TopologyException("Processor " + processorName + " is not added yet.");
                }

                var stateFactory = stateFactories[stateStoreName];
                foreach(var u in stateFactory.users)
                {
                    nodeGrouper.Unite(u, processorName);
                }

                stateFactory.users.Add(processorName);
                var nodeFactory = nodeFactories[processorName];

                if (nodeFactory is IProcessorNodeFactory)
                {
                    ((IProcessorNodeFactory)nodeFactory).AddStateStore(stateStoreName);
                }
                else
                {
                    throw new TopologyException($"Cannot connect a state store {stateStoreName} to a source node or a sink node.");
                }
            }
        }

        private void ConnectSourceStoreAndTopic(string sourceStoreName, string topic)
        {
            if (storesToTopics.ContainsKey(sourceStoreName))
            {
                throw new TopologyException($"Source store {sourceStoreName} is already added.");
            }
            storesToTopics[sourceStoreName] = topic;
            topicsToStores[topic] = sourceStoreName;
        }

        #endregion

        #region Add Processors / State Store

        internal void AddSourceOperator<K, V>(string topic, string nameNode, ConsumedInternal<K, V> consumed)
        {
            if (string.IsNullOrEmpty(topic))
            {
                throw new TopologyException("You must provide at least one topic");
            }

            if (nodeFactories.ContainsKey(nameNode))
            {
                throw new TopologyException($"Source processor {nameNode} is already added.");
            }

            if (sourceTopics.Contains(topic))
            {
                throw new TopologyException($"Topic {topic} has already been registered by another source.");
            }

            sourceTopics.Add(topic);
            nodeFactories.Add(nameNode,
                new SourceNodeFactory<K, V>(nameNode, topic, consumed.TimestampExtractor, consumed.KeySerdes, consumed.ValueSerdes));
            nodeGrouper.Add(nameNode);
            nodeGroups = null;
        }

        internal void AddSinkOperator<K, V>(ITopicNameExtractor<K, V> topicNameExtractor, string nameNode, Produced<K, V> produced, params string[] previousProcessorNames)
        {
            if (nodeFactories.ContainsKey(nameNode))
            {
                throw new TopologyException($"Sink processor {nameNode} is already added.");
            }

            nodeFactories.Add(nameNode,
                new SinkNodeFactory<K, V>(nameNode, previousProcessorNames, topicNameExtractor, produced.KeySerdes, produced.ValueSerdes));
            nodeGrouper.Add(nameNode);
            nodeGrouper.Unite(nameNode, previousProcessorNames);
            nodeGroups = null;
        }

        internal void AddProcessor<K, V>(string nameNode, IProcessorSupplier<K, V> processor, params string[] previousProcessorNames)
        {
            if (nodeFactories.ContainsKey(nameNode))
            {
                throw new TopologyException($"Processor {nameNode} is already added.");
            }

            nodeFactories.Add(nameNode, new ProcessorNodeFactory<K, V>(nameNode, previousProcessorNames, processor));
            nodeGrouper.Add(nameNode);
            nodeGrouper.Unite(nameNode, previousProcessorNames);
            nodeGroups = null;
        }

        internal void AddStateStore<S>(StoreBuilder<S> storeBuilder, params string[] processorNames)
            where S : IStateStore
        {
            AddStateStore(storeBuilder, false, processorNames);
        }

        internal void AddStateStore<S>(StoreBuilder<S> storeBuilder, bool allowOverride, params string[] processorNames)
            where S : IStateStore
        {
            if (!allowOverride && stateFactories.ContainsKey(storeBuilder.Name))
            {
                throw new TopologyException("StateStore " + storeBuilder.Name + " is already added.");
            }

            stateFactories.Add(storeBuilder.Name, new StateStoreFactory(storeBuilder));

            if (processorNames != null)
            {
                foreach (var processorName in processorNames)
                {
                    ConnectProcessorAndStateStore(processorName, storeBuilder.Name);
                }
            }
        }

        internal void AddGlobalStore<K, V, S>(string topicName,
            StoreBuilder<S> storeBuilder,
            string sourceName,
            ConsumedInternal<K, V> consumed,
            ProcessorParameters<K, V> processorParameters) where S : IStateStore
        {
            string processorName = processorParameters.ProcessorName;

            ValidateGlobalStoreArguments(sourceName, topicName, processorName, processorParameters.Processor, storeBuilder.Name, storeBuilder.LoggingEnabled);
            ValidateTopicNotAlreadyRegistered(topicName);

            var predecessors = new[] { sourceName };

            var nodeFactory = new ProcessorNodeFactory<K, V>(processorName, predecessors, processorParameters.Processor);

            globalTopics.Add(topicName);
            nodeFactories.Add(sourceName, new SourceNodeFactory<K, V>(sourceName, topicName, consumed.TimestampExtractor, consumed.KeySerdes, consumed.ValueSerdes));

            // TODO: ?
            // nodeToSourceTopics.put(sourceName, Arrays.asList(topics));
            nodeGrouper.Add(sourceName);
            nodeFactory.AddStateStore(storeBuilder.Name);
            nodeFactories.Add(processorName, nodeFactory);
            nodeGrouper.Add(processorName);
            nodeGrouper.Unite(processorName, predecessors);
            globalStateBuilders.Add(storeBuilder.Name, storeBuilder);
            ConnectSourceStoreAndTopic(storeBuilder.Name, topicName);
            nodeGroups = null;
        }

        private void ValidateTopicNotAlreadyRegistered(string topicName)
        {
            if (sourceTopics.Contains(topicName) || globalTopics.Contains(topicName))
            {
                throw new TopologyException("Topic " + topicName + " has already been registered by another source.");
            }

            // TODO: ?
            //for (Pattern pattern : nodeToSourcePatterns.values())
            //{
            //    if (pattern.matcher(topic).matches())
            //    {
            //        throw new TopologyException("Topic " + topic + " matches a Pattern already registered by another source.");
            //    }
            //}
        }

        private void ValidateGlobalStoreArguments<K, V>(string sourceName,
                                              string topicName,
                                              string processorName,
                                              IProcessorSupplier<K, V> stateUpdateSupplier,
                                              string storeName,
                                              bool loggingEnabled)
        {
            if (nodeFactories.ContainsKey(sourceName))
            {
                throw new TopologyException($"Processor {sourceName} is already added.");
            }
            if (nodeFactories.ContainsKey(processorName))
            {
                throw new TopologyException($"Processor {processorName} is already added.");
            }
            if (stateFactories.ContainsKey(storeName) || globalStateBuilders.ContainsKey(storeName))
            {
                throw new TopologyException("StateStore " + storeName + " is already added.");
            }
            if (loggingEnabled)
            {
                throw new TopologyException($"StateStore {storeName} for global table must not have logging enabled.");
            }
            if (sourceName.Equals(processorName))
            {
                throw new TopologyException("sourceName and processorName must be different.");
            }
        }

        #endregion

        #region Build

        public ProcessorTopology BuildTopology() => BuildTopology((int?)null);

        public ProcessorTopology BuildTopology(int? id)
        {
            ISet<string> nodeGroup = null;
            if (id.HasValue)
            {
                var groups = NodeGroups();
                if (groups.ContainsKey(id.Value))
                {
                    nodeGroup = NodeGroups()[id.Value];
                }
                else
                {
                    throw new TopologyException($"Subtopology {id.Value} doesn't exist in this topology");
                }
            }
            else
            {
                nodeGroup = NodeGroups().Values.SelectMany(i => i).ToHashSet();
            }

            ISet<string> globalNodeGroups = GlobalNodeGroups;
            nodeGroup = nodeGroup.Where(x => !globalNodeGroups.Contains(x)).ToHashSet();

            return BuildTopology(nodeGroup, null);
        }

        public ProcessorTopology BuildGlobalStateTopology()
        {
            if (!GlobalNodeGroups.Any())
            {
                return null;
            }
            return BuildTopology(GlobalNodeGroups, null);
        }

        public ProcessorTopology BuildTopology(TaskId taskId)
        {
            ISet<string> nodeGroup = null;
            if (taskId != null)
            {
                var groups = NodeGroups();
                if (groups.ContainsKey(taskId.Id))
                {
                    nodeGroup = NodeGroups()[taskId.Id];
                }
                else
                {
                    throw new TopologyException($"Task Id {taskId.Id} doesn't exist in this topology");
                }
            }
            else
            {
                nodeGroup = NodeGroups().Values.SelectMany(i => i).ToHashSet();
            }

            return BuildTopology(nodeGroup, taskId);
        }

        private ProcessorTopology BuildTopology(ISet<string> nodeGroup, TaskId taskId)
        {
            IProcessor rootProcessor = new RootProcessor();
            IDictionary<string, IProcessor> sources = new Dictionary<string, IProcessor>();
            IDictionary<string, IProcessor> sinks = new Dictionary<string, IProcessor>();
            IDictionary<string, IProcessor> processors = new Dictionary<string, IProcessor>();
            IDictionary<string, IStateStore> stateStores = new Dictionary<string, IStateStore>();

            foreach (var nodeFactory in nodeFactories.Values)
            {
                if (nodeGroup == null || nodeGroup.Contains(nodeFactory.Name))
                {
                    var processor = nodeFactory.Build();
                    processors.Add(nodeFactory.Name, processor);

                    if (nodeFactory is IProcessorNodeFactory)
                    {
                        BuildProcessorNode(processors, stateStores, nodeFactory as IProcessorNodeFactory, processor, taskId);
                    }
                    else if (nodeFactory is ISourceNodeFactory)
                    {
                        BuildSourceNode(sources, nodeFactory as ISourceNodeFactory, processor);
                    }
                    else if (nodeFactory is ISinkNodeFactory)
                    {
                        BuildSinkNode(processors, sinks, nodeFactory as ISinkNodeFactory, processor);
                    }
                    else
                    {
                        throw new TopologyException($"Unknown definition class: {nodeFactory.GetType().Name}");
                    }
                }
            }

            foreach (var sourceProcessor in sources.Values)
            {
                rootProcessor.AddNextProcessor(sourceProcessor);
            }

            var storesToChangelog = new Dictionary<string, string>(storesToTopics.Where(e => stateStores.ContainsKey(e.Key)));
            return new ProcessorTopology(rootProcessor, sources, sinks, processors, stateStores, GlobalStateStores, storesToChangelog);
        }

        private void BuildSinkNode(IDictionary<string, IProcessor> processors, IDictionary<string, IProcessor> sinks, ISinkNodeFactory factory, IProcessor processor)
        {
            foreach (var predecessor in factory.Previous)
            {
                processors[predecessor].AddNextProcessor(processor);
            }

            sinks.Add(factory.Name, processor);
        }

        private void BuildSourceNode(IDictionary<string, IProcessor> sources, ISourceNodeFactory factory, IProcessor processor)
        {
            sources.Add(factory.Name, processor);
        }

        private void BuildProcessorNode(IDictionary<string, IProcessor> processors, IDictionary<string, IStateStore> stateStores, IProcessorNodeFactory factory, IProcessor processor, TaskId taskId)
        {
            foreach (string predecessor in factory.Previous)
            {
                IProcessor predecessorNode = processors[predecessor];
                predecessorNode.AddNextProcessor(processor);
            }

            foreach (string stateStoreName in factory.StateStores)
            {
                if (!stateStores.ContainsKey(stateStoreName))
                {
                    if (stateFactories.ContainsKey(stateStoreName))
                    {
                        StateStoreFactory stateStoreFactory = stateFactories[stateStoreName];

                        if(stateStoreFactory.LoggingEnabled && !storesToTopics.ContainsKey(stateStoreName))
                        {
                            string changelogTopic = ProcessorStateManager.StoreChangelogTopic(applicationId, stateStoreName);
                            storesToTopics.Add(stateStoreName, changelogTopic);
                        }

                        stateStores.Add(stateStoreName, stateStoreFactory.Build(taskId));
                    }
                    else
                    {
                        stateStores.Add(stateStoreName, GlobalStateStores[stateStoreName]);
                    }
                }
            }
        }

        internal void RewriteTopology(IStreamConfig config)
        {
            foreach (var storeBuilder in globalStateBuilders.Values)
            {
                GlobalStateStores.Add(storeBuilder.Name, storeBuilder.Build());
            }
            
            applicationId = config.ApplicationId;
            config.DefaultKeySerDes?.Initialize(new SerDes.SerDesContext(config));
            config.DefaultValueSerDes?.Initialize(new SerDes.SerDesContext(config));
        }

        internal void BuildAndOptimizeTopology(RootNode root, IList<StreamGraphNode> nodes)
        {
            foreach (var node in nodes)
            {
                if (node.AllParentsWrittenToTopology && !node.HasWrittenToTopology)
                {
                    node.WriteToTopology(this);
                    node.HasWrittenToTopology = true;
                }
            }
        }

        private ISet<string> GlobalNodeGroups => nodeGroups
        .Where(group => group.Value.Any(IsGlobalSource))
        .SelectMany(group => group.Value)
        .ToHashSet();

        private bool IsGlobalSource(string node)
        {
            var factory = nodeFactories[node];
            if (factory is ISourceNodeFactory)
            {
                return globalTopics.Contains(((ISourceNodeFactory)factory).Topic);
            }
            return false;
        }


        #endregion

        #region Make Groups

        internal IDictionary<int, ISet<string>> NodeGroups()
        {
            if (nodeGroups == null)
            {
                nodeGroups = MakeNodeGroups();
            }

            return nodeGroups;
        }

        private IDictionary<int, ISet<string>> MakeNodeGroups()
        {
            IDictionary<int, ISet<string>> groups = new Dictionary<int, ISet<string>>();
            IDictionary<string, ISet<string>> rootToNodeGroup = new Dictionary<string, ISet<string>>();

            int nodeGroupId = 0;

            foreach (var nodeName in nodeFactories.Keys)
            {
                nodeGroupId = PutNodeGroupName(nodeName, nodeGroupId, groups, rootToNodeGroup);
            }

            return groups;
        }

        private int PutNodeGroupName(string nodeName, int nodeGroupId, IDictionary<int, ISet<string>> groups, IDictionary<string, ISet<string>> rootToNodeGroup)
        {
            int newNodeGroupId = nodeGroupId;
            string root = nodeGrouper.Root(nodeName);
            ISet<string> nodeGroup = rootToNodeGroup.ContainsKey(root) ? rootToNodeGroup[root] : null;
            if (nodeGroup == null)
            {
                nodeGroup = new HashSet<string>();
                rootToNodeGroup.Add(root, nodeGroup);
                groups.Add(newNodeGroupId++, nodeGroup);
            }
            nodeGroup.Add(nodeName);
            return newNodeGroupId;
        }

        internal IDictionary<int, TopologyTopicsInfo> MakeInternalTopicGroups()
        {
            IDictionary<int, TopologyTopicsInfo> topicGroups = new Dictionary<int, TopologyTopicsInfo>();

            var nodeGroups = NodeGroups();

            foreach(var entry in nodeGroups)
            {
                List<string> sinkTopics = new List<string>();
                List<string> sourceTopics = new List<string>();
                Dictionary<string, InternalTopicConfig> changelogTopics = new Dictionary<string, InternalTopicConfig>();

                foreach(var node in entry.Value)
                {
                    var nodeFactory = nodeFactories[node];
                    switch (nodeFactory)
                    {
                        case ISourceNodeFactory sourceNodeFactory:
                            sourceTopics.Add(sourceNodeFactory.Topic);
                            break;
                        case ISinkNodeFactory sinkNodeFactory:
                            sinkTopics.AddIfNotNull(sinkNodeFactory.Topic);
                            break;
                        case IProcessorNodeFactory processorNodeFactory:
                            foreach(var store in processorNodeFactory.StateStores)
                            {
                                if (storesToTopics.ContainsKey(store))
                                {
                                    var changelogTopic = storesToTopics[store];
                                    if (!changelogTopics.ContainsKey(changelogTopic))
                                    {
                                        if (stateFactories.ContainsKey(store))
                                        {
                                            var internalTopicConfig = CreateChangelogTopic(stateFactories[store], changelogTopic);
                                            changelogTopics.Add(changelogTopic, internalTopicConfig);
                                        }
                                    }
                                }
                            }
                            break;
                    }
                }

                if (sourceTopics.Any())
                {
                    topicGroups.Add(entry.Key, new TopologyTopicsInfo
                    {
                        ChangelogTopics = changelogTopics,
                        SinkTopics = sinkTopics,
                        SourceTopics = sourceTopics
                    });
                }
            }

            return topicGroups;
        }

        #endregion

        #region Describe

        internal ITopologyDescription Describe()
        {
            var topologyDes = new TopologyDescription();

            foreach (var kp in NodeGroups())
            {
                bool containsGlobalStore = kp.Value.Any(v => IsGlobalSource(v));
                if (!containsGlobalStore)
                {
                    DescribeSubTopology(topologyDes, kp.Key, kp.Value);
                }
                else
                {
                    DescribeGlobalStore(topologyDes, kp.Value, kp.Key);
                }
            }

            return topologyDes;
        }

        private void DescribeSubTopology(TopologyDescription description, int key, ISet<string> values)
        {
            IDictionary<string, NodeDescription> nodesByName = new Dictionary<string, NodeDescription>();
            foreach (var name in values)
            {
                nodesByName.Add(name, nodeFactories[name].Describe());
            }

            foreach (var node in nodesByName.Values)
            {
                foreach (var prev in nodeFactories[node.Name].Previous)
                {
                    var prevNode = nodesByName[prev];
                    node.AddPredecessor(prevNode);
                    prevNode.AddSuccessor(node);
                }
            }

            description.AddSubtopology(new SubTopologyDescription(key, nodesByName.Values.ToList<INodeDescription>()));
        }

        private void DescribeGlobalStore(TopologyDescription description, ISet<string> values, int key)
        {
            var enumerator = values.GetEnumerator();
            while (enumerator.MoveNext())
            {
                string node = enumerator.Current;
                if (IsGlobalSource(node))
                {
                    enumerator.MoveNext();
                    string processorNode = enumerator.Current;
                    description.AddGlobalStore(
                        new GlobalStoreDescription(
                            node,
                            processorNode,
                            ((IProcessorNodeFactory)nodeFactories[processorNode]).StateStores[0],
                            ((ISourceNodeFactory)nodeFactories[node]).Topic,
                            key)
                        );
                    break;
                }
            }
        }

        #endregion

        private InternalTopicConfig CreateChangelogTopic(StateStoreFactory stateStoreFactory, string changelogTopic)
        {
            if (stateStoreFactory.IsWindowStore)
            {
                var windowChangelogTopicConfig = new WindowedChangelogTopicConfig()
                {
                    Configs = stateStoreFactory.LogConfig,
                    Name = changelogTopic,
                    RetentionMs = stateStoreFactory.RetentionMs
                };
                return windowChangelogTopicConfig;
            }
            else
            {
                var unknownChangelogTopicConfig = new UnwindowedChangelogTopicConfig()
                {
                    Configs = stateStoreFactory.LogConfig,
                    Name = changelogTopic,
                };
                return unknownChangelogTopicConfig;
            }
        }

        public TaskId GetTaskIdFromPartition(TopicPartition topicPartition)
        {
            var description = Describe();
            var subTopo =
                description
                    .SubTopologies
                    .FirstOrDefault(sub => sub.Nodes.OfType<ISourceNodeDescription>().FirstOrDefault(source => source.Topics.Contains(topicPartition.Topic)) != null);

            if (subTopo != null)
            {
                return new TaskId
                {
                    Id = subTopo.Id,
                    Partition = topicPartition.Partition
                };
            }
            else
            {
                var global =
                    description
                        .GlobalStores
                        .FirstOrDefault(g => g.Source.Topics.Contains(topicPartition.Topic));
                if (global != null)
                {
                    return new TaskId
                    {
                        Id = global.Id,
                        Partition = topicPartition.Partition
                    };
                }
                else
                {
                    throw new TopologyException($"Topic {topicPartition.Topic} doesn't exist in this topology !");
                }
            }
        }
    }
}