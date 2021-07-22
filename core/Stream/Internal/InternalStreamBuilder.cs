using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Table.Internal;
using Streamiz.Kafka.Net.Table.Internal.Graph;
using Streamiz.Kafka.Net.Table.Internal.Graph.Nodes;
using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    internal class InternalStreamBuilder : INameProvider
    {
        private static string TABLE_SOURCE_SUFFIX = "-source";

        private int index = -1;
        private static readonly object _locker = new object();
        private readonly ILogger logger = Logger.GetLogger(typeof(InternalStreamBuilder));

        internal IList<StreamGraphNode> nodes = new List<StreamGraphNode>();
        internal readonly RootNode root = new RootNode();

        internal readonly InternalTopologyBuilder internalTopologyBuilder;

        internal InternalStreamBuilder(InternalTopologyBuilder internalTopologyBuilder)
        {
            this.internalTopologyBuilder = internalTopologyBuilder;
        }

        #region Build Stream

        private int NextIndex { get { lock (_locker) return ++index; } }

        internal KStream<K, V> Stream<K, V>(string topic, ConsumedInternal<K, V> consumed)
        {
            var name = new Named(consumed.Named).OrElseGenerateWithPrefix(this, KStream.SOURCE_NAME);
            var node = new StreamSourceNode<K,V>(topic, name, consumed);
            this.AddGraphNode(root, node);
            KStream<K, V> stream = new KStream<K, V>(name, consumed.KeySerdes, consumed.ValueSerdes, new List<string> { name }, node, this);
            return stream;
        }

        public string NewProcessorName(string prefix)
        {
            return $"{prefix}{NextIndex.ToString("D10")}";
        }
  
        #endregion

        #region Build Table

        public string NewStoreName(string prefix)
        {
            return $"{prefix}{KTable.STATE_STORE_NAME}{NextIndex.ToString("D10")}";
        }

        internal IKTable<K, V> Table<K, V>(string topic, ConsumedInternal<K, V> consumed, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            var sourceName = new Named(consumed.Named).SuffixWithOrElseGet(TABLE_SOURCE_SUFFIX, this, KStream.SOURCE_NAME);
            var tableSourceName = new Named(consumed.Named).OrElseGenerateWithPrefix(this, KTable.SOURCE_NAME);

            KTableSource<K, V> tableSource = new KTableSource<K,V>(materialized.StoreName , materialized.QueryableStoreName);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K,V>(tableSource, tableSourceName);

            var tableSourceNode = new TableSourceNode<K, V, IKeyValueStore<Bytes, byte[]>>(
                topic, tableSourceName, sourceName, consumed,
                materialized, processorParameters, false);

            this.AddGraphNode(root, tableSourceNode);

            return new KTable<K,V,V>(tableSourceName,
                                    consumed.KeySerdes,
                                    consumed.ValueSerdes,
                                    new List<string> { sourceName },
                                    materialized.QueryableStoreName,
                                    tableSource,
                                    tableSourceNode,
                                    this);
        }

        #endregion

        #region Build GlobalTable

        internal IGlobalKTable<K, V> GlobalTable<K, V>(string topic, ConsumedInternal<K, V> consumed, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
        {
            if (string.IsNullOrEmpty(topic))
            {
                throw new ArgumentException("topic can't be null or empty", nameof(topic));
            }

            // explicitly disable logging for global stores
            materialized.WithLoggingDisabled();

            string sourceName = new Named(consumed.Named).SuffixWithOrElseGet(TABLE_SOURCE_SUFFIX, this, KStream.SOURCE_NAME);
            string tableSourceName = new Named(consumed.Named).OrElseGenerateWithPrefix(this, KTable.SOURCE_NAME);
            string storeName = materialized.StoreName;

            // enforce store name as queryable name to always materialize global table stores
            var tableSource = new KTableSource<K, V>(storeName, storeName);
            var processorParameters = new ProcessorParameters<K, V>(tableSource, tableSourceName);

            var tableSourceNode = new TableSourceNode<K, V, IKeyValueStore<Bytes, byte[]>>(
                topic, tableSourceName, sourceName, consumed,
                materialized, processorParameters, true);

            this.AddGraphNode(root, tableSourceNode);

            return new GlobalKTable<K, V>(new KTableSourceValueGetterSupplier<K, V>(storeName), materialized.QueryableStoreName);
        }

        #endregion

        #region Build Topology

        internal void Build()
        {
            internalTopologyBuilder.BuildAndOptimizeTopology(root, nodes);
        }

        internal void AddGraphNode(List<StreamGraphNode> rootNodes, StreamGraphNode node)
        {
            if (rootNodes.Count == 0)
                throw new TopologyException("Parent node collection can't be empty");

            foreach (var p in rootNodes)
                AddGraphNode(p, node);
        }

        internal void AddGraphNode(StreamGraphNode root, StreamGraphNode node)
        {
            logger.LogDebug("Adding node {Node} in root node {Root}", node, root);
            root.AppendChild(node);
            nodes.Add(node);
        }


        #endregion
    }
}