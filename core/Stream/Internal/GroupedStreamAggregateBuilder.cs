using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Table.Internal;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    internal class GroupedStreamAggregateBuilder<K, V>
    {
        private readonly InternalStreamBuilder builder;
        private readonly Grouped<K, V> grouped;
        private readonly List<string> sourceNodes;
        private readonly string name;
        private readonly StreamGraphNode node;

        public GroupedStreamAggregateBuilder(InternalStreamBuilder builder, Grouped<K, V> grouped, List<string> sourceNodes, string name, StreamGraphNode node)
        {
            this.builder = builder;
            this.grouped = grouped;
            this.sourceNodes = sourceNodes;
            this.name = name;
            this.node = node;
        }

        internal IKTable<K, VR> Build<VR>(
            string functionName,
            StoreBuilder<ITimestampedKeyValueStore<K, VR>> storeBuilder,
            IKStreamAggProcessorSupplier<K, K, V, VR> aggregateSupplier,
            string queryableStoreName,
            ISerDes<K> keySerdes,
            ISerDes<VR> valueSerdes)
        {
            // if repartition required TODO
            // ELSE
            StatefulProcessorNode<K, V, ITimestampedKeyValueStore<K, VR>> statefulProcessorNode =
               new StatefulProcessorNode<K, V, ITimestampedKeyValueStore<K, VR>>(
                   functionName,
                   new ProcessorParameters<K, V>(aggregateSupplier, functionName),
                   storeBuilder);

            builder.AddGraphNode(node, statefulProcessorNode);

            return new KTable<K, V, VR>(functionName,
                                    keySerdes,
                                    valueSerdes,
                                    sourceNodes,
                                    queryableStoreName,
                                    aggregateSupplier,
                                    statefulProcessorNode,
                                    builder);
        }

        internal IKTable<KR, VR> BuildWindow<KR, VR>(
            string functionName,
            StoreBuilder<ITimestampedWindowStore<K, VR>> storeBuilder,
            IKStreamAggProcessorSupplier<K, KR, V, VR> aggregateSupplier,
            string queryableStoreName,
            ISerDes<KR> keySerdes,
            ISerDes<VR> valueSerdes)
        {
            // if repartition required TODO
            // ELSE
            StatefulProcessorNode<K, V, ITimestampedWindowStore<K, VR>> statefulProcessorNode =
               new StatefulProcessorNode<K, V, ITimestampedWindowStore<K, VR>>(
                   functionName,
                   new ProcessorParameters<K, V>(aggregateSupplier, functionName),
                   storeBuilder);

            builder.AddGraphNode(node, statefulProcessorNode);

            return new KTableGrouped<K, KR, V, VR>(functionName,
                                    keySerdes,
                                    valueSerdes,
                                    sourceNodes,
                                    queryableStoreName,
                                    aggregateSupplier,
                                    statefulProcessorNode,
                                    builder);
        }
    }
}
