using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using System;

namespace Streamiz.Kafka.Net.Table.Internal.Graph.Nodes
{
    internal abstract class TableProcessorNode : StreamGraphNode
    {
        protected TableProcessorNode(string streamGraphNode) 
            : base(streamGraphNode)
        {
        }
    }

    internal class TableProcessorNode<K, V, KS, VS> : TableProcessorNode
    {
        private readonly ProcessorParameters<K, Change<V>> processorParameters;
        private readonly StoreBuilder<ITimestampedKeyValueStore<KS, VS>> storeBuilder;
        private readonly String[] storeNames;

        public TableProcessorNode(string name, ProcessorParameters<K, Change<V>> processorParameters, StoreBuilder<ITimestampedKeyValueStore<KS, VS>> storeBuilder)
            : this(name, processorParameters, storeBuilder, null)
        {
        }

        public TableProcessorNode(string name, ProcessorParameters<K, Change<V>> processorParameters, StoreBuilder<ITimestampedKeyValueStore<KS, VS>> storeBuilder, string[] storeNames)
            : base(name)
        {
            this.processorParameters = processorParameters;
            this.storeBuilder = storeBuilder;
            this.storeNames = storeNames != null ? storeNames : new string[0];
        }

        public override void WriteToTopology(InternalTopologyBuilder builder)
        {
            // TODO : 
            string processorName = processorParameters.ProcessorName;
            builder.AddProcessor(processorName, processorParameters.Processor, ParentNodeNames());

            if (storeNames.Length > 0)
            {
                // builder.connectProcessorAndStateStores(processorName, storeNames);
            }

            // TODO: we are enforcing this as a keyvalue store, but it should go beyond any type of stores
            if (storeBuilder != null)
            {
                builder.AddStateStore(storeBuilder, processorName);
            }
        }
    }
}
