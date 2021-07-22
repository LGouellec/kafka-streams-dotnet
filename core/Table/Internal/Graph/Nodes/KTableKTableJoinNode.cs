using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;

namespace Streamiz.Kafka.Net.Table.Internal.Graph.Nodes
{
    internal class KTableKTableJoinNode<K, V1, V2, VR> :
        BaseJoinProcessorNode<K, Change<V1>, Change<V2>, Change<VR>>
    {
        private readonly string[] leftStoreNames;
        private readonly string[] rightStoreNames;
        private readonly string queryableStoreName;
        private readonly StoreBuilder<ITimestampedKeyValueStore<K, VR>> storeBuilder;

        public KTableKTableJoinNode(
            string name,
            ProcessorParameters<K, Change<V1>> joinLeftParams,
            ProcessorParameters<K, Change<V2>> joinRightParams,
            string leftJoinSideName,
            string rightJoinSideName,
            string[] leftStoreNames,
            string[] rightStoreNames,
            string queryableStoreName,
            StoreBuilder<ITimestampedKeyValueStore<K, VR>> storeBuilder)
            : base(name, null, joinLeftParams, joinRightParams, null, leftJoinSideName, rightJoinSideName)
        {
            this.leftStoreNames = leftStoreNames;
            this.rightStoreNames = rightStoreNames;
            this.queryableStoreName = queryableStoreName;
            this.storeBuilder = storeBuilder;

            JoinMergeProcessorSupplier = new KTableKTableJoinMerger<K, V1, V2, VR>(
                (IKTableProcessorSupplier<K, V1, VR>)joinLeftParams.Processor,
                (IKTableProcessorSupplier<K, V2, VR>)joinRightParams.Processor,
                queryableStoreName);
            JoinMergeParams = new ProcessorParameters<K, Change<VR>>(JoinMergeProcessorSupplier, name);
        }

        public IProcessorSupplier<K, Change<VR>> JoinMergeProcessorSupplier { get; }

        public override void WriteToTopology(InternalTopologyBuilder builder)
        {
            builder.AddProcessor(JoinLeftParams.ProcessorName, JoinLeftParams.Processor, LeftJoinSideName);
            builder.AddProcessor(JoinRightParams.ProcessorName, JoinRightParams.Processor, RightJoinSideName);
            builder.AddProcessor(JoinMergeParams.ProcessorName, JoinMergeProcessorSupplier, JoinLeftParams.ProcessorName, JoinRightParams.ProcessorName);

            builder.ConnectProcessorAndStateStore(JoinLeftParams.ProcessorName, leftStoreNames);
            builder.ConnectProcessorAndStateStore(JoinRightParams.ProcessorName, rightStoreNames);

            if (storeBuilder != null)
                builder.AddStateStore(storeBuilder, JoinMergeParams.ProcessorName);
        }
    }
}
