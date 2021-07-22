using System.Collections.Generic;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream.Internal;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;

namespace Streamiz.Kafka.Net.Table.Internal
{
    internal class KTableGrouped<K, KR, V, VR> : KTable<KR, V, VR>
    {
        private readonly IProcessorSupplier<K, V> processorSupplier;

        internal KTableGrouped(string name, ISerDes<KR> keySerde, ISerDes<VR> valSerde, List<string> sourceNodes, string queryableStoreName, IProcessorSupplier<K, V> processorSupplier, StreamGraphNode streamsGraphNode, InternalStreamBuilder builder)
            : base(name, keySerde, valSerde, sourceNodes, queryableStoreName, (IProcessorSupplier<KR, V>)null, streamsGraphNode, builder)
        {
            this.processorSupplier = processorSupplier;
        }

        public override IKTableValueGetterSupplier<KR, VR> ValueGetterSupplier
        {
            get
            {
                if (processorSupplier == null)
                    return base.ValueGetterSupplier;
                else if (processorSupplier is IKStreamAggProcessorSupplier<K, KR, V, VR>)
                    return ((IKStreamAggProcessorSupplier<K, KR, V, VR>)processorSupplier).View();
                else
                    return null;
            }
        }

        public override void EnableSendingOldValues()
        {
            if (!SendOldValues)
            {
                if (processorSupplier == null)
                    base.EnableSendingOldValues();
                else
                {
                    if (processorSupplier is IKStreamAggProcessorSupplier<KR, VR>)
                    {
                        ((IKStreamAggProcessorSupplier<KR, VR>)processorSupplier).EnableSendingOldValues();
                    }
                    SendOldValues = true;
                }
            }
        }
    }
}
