using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Stream.Internal;
using Streamiz.Kafka.Net.Table.Internal.Graph;
using Streamiz.Kafka.Net.Table.Internal.Graph.Nodes;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Table.Internal
{
    internal class TableJoinBuilder
    {
        private readonly InternalStreamBuilder builder;
        private readonly bool leftOuter;
        private readonly bool rightOuter;

        public TableJoinBuilder(InternalStreamBuilder builder, bool leftOuter, bool rightOuter)
        {
            this.builder = builder;
            this.leftOuter = leftOuter;
            this.rightOuter = rightOuter;
        }

        public IKTable<K, VR> Join<K, V, V0, VR>(
            IKTable<K, V> tableLeft,
            IKTable<K, V0> tableRight,
            IValueJoiner<V, V0, VR> joiner,
            string named,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materializedInternal)
        {
            var renamed = new Named(named);
            var joinMergeName = renamed.OrElseGenerateWithPrefix(builder, KTable.MERGE_NAME);
            ISet<string> allSourceNodes = new HashSet<string>((tableLeft as AbstractStream<K, V>).SetSourceNodes);
            allSourceNodes.AddRange((tableRight as AbstractStream<K, V0>).SetSourceNodes);
            materializedInternal.UseProvider(builder, $"{joinMergeName}-");

            if (leftOuter)
            {
                (tableLeft as IKTableGetter<K, V>)?.EnableSendingOldValues();
            }

            if (rightOuter)
            {
                (tableRight as IKTableGetter<K, V0>)?.EnableSendingOldValues();
            }

            AbstractKTableKTableJoin<K, VR, V, V0> joinLeft = null;
            AbstractKTableKTableJoin<K, VR, V0, V> joinRight = null;

            if (!leftOuter) // INNER JOIN
            {
                joinLeft = new KTableKTableInnerJoin<K, VR, V, V0>((tableLeft as IKTableGetter<K, V>), (tableRight as IKTableGetter<K, V0>), joiner);
                joinRight = new KTableKTableInnerJoin<K, VR, V0, V>((tableRight as IKTableGetter<K, V0>), (tableLeft as IKTableGetter<K, V>), joiner.Reverse());
            }
            else if (!rightOuter) // LEFT JOIN
            {
                joinLeft = new KTableKTableLeftJoin<K, VR, V, V0>((tableLeft as IKTableGetter<K, V>), (tableRight as IKTableGetter<K, V0>), joiner);
                joinRight = new KTableKTableRightJoin<K, VR, V0, V>((tableRight as IKTableGetter<K, V0>), (tableLeft as IKTableGetter<K, V>), joiner.Reverse());
            }
            else // OUTER JOIN
            {
                joinLeft = new KTableKTableOuterJoin<K, VR, V, V0>((tableLeft as IKTableGetter<K, V>), (tableRight as IKTableGetter<K, V0>), joiner);
                joinRight = new KTableKTableOuterJoin<K, VR, V0, V>((tableRight as IKTableGetter<K, V0>), (tableLeft as IKTableGetter<K, V>), joiner.Reverse());
            }

            var joinLeftName = renamed.SuffixWithOrElseGet("-join-this", builder, KTable.JOINTHIS_NAME);
            var joinRigthName = renamed.SuffixWithOrElseGet("-join-other", builder, KTable.JOINOTHER_NAME);

            var joinLeftProcessorParameters = new TableProcessorParameters<K, V>(joinLeft, joinLeftName);
            var joinRightProcessorParameters = new TableProcessorParameters<K, V0>(joinRight, joinRigthName);

            if (materializedInternal.KeySerdes == null && tableLeft is AbstractStream<K, V>)
            {
                materializedInternal.WithKeySerdes((tableLeft as AbstractStream<K, V>).KeySerdes);
            }

            ISerDes<K> keySerdes = materializedInternal.KeySerdes;
            ISerDes<VR> ValueSerdes = materializedInternal.ValueSerdes;
            string queryableStoreName = materializedInternal.QueryableStoreName;
            StoreBuilder<ITimestampedKeyValueStore<K, VR>> storeBuilder = queryableStoreName != null ? new TimestampedKeyValueStoreMaterializer<K, VR>(materializedInternal).Materialize() : null;

            var tableNode = new KTableKTableJoinNode<K, V, V0, VR>(
                joinMergeName,
                joinLeftProcessorParameters,
                joinRightProcessorParameters,
                (tableLeft as AbstractStream<K, V>).NameNode,
                (tableRight as AbstractStream<K, V0>).NameNode,
                (tableLeft as IKTableGetter<K, V>).ValueGetterSupplier.StoreNames,
                (tableRight as IKTableGetter<K, V0>).ValueGetterSupplier.StoreNames,
                queryableStoreName,
                storeBuilder);

            builder.AddGraphNode((tableLeft as AbstractStream<K, V>).Node, tableNode);

            return new KTable<K, VR, VR>(
                tableNode.streamGraphNode,
                keySerdes,
                ValueSerdes,
                allSourceNodes.ToList(),
                queryableStoreName,
                tableNode.JoinMergeProcessorSupplier,
                tableNode,
                builder);
        }
    }
}
