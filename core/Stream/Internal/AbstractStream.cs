using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    internal abstract class AbstractStream<K, V>
    {
        public string NameNode { get; protected set; }
        public ISerDes<K> KeySerdes { get; protected set; }
        public ISerDes<V> ValueSerdes { get; protected set; }
        public List<string> SetSourceNodes { get; protected set; }
        public StreamGraphNode Node { get; protected set; }

        protected readonly InternalStreamBuilder builder;


        protected AbstractStream(AbstractStream<K, V> stream)
        {
            NameNode = stream.NameNode;
            builder = stream.builder;
            KeySerdes = stream.KeySerdes;
            ValueSerdes = stream.ValueSerdes;
            SetSourceNodes = stream.SetSourceNodes;
            Node = stream.Node;
        }

        protected AbstractStream(String name,
               ISerDes<K> keySerde,
               ISerDes<V> valSerde,
               List<String> sourceNodes,
               StreamGraphNode streamsGraphNode,
               InternalStreamBuilder builder)
        {
            if (sourceNodes == null || sourceNodes.Count == 0)
            {
                throw new ArgumentException("parameter <sourceNodes> must not be null or empty");
            }

            NameNode = name;
            this.builder = builder;
            KeySerdes = keySerde;
            ValueSerdes = valSerde;
            SetSourceNodes = sourceNodes;
            Node = streamsGraphNode;
        }

        protected ISet<string> EnsureJoinableWith<K1, V1>(AbstractStream<K1, V1> other)
        {
            ISet<string> allSourceNodes = new HashSet<string>();
            allSourceNodes.AddRange(SetSourceNodes);
            allSourceNodes.AddRange(other.SetSourceNodes);

            builder.internalTopologyBuilder.CopartitionSources(allSourceNodes);

            return allSourceNodes;
        }

        protected void CheckIfParamNull(object o, string paramName)
        {
            if (o == null)
                throw new ArgumentNullException(paramName);
        }

        protected static WrappedValueMapperWithKey<K, V, VR> WithKey<VR>(Func<V, VR> valueMapper)
        {
            valueMapper = valueMapper ?? throw new ArgumentNullException(nameof(valueMapper));

            return new WrappedValueMapperWithKey<K, V, VR>((readOnlyKey, value) => valueMapper(value));
        }

        protected static WrappedValueMapperWithKey<K, V, VR> WithKey<VR>(IValueMapper<V, VR> valueMapper)
        {
            valueMapper = valueMapper ?? throw new ArgumentNullException(nameof(valueMapper));

            return new WrappedValueMapperWithKey<K, V, VR>((readOnlyKey, value) => valueMapper.Apply(value));
        }
    }
}
