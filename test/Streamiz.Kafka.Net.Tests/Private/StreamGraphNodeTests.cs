using NUnit.Framework;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class StreamGraphNodeTests
    {
        [Test]
        public void AppendChildTest()
        {
            RootNode root = new RootNode();
            var source = new StreamSourceNode<string, string>(
                "topic",
                "source-01",
                new Stream.Internal.ConsumedInternal<string, string>("source-01", new StringSerDes(), new StringSerDes(), null));
            root.AppendChild(source);
            
            Assert.IsFalse(root.IsEmpty);
            Assert.AreEqual(new string[] { "ROOT-NODE" }, source.ParentNodeNames());
        }

        [Test]
        public void RemoveChildTest()
        {
            RootNode root = new RootNode();
            var source = new StreamSourceNode<string, string>(
                "topic",
                "source-01",
                new Stream.Internal.ConsumedInternal<string, string>("source-01", new StringSerDes(), new StringSerDes(), null));
            root.AppendChild(source);
            root.RemoveChild(source);

            Assert.IsTrue(root.IsEmpty);
        }

        [Test]
        public void ClearChildTest()
        {
            RootNode root = new RootNode();
            var source = new StreamSourceNode<string, string>(
                "topic",
                "source-01",
                new Stream.Internal.ConsumedInternal<string, string>("source-01", new StringSerDes(), new StringSerDes(), null));
            root.AppendChild(source);
            root.ClearChildren();

            Assert.IsTrue(root.IsEmpty);
        }
        
        [Test]
        public void WriteTopologyTest() 
        {
            var builder = new InternalTopologyBuilder();
            List<StreamGraphNode> nodes = new List<StreamGraphNode>();

            RootNode root = new RootNode();
            var source = new StreamSourceNode<string, string>(
                "topic",
                "source-01",
                new Stream.Internal.ConsumedInternal<string, string>("source-01", new StringSerDes(), new StringSerDes(), null));
            root.AppendChild(source);
            nodes.Add(source);

            var filterParameters =
                new ProcessorParameters<string, string>(
                    new KStreamFilter<string, string>((k, v) => true, false), "filter-02");
            var filter = new ProcessorGraphNode<string, string>("filter-02", filterParameters);
            source.AppendChild(filter);
            nodes.Add(filter);

            var to = new StreamSinkNode<string, string>(
                new StaticTopicNameExtractor<string, string>("topic2"), "to-03",
                new Stream.Internal.Produced<string, string>(
                    new StringSerDes(),
                    new StringSerDes())
                );
            filter.AppendChild(to);
            nodes.Add(to);

            builder.BuildAndOptimizeTopology(root, nodes);

            Assert.IsTrue(root.AllParentsWrittenToTopology);
            Assert.IsTrue(source.AllParentsWrittenToTopology);
            Assert.IsTrue(filter.AllParentsWrittenToTopology);
            Assert.IsTrue(to.AllParentsWrittenToTopology);

            var topology = builder.BuildTopology();
            Assert.IsTrue(topology.SourceOperators.ContainsKey("source-01"));
            Assert.IsTrue(topology.ProcessorOperators.ContainsKey("filter-02"));
            Assert.IsTrue(topology.SinkOperators.ContainsKey("to-03"));
        }
    }
}
