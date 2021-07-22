using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Mock.Kafka;
using Streamiz.Kafka.Net.Mock.Pipes;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Streamiz.Kafka.Net.Mock
{
    /// <summary>
    /// This class makes it easier to write tests to verify the behavior of topologies created with <see cref="Topology"/> or
    /// <see cref="StreamBuilder" />.
    /// <para>
    /// <see cref="TopologyTestDriver"/> is <see cref="IDisposable"/>. Be warning to dispose or use <code>using</code> keyword in your unit tests.
    /// </para>
    /// You can test simple topologies that have a single processor, or very complex topologies that have multiple sources,
    /// processors, sinks, or sub-topologies.
    /// Best of all, the class works without a real Kafka broker, so the tests execute very quickly with very little overhead.
    /// <p>
    /// Using the <see cref="TopologyTestDriver"/> in tests is easy: simply instantiate the driver and provide a <see cref="Topology"/>
    /// (cf. <see cref="StreamBuilder.Build()"/>) and <see cref="IStreamConfig"/>, <see cref="CreateInputTopic{K, V}(string, ISerDes{K}, ISerDes{V})"/>
    /// and use a <see cref="TestInputTopic{K, V}"/> to supply an input records to the topology,
    /// and then <see cref="CreateOuputTopic{K, V}(string, TimeSpan, ISerDes{K}, ISerDes{V})"/> and use a <see cref="TestOutputTopic{K, V}"/> to read and
    /// verify any output records by the topology.
    /// </p>
    /// <p>
    /// Although the driver doesn't use a real Kafka broker, it does simulate Kafka Cluster in memory <see cref="MockConsumer"/> and
    /// <see cref="MockProducer"/> that read and write raw {@code byte[]} messages.
    /// </p>
    /// <example>
    /// Driver setup
    /// <code>
    /// static void Main(string[] args)
    /// {
    ///     var config = new StreamConfig&lt;StringSerDes, StringSerDes&gt;();
    ///     config.ApplicationId = "test-test-driver-app";
    ///     
    ///     StreamBuilder builder = new StreamBuilder();
    /// 
    ///     builder.Stream&lt;string, string&gt;("test").Filter((k, v) => v.Contains("test")).To("test-output");
    /// 
    ///     Topology t = builder.Build();
    /// 
    ///     using (var driver = new TopologyTestDriver(t, config))
    ///     {
    ///         var inputTopic = driver.CreateInputTopic&lt;string, string&gt;("test");
    ///         var outputTopic = driver.CreateOuputTopic&lt;string, string&gt;("test-output", TimeSpan.FromSeconds(5));
    ///         inputTopic.PipeInput("test", "test-1234");
    ///         var r = outputTopic.ReadKeyValue();
    ///         // YOU SOULD ASSERT HERE
    ///     }
    /// }
    /// </code>
    /// </example>
    /// </summary>
    public class TopologyTestDriver : IDisposable
    {
        /// <summary>
        /// Test driver behavior mode
        /// </summary>
        public enum Mode
        {
            /// <summary>
            /// Each record in send synchronous. 
            /// This is the default mode use in <see cref="TopologyTestDriver"/>.
            /// </summary>
            SYNC_TASK,
            /// <summary>
            /// A Cluster Kafka is emulated in memory with topic, partitions, etc ...
            /// Also, if you send 1:1, 2:2, 3:3 in a topic, you could have in destination topic this order : 1:1, 3:3, 2:2.
            /// If your unit test use record order, please <see cref="Mode.SYNC_TASK"/>
            /// </summary>
            ASYNC_CLUSTER_IN_MEMORY
        }

        private readonly CancellationTokenSource tokenSource = new CancellationTokenSource();
        private readonly InternalTopologyBuilder topologyBuilder;
        private readonly IStreamConfig configuration;
        private readonly IStreamConfig topicConfiguration;

        private readonly IDictionary<string, IPipeInput> inputs = new Dictionary<string, IPipeInput>();
        private readonly IDictionary<string, IPipeOutput> outputs = new Dictionary<string, IPipeOutput>();

        private readonly IBehaviorTopologyTestDriver behavior = null;

        /// <summary>
        /// Create a new test diver instance.
        /// </summary>
        /// <param name="topology">Topology to be tested</param>
        /// <param name="config">Configuration for topology. One property will be modified : <see cref="IStreamConfig.NumStreamThreads"/> will set to 1</param>
        /// <param name="mode">Topology driver mode</param>
        public TopologyTestDriver(Topology topology, IStreamConfig config, Mode mode = Mode.SYNC_TASK)
            : this(topology.Builder, config, mode)
        { }

        private TopologyTestDriver(InternalTopologyBuilder builder, IStreamConfig config, Mode mode)
            : this(builder, config, mode, null)
        {
        }

        internal TopologyTestDriver(InternalTopologyBuilder builder, IStreamConfig config, Mode mode, IKafkaSupplier supplier)
        {
            topologyBuilder = builder;
            configuration = config;

            // ONLY 1 thread for test driver (use only for ASYNC_CLUSTER_IN_MEMORY)
            configuration.NumStreamThreads = 1;
            configuration.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;

            topicConfiguration = config.Clone();
            topicConfiguration.ApplicationId = $"test-driver-{configuration.ApplicationId}";

            var clientId = string.IsNullOrEmpty(configuration.ClientId) ? $"{configuration.ApplicationId.ToLower()}-{Guid.NewGuid()}" : configuration.ClientId;

            // sanity check
            topologyBuilder.BuildTopology();

            topologyBuilder.RewriteTopology(configuration);

            switch (mode)
            {
                case Mode.SYNC_TASK:
                    behavior = new TaskSynchronousTopologyDriver(
                        clientId,
                        topologyBuilder,
                        configuration,
                        topicConfiguration,
                        supplier,
                        tokenSource.Token);
                    break;
                case Mode.ASYNC_CLUSTER_IN_MEMORY:
                    behavior = new ClusterInMemoryTopologyDriver(
                        clientId,
                        topologyBuilder,
                        configuration,
                        topicConfiguration,
                        supplier,
                        tokenSource.Token);
                    break;
            }

            behavior.StartDriver();
        }
        
        /// <summary>
        /// Close the driver, its topology, and all processors.
        /// </summary>
        public void Dispose()
        {
            tokenSource.Cancel();

            foreach (var k in inputs)
                k.Value.Dispose();

            foreach (var k in outputs)
                k.Value.Dispose();
            
            behavior.Dispose();
        }

        #region Create Input Topic

        /// <summary>
        /// Create <see cref="TestInputTopic{K, V}"/> to be used for piping records to topic.
        /// The key and value serializer as specified in the <see cref="IStreamConfig"/> are used.
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <param name="topicName">the name of the topic</param>
        /// <returns><see cref="TestInputTopic{K, V}"/> instance</returns>
        public TestInputTopic<K, V> CreateInputTopic<K, V>(string topicName)
            => CreateInputTopic<K, V>(topicName, null, null);

        /// <summary>
        /// Create <see cref="TestInputTopic{K, V}"/> to be used for piping records to topic
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <param name="keySerdes">Key serializer</param>
        /// <param name="valueSerdes">Value serializer</param>
        /// <param name="topicName">the name of the topic</param>
        /// <returns><see cref="TestInputTopic{K, V}"/> instance</returns>
        public TestInputTopic<K, V> CreateInputTopic<K, V>(string topicName, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            var input = behavior.CreateInputTopic(topicName, keySerdes, valueSerdes);
            inputs.Add(topicName, input.Pipe);
            return input;
        }

        /// <summary>
        /// Create <see cref="TestInputTopic{K, V}"/> to be used for piping records to topic
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <typeparam name="KS">Key serializer type</typeparam>
        /// <typeparam name="VS">Value serializer type</typeparam>
        /// <param name="topicName">the name of the topic</param>
        /// <returns><see cref="TestInputTopic{K, V}"/> instance</returns>
        public TestInputTopic<K, V> CreateInputTopic<K, V, KS, VS>(string topicName)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => CreateInputTopic(topicName, new KS(), new VS());

        #endregion

        #region Create Output Topic

        /// <summary>
        /// Create <see cref="TestOutputTopic{K, V}"/> to be used for reading records from topic.
        /// The key and value serializer as specified in the <see cref="IStreamConfig"/> are used.
        /// By default, the consume timeout is set to 1 seconds.
        /// </summary>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="V">Value type</typeparam>
        /// <param name="topicName">the name of the topic</param>
        /// <returns><see cref="TestOutputTopic{K, V}"/> instance</returns>
        public TestOutputTopic<K, V> CreateOuputTopic<K, V>(string topicName)
            => CreateOuputTopic<K, V>(topicName, TimeSpan.FromSeconds(1), null, null);

        /// <summary>
        /// Create <see cref="TestOutputTopic{K, V}"/> to be used for reading records from topic.
        /// </summary>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="V">Value type</typeparam>
        /// <param name="topicName">the name of the topic</param>
        /// <param name="consumeTimeout">Consumer timeout</param>
        /// <param name="keySerdes">Key deserializer</param>
        /// <param name="valueSerdes">Value deserializer</param>
        /// <returns><see cref="TestOutputTopic{K, V}"/> instance</returns>
        public TestOutputTopic<K, V> CreateOuputTopic<K, V>(string topicName, TimeSpan consumeTimeout, ISerDes<K> keySerdes = null, ISerDes<V> valueSerdes = null)
        {
            var output = behavior.CreateOutputTopic(topicName, consumeTimeout, keySerdes, valueSerdes);
            outputs.Add(topicName, output.Pipe);
            return output;
        }

        /// <summary>
        /// Create <see cref="TestOutputTopic{K, V}"/> to be used for reading records from topic.
        /// By default, the consume timeout is set to 5 seconds.
        /// </summary>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="V">Value type</typeparam>
        /// <typeparam name="KS">Key serializer type</typeparam>
        /// <typeparam name="VS">Value serializer type</typeparam>
        /// <param name="topicName">the name of the topic</param>
        /// <returns><see cref="TestOutputTopic{K, V}"/> instance</returns>
        public TestOutputTopic<K, V> CreateOuputTopic<K, V, KS, VS>(string topicName)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => CreateOuputTopic<K, V, KS, VS>(topicName, TimeSpan.FromSeconds(5));

        /// <summary>
        /// Create <see cref="TestOutputTopic{K, V}"/> to be used for reading records from topic.
        /// </summary>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="V">Value type</typeparam>
        /// <typeparam name="KS">Key serializer type</typeparam>
        /// <typeparam name="VS">Value serializer type</typeparam>
        /// <param name="topicName">the name of the topic</param>
        /// <param name="consumeTimeout">Consumer timeout</param>
        /// <returns><see cref="TestOutputTopic{K, V}"/> instance</returns>
        public TestOutputTopic<K, V> CreateOuputTopic<K, V, KS, VS>(string topicName, TimeSpan consumeTimeout)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => CreateOuputTopic<K, V>(topicName, consumeTimeout, new KS(), new VS());

        #endregion

        #region Create Multi Input Topic

        /// <summary>
        /// Create <see cref="TestMultiInputTopic{K, V}"/> to be used for piping records to multiple topics in same time.
        /// Need to call <see cref="TestMultiInputTopic{K, V}.Flush"/> at the end of writting !
        /// The key and value serializer as specified in the <see cref="IStreamConfig"/> are used.
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <param name="topics">the list of topics</param>
        /// <returns><see cref="TestMultiInputTopic{K, V}"/> instance</returns>
        public TestMultiInputTopic<K, V> CreateMultiInputTopic<K, V>(params string[] topics)
            => CreateMultiInputTopic<K, V>(null, null, topics);

        /// <summary>
        /// Create <see cref="TestMultiInputTopic{K, V}"/> to be used for piping records to multiple topics in same time.
        /// Need to call <see cref="TestMultiInputTopic{K, V}.Flush"/> at the end of writting !
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <param name="keySerdes">key serializer instance</param>
        /// <param name="valueSerdes">value serializer instance</param>
        /// <param name="topics">the list of topics</param>
        /// <returns><see cref="TestMultiInputTopic{K, V}"/> instance</returns>
        public TestMultiInputTopic<K, V> CreateMultiInputTopic<K, V>(ISerDes<K> keySerdes, ISerDes<V> valueSerdes, params string[] topics)
        {
            var multi = behavior.CreateMultiInputTopic(topics, keySerdes, valueSerdes);
            foreach (var topic in topics)
                inputs.Add(topic, multi.GetPipe(topic));
            return multi;
        }

        /// <summary>
        /// Create <see cref="TestMultiInputTopic{K, V}"/> to be used for piping records to multiple topics in same time.
        /// Need to call <see cref="TestMultiInputTopic{K, V}.Flush"/> at the end of writting !
        /// The key and value serializer as specified in the type parameters.
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <typeparam name="KS">key serializer type</typeparam>
        /// <typeparam name="VS">value serializer type</typeparam>
        /// <param name="topics">the list of topics</param>
        /// <returns><see cref="TestMultiInputTopic{K, V}"/> instance</returns>
        public TestMultiInputTopic<K, V> CreateMultiInputTopic<K, V, KS, VS>(params string[] topics)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => CreateMultiInputTopic(new KS(), new VS(), topics);

        #endregion

        #region Store

        /// <summary>
        /// Get the <see cref="IReadOnlyKeyValueStore{K, V}"/> or <see cref="ITimestampedKeyValueStore{K, V}"/> with the given name.
        /// The store can be a "regular" or global store.
        /// <p>
        /// If the registered store is a <see cref="ITimestampedKeyValueStore{K, V}"/> this method will return a value-only query
        /// interface.
        /// </p>
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <param name="name">the name of the store</param>
        /// <returns>the key value store, or null if no <see cref="IReadOnlyKeyValueStore{K, V}"/> or <see cref="ITimestampedKeyValueStore{K, V}"/> has been registered with the given name</returns>
        public IReadOnlyKeyValueStore<K, V> GetKeyValueStore<K, V>(string name)
        {
            var store = behavior.GetStateStore<K, V>(name);
            if (store is ITimestampedKeyValueStore<K, V>)
                return new ReadOnlyKeyValueStoreFacade<K, V>(store as ITimestampedKeyValueStore<K, V>);
            else if (store is IReadOnlyKeyValueStore<K, V>)
                return (IReadOnlyKeyValueStore<K, V>)store;
            else
                return null;
        }

        /// <summary>
        /// Get the <see cref="IReadOnlyWindowStore{K, V}"/> or <see cref="ITimestampedWindowStore{K, V}"/> with the given name.
        /// The store can be a "regular" or global store.
        /// <p>
        /// If the registered store is a <see cref="ITimestampedWindowStore{K, V}"/> this method will return a value-only query
        /// interface.
        /// </p>
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <param name="name">the name of the store</param>
        /// <returns>the key value store, or null if no <see cref="IReadOnlyWindowStore{K, V}"/> or <see cref="ITimestampedWindowStore{K, V}"/> has been registered with the given name</returns>
        public IReadOnlyWindowStore<K, V> GetWindowStore<K, V>(string name)
        {
            var store = behavior.GetStateStore<K, V>(name);
            if (store is ITimestampedWindowStore<K, V>)
                return new ReadOnlyWindowStoreFacade<K, V>(store as ITimestampedWindowStore<K, V>);
            else if (store is IReadOnlyWindowStore<K, V>)
                return (IReadOnlyWindowStore<K, V>)store;
            else
                return null;
        }

        #endregion

        #region Property

        /// <summary>
        /// Indicate if <see cref="TopologyTestDriver"/> is running or not.
        /// </summary>
        public bool IsRunning => behavior.IsRunning;

        /// <summary>
        /// Indicate if <see cref="TopologyTestDriver"/> is stopped or not.
        /// </summary>
        public bool IsStopped => behavior.IsStopped;

        /// <summary>
        /// Indicate if <see cref="TopologyTestDriver"/> is in error or not.
        /// </summary>
        public bool IsError => behavior.IsError;

        #endregion
    }
}