using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Stream.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using ThreadState = Streamiz.Kafka.Net.Processors.ThreadState;

[assembly: InternalsVisibleTo("Streamiz.Kafka.Net.Tests, PublicKey=00240000048000009400000006020000002400005253413100040000010001000d9d4a8e90a3b987f68f047ec499e5a3405b46fcad30f52abadefca93b5ebce094d05976950b38cc7f0855f600047db0a351ede5e0b24b9d5f1de6c59ab55dee145da5d13bb86f7521b918c35c71ca5642fc46ba9b04d4900725a2d4813639ff47898e1b762ba4ccd5838e2dd1e1664bd72bf677d872c87749948b1174bd91ad")]
[assembly: InternalsVisibleTo("Streamiz.Kafka.Net.Metrics.Prometheus, PublicKey=00240000048000009400000006020000002400005253413100040000010001000d9d4a8e90a3b987f68f047ec499e5a3405b46fcad30f52abadefca93b5ebce094d05976950b38cc7f0855f600047db0a351ede5e0b24b9d5f1de6c59ab55dee145da5d13bb86f7521b918c35c71ca5642fc46ba9b04d4900725a2d4813639ff47898e1b762ba4ccd5838e2dd1e1664bd72bf677d872c87749948b1174bd91ad")]
[assembly: InternalsVisibleTo("Streamiz.Kafka.Net.Metrics.OpenTelemetry, PublicKey=00240000048000009400000006020000002400005253413100040000010001000d9d4a8e90a3b987f68f047ec499e5a3405b46fcad30f52abadefca93b5ebce094d05976950b38cc7f0855f600047db0a351ede5e0b24b9d5f1de6c59ab55dee145da5d13bb86f7521b918c35c71ca5642fc46ba9b04d4900725a2d4813639ff47898e1b762ba4ccd5838e2dd1e1664bd72bf677d872c87749948b1174bd91ad")]
[assembly: InternalsVisibleTo("Streamiz.Kafka.Net.Azure.RemoteStorage, PublicKey=00240000048000009400000006020000002400005253413100040000010001000d9d4a8e90a3b987f68f047ec499e5a3405b46fcad30f52abadefca93b5ebce094d05976950b38cc7f0855f600047db0a351ede5e0b24b9d5f1de6c59ab55dee145da5d13bb86f7521b918c35c71ca5642fc46ba9b04d4900725a2d4813639ff47898e1b762ba4ccd5838e2dd1e1664bd72bf677d872c87749948b1174bd91ad")]
[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2, PublicKey=0024000004800000940000000602000000240000525341310004000001000100c547cac37abd99c8db225ef2f6c8a3602f3b3606cc9891605d02baa56104f4cfc0734aa39b93bf7852f7d9266654753cc297e7d2edfe0bac1cdcf9f717241550e0a7b191195b7667bb4f64bcb8e2121380fd1d9d46ad2d92d2d15605093924cceaf74c4861eff62abf69b9291ed0a340e113be11e6a7d3113e92484cf7045cc7")]

namespace Streamiz.Kafka.Net
{
    /// <summary>
    /// Listen to <see cref="KafkaStream"/> change events.
    /// </summary>
    /// <param name="oldState">previous statenew</param>
    /// <param name="newState">new state</param>
    public delegate void StateListener(KafkaStream.State oldState, KafkaStream.State newState);

    /// <summary>
    /// A Kafka client that allows for performing continuous computation on input coming from one or more input topics and
    /// sends output to zero, one, or more output topics. 
    /// <see cref="KafkaStream"/> is disposable, so please call <see cref="KafkaStream.Dispose"/> when you exit your application,
    /// or using <see cref="CancellationToken"/> token pass to <see cref="KafkaStream.StartAsync(CancellationToken?)"/>.
    /// 
    /// The computational logic can be specified using the <see cref="StreamBuilder"/> which provides the high-level DSL to define
    /// transformations.
    ///
    /// One <see cref="KafkaStream"/> instance can contain one or more threads specified in the configs for the processing work.
    /// 
    /// A <see cref="KafkaStream"/> instance can co-ordinate with any other instances with the same
    /// <see cref="IStreamConfig.ApplicationId"/> (whether in the same process, on other processes on this
    /// machine, or on remote machines) as a single (possibly distributed) stream processing application.
    /// These instances will divide up the work based on the assignment of the input topic partitions so that all partitions
    /// are being consumed.
    /// If instances are added or fail, all (remaining) instances will rebalance the partition assignment among themselves
    /// to balance processing load and ensure that all input topic partitions are processed.
    /// Internally a <see cref="KafkaStream"/> instance contains a normal producer and consumer instance
    /// that is used for reading input and writing output.
    /// <example>
    /// A simple example might look like this:
    /// <code>
    /// var config = new StreamConfig&lt;StringSerDes, StringSerDes&gt;();
    /// config.ApplicationId = "test-kstream-app";
    /// config.BootstrapServers = "192.168.56.1:9092";
    /// 
    /// StreamBuilder builder = new StreamBuilder();
    /// builder.Stream&lt;string, string&gt;("test").FilterNot((k, v) => v.Contains("test")).To("test-output");
    /// 
    /// KafkaStream stream = new KafkaStream(builder.Build(), config);
    /// 
    /// Console.CancelKeyPress += (o, e) => {
    ///     stream.Dispose();
    /// };
    ///      
    /// stream.Start(source.Token);
    /// </code>
    /// </example>
    /// </summary>
    public sealed class KafkaStream : IDisposable
    {
        #region State Stream

        /// <summary>
        /// Kafka Streams states are the possible state that a Kafka Streams instance can be in.
        /// An instance must only be in one state at a time.
        /// The expected state transition with the following defined states is:
        /// 
        ///                 +--------------+
        ///         +&lt;----- | Created (0)  |
        ///         |       +-----+--------+
        ///         |             |
        ///         |             v
        ///         |       +----+--+------+
        ///         |       | Re-          |
        ///         +&lt;----- | Balancing (1)| --------&gt;+
        ///         |       +-----+-+------+          |
        ///         |             | ^                 |
        ///         |             v |                 |
        ///         |       +--------------+          v
        ///         |       | Running (2)  | --------&gt;+
        ///         |       +------+-------+          |
        ///         |              |                  |
        ///         |              v                  |
        ///         |       +------+-------+     +----+-------+
        ///         +-----&gt; | Pending      |&lt;--- | Error (5)  |
        ///                 | Shutdown (3) |     +------------+
        ///                 +------+-------+
        ///                        |
        ///                        v
        ///                 +------+-------+
        ///                 | Not          |
        ///                 | Running (4)  |
        ///                 +--------------+
        /// 
        /// 
        /// Note the following:
        /// - RUNNING state will transit to REBALANCING if any of its threads is in PARTITION_REVOKED or PARTITIONS_ASSIGNED state
        /// - REBALANCING state will transit to RUNNING if all of its threads are in RUNNING state
        /// - Any state except NOT_RUNNING can go to PENDING_SHUTDOWN (whenever close is called)
        /// - Of special importance: If the global stream thread dies, or all stream threads die (or both) then
        ///   the instance will be in the ERROR state. The user will need to close it.
        /// </summary>
        public sealed class State : IEquatable<State>
        {
            /// <summary>
            /// Static CREATED State of a <see cref="KafkaStream"/> instance.
            /// </summary>
            public static readonly State CREATED = new State(1, 3).Order(0).Named("CREATED");
            /// <summary>
            /// Static REBALANCING State of a <see cref="KafkaStream"/> instance.
            /// </summary>
            public static readonly State REBALANCING = new State(2, 3, 5).Order(1).Named("REBALANCING");
            /// <summary>
            /// Static RUNNING State of a <see cref="KafkaStream"/> instance.
            /// </summary>
            public static readonly State RUNNING = new State(1, 2, 3, 5).Order(2).Named("RUNNING");
            /// <summary>
            /// Static PENDING_SHUTDOWN State of a <see cref="KafkaStream"/> instance.
            /// </summary>
            public static readonly State PENDING_SHUTDOWN = new State(4).Order(3).Named("PENDING_SHUTDOWN");
            /// <summary>
            /// Static NOT_RUNNING State of a <see cref="KafkaStream"/> instance.
            /// </summary>
            public static readonly State NOT_RUNNING = new State().Order(4).Named("NOT_RUNNING");
            /// <summary>
            /// Static ERROR State of a <see cref="KafkaStream"/> instance.
            /// </summary>
            public static readonly State ERROR = new State(3).Order(5).Named("ERROR");

            /// <summary>
            /// Name of the state
            /// </summary>
            public string Name { get; private set; }

            /// <summary>
            /// Order's state
            /// </summary>
            public int Ordinal { get; private set; }

            /// <summary>
            /// Valid transition of the current state
            /// </summary>
            public ISet<int> Transitions { get; } = new HashSet<int>();

            private State(params int[] validTransitions)
            {
                Transitions.AddRange(validTransitions);
            }

            private State Order(int ordinal)
            {
                Ordinal = ordinal;
                return this;
            }

            private State Named(string name)
            {
                Name = name;
                return this;
            }

            /// <summary>
            /// Return true if the state is a running state, false otherwise
            /// </summary>
            /// <returns>Return true if the state is a running state, false otherwise</returns>
            public bool IsRunning()

            {
                return Equals(RUNNING) || Equals(REBALANCING);
            }

            internal bool IsValidTransition(State newState)
            {
                return Transitions.Contains(newState.Ordinal);
            }

            /// <summary>
            /// == operator between two <see cref="State"/>
            /// </summary>
            /// <param name="a"></param>
            /// <param name="b"></param>
            /// <returns></returns>
            public static bool operator ==(State a, State b) => a?.Ordinal == b?.Ordinal;

            /// <summary>
            /// != operator between two <see cref="State"/>
            /// </summary>
            /// <param name="a"></param>
            /// <param name="b"></param>
            /// <returns></returns>
            public static bool operator !=(State a, State b) => a?.Ordinal != b?.Ordinal;

            /// <summary>
            /// Override Equals method
            /// </summary>
            /// <param name="obj"></param>
            /// <returns></returns>
            public override bool Equals(object obj)
            {
                return obj is State && Equals((State)obj);
            }

            /// <summary>
            /// Override GetHashCode method
            /// </summary>
            /// <returns></returns>
            public override int GetHashCode()
            {
                return Ordinal.GetHashCode();
            }

            /// <summary>
            /// Override ToString method
            /// </summary>
            /// <returns></returns>
            public override string ToString()
            {
                return $"{Name}";
            }

            /// <summary>
            /// 
            /// </summary>
            /// <param name="other"></param>
            /// <returns></returns>
            public bool Equals(State other) => other.Ordinal.Equals(Ordinal);
        }

        #endregion

        private readonly Topology topology;
        private readonly IKafkaSupplier kafkaSupplier;
        private readonly IThread[] threads;
        private readonly IThread externalStreamThread;
        private readonly IStreamConfig configuration;
        private readonly string clientId;
        private readonly ILogger logger;
        private readonly string logPrefix;
        private readonly object stateLock = new object();
        private readonly QueryableStoreProvider queryableStoreProvider;
        private readonly GlobalStreamThread globalStreamThread;
        private readonly StreamStateManager manager;
        private readonly StreamMetricsRegistry metricsRegistry;

        private readonly CancellationTokenSource _cancelSource = new();

        internal State StreamState { get; private set; }

        /// <summary>
        /// An app can subscribe to <see cref="StateListener"/> and he is notified when state changes.
        /// </summary>
        public event StateListener StateChanged;

        /// <summary>
        /// Create a <see cref="KafkaStream"/> instance.
        /// Please DO NOT FORGET to call Close to avoid resources leak !
        /// </summary>
        /// <param name="topology">the topology specifying the computational logic</param>
        /// <param name="configuration">configuration about this stream</param>
        public KafkaStream(Topology topology, IStreamConfig configuration)
            : this(topology, configuration, new DefaultKafkaClientSupplier(new KafkaLoggerAdapter(configuration), configuration))
        { }

        /// <summary>
        /// Create a <see cref="KafkaStream"/> instance with your own <see cref="IKafkaSupplier" />
        /// Please DO NOT FORGET to call Close to avoid resources leak !
        /// </summary>
        /// <param name="topology">the topology specifying the computational logic</param>
        /// <param name="configuration">configuration about this stream</param>
        /// <param name="kafkaSupplier">the Kafka clients supplier which provides underlying producer and consumer clients for the new <see cref="KafkaStream"/> instance</param>
        public KafkaStream(Topology topology, IStreamConfig configuration, IKafkaSupplier kafkaSupplier)
        {
            this.topology = topology;
            this.kafkaSupplier = kafkaSupplier;
            this.configuration = configuration;
            Logger.LoggerFactory = configuration.Logger;
            
            logger = Logger.GetLogger(typeof(KafkaStream));
            
            // check if ApplicationId & BootstrapServers has been set
            if (string.IsNullOrEmpty(configuration.ApplicationId) || string.IsNullOrEmpty(configuration.BootstrapServers))
            {
                throw new StreamConfigException($"Stream configuration is not correct. Please set ApplicationId and BootstrapServers as minimal.");
            }

            var processID = Guid.NewGuid();
            clientId = string.IsNullOrEmpty(configuration.ClientId) ? $"{configuration.ApplicationId.ToLower()}-{processID}" : configuration.ClientId;
            logPrefix = $"stream-application[{configuration.ApplicationId}] ";
            metricsRegistry = new StreamMetricsRegistry(clientId, configuration.MetricsRecording);
            this.kafkaSupplier.MetricsRegistry = metricsRegistry;
            
            logger.LogInformation($"{logPrefix} Start creation of the stream application with this configuration: {configuration}");

            // re-write the physical topology according to the config
            topology.Builder.RewriteTopology(configuration);
            
            // sanity check
            var processorTopology = topology.Builder.BuildTopology();

            int numStreamThreads = topology.Builder.HasNoNonGlobalTopology ? 0 : configuration.NumStreamThreads;

            string Protect(string str)
                => str.Replace("\n", "\\n");
            
            GeneralClientMetrics.StreamsAppSensor(
                configuration.ApplicationId,
                Protect(topology.Describe().ToString()),
                () => StreamState != null && StreamState.IsRunning() ? 1 : 0,
                () => threads.Count(t => t.State != ThreadState.DEAD && t.State != ThreadState.PENDING_SHUTDOWN),
                metricsRegistry);
            
            threads = new IThread[numStreamThreads];
            var threadState = new Dictionary<long, Processors.ThreadState>();

            ProcessorTopology globalTaskTopology = topology.Builder.BuildGlobalStateTopology();
            bool hasGlobalTopology = globalTaskTopology != null;
            if (numStreamThreads == 0 && !hasGlobalTopology)
            {
                throw new TopologyException("Topology has no stream threads and no global threads, " +
                    "must subscribe to at least one source topic or global table.");
            }

            GlobalThreadState globalThreadState = null;
            if (hasGlobalTopology)
            {
                string globalThreadId = $"{clientId}-GlobalStreamThread";
                GlobalStreamThreadFactory globalStreamThreadFactory = new GlobalStreamThreadFactory(
                    globalTaskTopology,
                    globalThreadId,
                    kafkaSupplier.GetGlobalConsumer(configuration.ToGlobalConsumerConfig(globalThreadId).Wrap(globalThreadId)),
                    configuration,
                    kafkaSupplier.GetAdmin(configuration.ToAdminConfig(clientId)),
                    metricsRegistry);
                globalStreamThread = globalStreamThreadFactory.GetGlobalStreamThread();
                globalThreadState = globalStreamThread.State;
            }

            if (this.topology.Builder.ExternalCall)
            {
                var threadId = $"{clientId}-external-stream-thread";
                var externalStreamConfig = configuration.Clone();
                externalStreamConfig.ApplicationId += "-external";
                externalStreamThread = new ExternalStreamThread(
                    threadId,
                    clientId,
                    this.kafkaSupplier,
                    this.topology.Builder,
                    metricsRegistry,
                    externalStreamConfig);
            }

            List<StreamThreadStateStoreProvider> stateStoreProviders = new List<StreamThreadStateStoreProvider>();
            for (int i = 0; i < numStreamThreads; ++i)
            {
                var threadId = $"{clientId}-stream-thread-{i}";

                var adminClient = this.kafkaSupplier.GetAdmin(configuration.ToAdminConfig(StreamThread.GetSharedAdminClientId(clientId)));

                threads[i] = StreamThread.Create(
                    threadId,
                    clientId,
                    this.topology.Builder,
                    metricsRegistry,
                    configuration,
                    this.kafkaSupplier,
                    adminClient,
                    i);

                threadState.Add(threads[i].Id, threads[i].State);

                stateStoreProviders.Add(new StreamThreadStateStoreProvider(threads[i], this.topology.Builder));
            }

            manager = new StreamStateManager(this, threadState, globalThreadState);
            if (hasGlobalTopology)
            {
                globalStreamThread.StateChanged += manager.OnGlobalThreadStateChange;
            }
            foreach (var t in threads)
            {
                t.StateChanged += manager.OnChange;
            }

            var globalStateStoreProvider = new GlobalStateStoreProvider(topology.Builder.GlobalStateStores);
            queryableStoreProvider = new QueryableStoreProvider(stateStoreProviders, globalStateStoreProvider);

            StreamState = State.CREATED;
        }

        /// <summary>
        /// Start the <see cref="KafkaStream"/> instance by starting all its threads.
        /// This function is expected to be called only once during the life cycle of the client.
        /// Because threads are started in the background, this method does not block.
        /// </summary>
        /// <param name="token">Token for propagates notification that the stream should be canceled.</param>
        [Obsolete("This method is deprecated, please use StartAsync(...) instead. It will be removed in next release version.")]
        public void Start(CancellationToken? token = null)
        {
            StartAsync(token).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Start asynchronously the <see cref="KafkaStream"/> instance by starting all its threads.
        /// This function is expected to be called only once during the life cycle of the client.
        /// Because threads are started in the background, this method does not block.
        /// </summary>
        /// <param name="token">Token for propagates notification that the stream should be canceled.</param>
        public async Task StartAsync(CancellationToken? token = null)
        {
            if (token.HasValue)
            {
                token.Value.Register(() => {
                    _cancelSource.Cancel();
                    Dispose();
                });
            }
            await Task.Factory.StartNew(async () =>
            {
                if (SetState(State.REBALANCING))
                {
                    logger.LogInformation("{LogPrefix}Starting Streams client with this topology : {Topology}", logPrefix, topology.Describe());
                    
                    try
                    {
                        Task.WaitAll(InitializeInternalTopicManagerAsync());
                    }
                    catch (AggregateException e)
                    {
                        foreach (var innerE in e.InnerExceptions)
                        {
                            logger.Log(LogLevel.Error, innerE, $"{logPrefix}Error during initializing internal topics");
                            SetState(State.PENDING_SHUTDOWN);
                            SetState(State.ERROR);
                        }
                        return;
                    }

                    RunMiddleware(true, true);
                    
                    globalStreamThread?.Start(_cancelSource.Token);
                    externalStreamThread?.Start(_cancelSource.Token);
                    
                    foreach (var t in threads)
                    {
                        t.Start(_cancelSource.Token);
                    }
                    
                    RunMiddleware(false, true);
                }
            }, token ?? _cancelSource.Token);


            try
            {
                // Allow time for streams thread to run
                await Task.Delay(TimeSpan.FromMilliseconds(configuration.StartTaskDelayMs),
                    token ?? _cancelSource.Token);
            }
            catch
            {
                 // nothing, in case or Application crash or stop before end of configuration.StartTaskDelayMs
            }
        }

        /// <summary>
        /// Shutdown this <see cref="KafkaStream"/> instance by signaling all the threads to stop, and then wait for them to join.
        /// This will block until all threads have stopped.
        /// </summary>
        public void Dispose()
        {
            Task.Factory.StartNew(() =>
            {
                if (!_cancelSource.IsCancellationRequested)
                {
                    _cancelSource.Cancel();
                }

                Close();
                _cancelSource.Dispose();
            }).Wait(TimeSpan.FromSeconds(30));
        }

        /// <summary>
        /// Shutdown this <see cref="KafkaStream"/> instance by signaling all the threads to stop, and then wait for them to join.
        /// This will block until all threads have stopped.
        /// </summary>
        private void Close()
        {
            if (!SetState(State.PENDING_SHUTDOWN))
            {
                // if transition failed, it means it was either in PENDING_SHUTDOWN
                // or NOT_RUNNING already; just check that all threads have been stopped
                logger.LogInformation($"{logPrefix}Already in the pending shutdown state, wait to complete shutdown");
            }
            else
            {
                RunMiddleware(true, false);
                
                foreach (var t in threads)
                {
                    t.Dispose();
                }

                externalStreamThread?.Dispose();
                globalStreamThread?.Dispose();

                RunMiddleware(false, false);
                metricsRegistry.RemoveClientSensors();
                SetState(State.NOT_RUNNING);
                logger.LogInformation($"{logPrefix}Streams client stopped completely");
            }
        }

        /// <summary>
        /// Get a facade wrapping the local <see cref="IStateStore"/> instances with the provided <see cref="StoreQueryParameters{T, K, V}"/>
        /// The returned object can be used to query the <see cref="IStateStore"/> instances.
        /// </summary>
        /// <typeparam name="T">return type</typeparam>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="V">Value type</typeparam>
        /// <param name="storeQueryParameters">the parameters used to fetch a queryable store</param>
        /// <returns>A facade wrapping the local <see cref="IStateStore"/> instances</returns>
        /// <exception cref="InvalidStateStoreException ">if Kafka Streams is (re-)initializing or a store with <code>storeName</code> } and
        /// <code>queryableStoreType</code> doesn't exist </exception>
        public T Store<T, K, V>(StoreQueryParameters<T, K, V> storeQueryParameters)
            where T : class
        {
            ValidateIsRunning();
            return queryableStoreProvider.GetStore(storeQueryParameters);
        }


        /// <summary>
        /// Get read-only handle on global sensors registry, including streams client's own sensors plus
        /// its embedded producer, consumer sensors (if <see cref="IStreamConfig.ExposeLibrdKafkaStats"/> is enable.
        /// </summary>
        /// <returns><see cref="IReadOnlyCollection{T}"/> of all sensors</returns>
        public IEnumerable<Sensor> Metrics()
            => metricsRegistry.GetSensors();

        #region Privates

        /// <summary>
        /// Set internal state. This method is thread safe.
        /// </summary>
        /// <param name="newState">New state</param>
        /// <returns>Return true if <paramref name="newState"/> was setted, false otherwise</returns>
        internal bool SetState(State newState)
        {
            State oldState;

            lock (stateLock)
            {
                oldState = StreamState;

                if (StreamState == State.PENDING_SHUTDOWN && newState != State.NOT_RUNNING)
                {
                    // when the state is already in PENDING_SHUTDOWN, all other transitions than NOT_RUNNING (due to thread dying) will be
                    // refused but we do not throw exception here, to allow appropriate error handling
                    return false;
                }
                else if (StreamState == State.NOT_RUNNING && (newState == State.PENDING_SHUTDOWN || newState == State.NOT_RUNNING))
                {
                    // when the state is already in NOT_RUNNING, its transition to PENDING_SHUTDOWN or NOT_RUNNING (due to consecutive close calls)
                    // will be refused but we do not throw exception here, to allow idempotent close calls
                    return false;
                }
                else if (StreamState == State.REBALANCING && newState == State.REBALANCING)
                {
                    // when the state is already in REBALANCING, it should not transit to REBALANCING again
                    return false;
                }
                else if (StreamState == State.ERROR && newState == State.ERROR)
                {
                    // when the state is already in ERROR, it should not transit to ERROR again
                    return false;
                }
                else if (!StreamState.IsValidTransition(newState))
                {
                    throw new IllegalStateException($"Stream-client {clientId}: Unexpected state transition from {oldState} to {newState}");
                }
                else
                {
                    logger.LogInformation($"{logPrefix}State transition from {oldState} to {newState}");
                }
                StreamState = newState;
            }

            StateChanged?.Invoke(oldState, newState);

            return true;
        }

        private void ValidateIsRunning()
        {
            bool isRunning;
            lock (stateLock)
            {
                isRunning = StreamState.IsRunning();
            }
            if (!isRunning)
            {
                throw new IllegalStateException($"KafkaStreams is not running. State is {StreamState}.");
            }
        }

        private async Task InitializeInternalTopicManagerAsync()
        {
            // Create internal topics (changelogs & repartition) if need
            var adminClientInternalTopicManager = kafkaSupplier.GetAdmin(configuration.ToAdminConfig(StreamThread.GetSharedAdminClientId($"{configuration.ApplicationId.ToLower()}-admin-internal-topic-manager")));
            using(var internalTopicManager = new DefaultTopicManager(configuration, adminClientInternalTopicManager))
                await InternalTopicManagerUtils.New().CreateInternalTopicsAsync(internalTopicManager, topology.Builder);
        }

        private void RunMiddleware(bool before, bool start)
        {
            if (configuration.Middlewares.Any())
            {
                int index = start ? (before ? 0 : 1) : (before ? 2 : 3);
                var methods = typeof(IStreamMiddleware).GetMethods();
                logger.LogInformation($"{logPrefix}Starting middleware {methods[index].Name.ToLowerInvariant()}");
                foreach (var middleware in configuration.Middlewares)
                    methods[index].Invoke(middleware, new object[] {configuration, _cancelSource.Token});
                logger.LogInformation($"{logPrefix}Middleware {methods[index].Name.ToLowerInvariant()} done");
            }
        }
        
        #endregion
    }
}