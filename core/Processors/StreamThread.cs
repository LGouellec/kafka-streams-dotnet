using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Processors.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.Processors
{
    internal class StreamThread : IThread, IDisposable
    {
        #region Static 

        public static string GetTaskProducerClientId(string threadClientId, TaskId taskId)
        {
            return threadClientId + "-" + taskId + "-producer";
        }

        public static string GetThreadProducerClientId(string threadClientId)
        {
            return threadClientId + "-producer";
        }

        public static string GetConsumerClientId(string threadClientId)
        {
            return threadClientId + "-consumer";
        }

        public static string GetRestoreConsumerClientId(string threadClientId)
        {
            return threadClientId + "-restore-consumer";
        }

        // currently admin client is shared among all threads
        public static string GetSharedAdminClientId(string clientId)
        {
            return clientId + "-admin";
        }

        internal static IThread Create(string threadId, string clientId, InternalTopologyBuilder builder, IStreamConfig configuration, IKafkaSupplier kafkaSupplier, IAdminClient adminClient, int threadInd)
        {
            string logPrefix = $"stream-thread[{threadId}] ";
            var log = Logger.GetLogger(typeof(StreamThread));
            var customerID = $"{clientId}-StreamThread-{threadInd}";
            IProducer<byte[], byte[]> producer = null;

            // Due to limitations outlined in KIP-447 (which KIP-447 overcomes), it is
            // currently necessary to use a separate producer per input partition. The
            // producerState dictionary is used to keep track of these, and the current
            // consumed offset.
            // https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics
            // IF Guarantee is AT_LEAST_ONCE, producer is the same of all StreamTasks in this thread, 
            // ELSE one producer by StreamTask.
            if (configuration.Guarantee == ProcessingGuarantee.AT_LEAST_ONCE)
            {
                log.LogInformation("{LogPrefix}Creating shared producer client", logPrefix);
                producer = kafkaSupplier.GetProducer(configuration.ToProducerConfig(GetThreadProducerClientId(threadId)));
            }

            var taskCreator = new TaskCreator(builder, configuration, threadId, kafkaSupplier, producer);
            var manager = new TaskManager(builder, taskCreator, adminClient);

            var listener = new StreamsRebalanceListener(manager);

            log.LogInformation("{LogPrefix}Creating consumer client", logPrefix);
            var consumer = kafkaSupplier.GetConsumer(configuration.ToConsumerConfig(GetConsumerClientId(customerID)), listener);
            manager.Consumer = consumer;

            var thread = new StreamThread(threadId, customerID, manager, consumer, builder, configuration);
            listener.Thread = thread;

            return thread;
        }

        #endregion

        public ThreadState State { get; private set; }

        private readonly IStreamConfig streamConfig;
        private readonly ILogger log = Logger.GetLogger(typeof(StreamThread));
        private readonly Thread thread;
        private readonly IConsumer<byte[], byte[]> consumer;
        private readonly TaskManager manager;
        private readonly InternalTopologyBuilder builder;
        private readonly TimeSpan consumeTimeout;
        private readonly string threadId;
        private readonly string clientId;
        private readonly string logPrefix = "";
        private readonly long commitTimeMs = 0;
        private CancellationToken token;
        private DateTime lastCommit = DateTime.Now;

        private int numIterations = 1;
        private long lastPollMs;

        private readonly object stateLock = new object();

        public event ThreadStateListener StateChanged;

        private StreamThread(string threadId, string clientId, TaskManager manager, IConsumer<byte[], byte[]> consumer, InternalTopologyBuilder builder, IStreamConfig configuration)
            : this(threadId, clientId, manager, consumer, builder, TimeSpan.FromMilliseconds(configuration.PollMs), configuration.CommitIntervalMs)
        {
            streamConfig = configuration;
        }

        private StreamThread(string threadId, string clientId, TaskManager manager, IConsumer<byte[], byte[]> consumer, InternalTopologyBuilder builder, TimeSpan timeSpan, long commitInterval)
        {
            this.manager = manager;
            this.consumer = consumer;
            this.builder = builder;
            consumeTimeout = timeSpan;
            this.threadId = threadId;
            this.clientId = clientId;
            logPrefix = $"stream-thread[{threadId}] ";
            commitTimeMs = commitInterval;

            thread = new Thread(Run);
            thread.Name = this.threadId;
            Name = this.threadId;

            State = ThreadState.CREATED;
        }

        #region IThread Impl

        public string Name { get; }

        public bool IsRunning { get; private set; } = false;

        public bool IsDisposable { get; private set; } = false;

        public int Id => thread.ManagedThreadId;

        public void Dispose() => Close(true);

        public void Run()
        {
            Exception exception = null;
            if (IsRunning)
            {
                while (!token.IsCancellationRequested)
                {
                    #region Treat Exception
                    /*
                     * exception variable is set in try { ... }catch {..} just after.
                     * TreatException(..) close consumer, taskManager, flush all state stores and return an enumeration (FAIL if thread must stop, CONTINUE if developer want to continue stream processing).
                     * IF response == ExceptionHandlerResponse.FAIL, break infinte thread loop.
                     * ELSE IF response == ExceptionHandlerResponse.CONTINUE, reset exception variable, flush all states, revoked partitions, and re-subscribe source topics to resume processing.
                     * exception behavior is implemented in the begin of infinite loop to be more readable, catch block just setted exception variable and log content to track it.
                    */
                    if (exception != null)
                    {
                        ExceptionHandlerResponse response = TreatException(exception);
                        if (response == ExceptionHandlerResponse.FAIL)
                            break;
                        else if (response == ExceptionHandlerResponse.CONTINUE)
                        {
                            exception = null;
                            HandleInnerException();
                        }
                    }
                    #endregion

                    try
                    {
                        if (!manager.RebalanceInProgress)
                        {
                            long now = DateTime.Now.GetMilliseconds();
                            var records = PollRequest(GetTimeout());

                            DateTime n = DateTime.Now;
                            var count = AddToTasks(records);
                            if (count > 0)
                                log.LogDebug("Add {Count} records in tasks in {DateTime}", count, DateTime.Now - n);

                            n = DateTime.Now;
                            int processed = 0;
                            long timeSinceLastPoll = 0;
                            do
                            {
                                processed = 0;
                                for (int i = 0; i < numIterations; ++i)
                                {
                                    processed = manager.Process(now);

                                    if (processed == 0)
                                        break;
                                    // NOT AVAILABLE NOW, NEED PROCESSOR API
                                    //if (processed > 0)
                                    //    manager.MaybeCommitPerUserRequested();
                                    //else
                                    //    break;
                                }

                                timeSinceLastPoll = Math.Max(DateTime.Now.GetMilliseconds() - lastPollMs, 0);

                                if (MaybeCommit())
                                {
                                    numIterations = numIterations > 1 ? numIterations / 2 : numIterations;
                                }
                                else if (timeSinceLastPoll > streamConfig.MaxPollIntervalMs.Value / 2)
                                {
                                    numIterations = numIterations > 1 ? numIterations / 2 : numIterations;
                                    break;
                                }
                                else if (processed > 0)
                                {
                                    numIterations++;
                                }

                            } while (processed > 0);

                            if (State == ThreadState.RUNNING)
                                MaybeCommit();

                            if (State == ThreadState.PARTITIONS_ASSIGNED)
                                SetState(ThreadState.RUNNING);

                            if (records.Any())
                                log.LogDebug("Processing {Count} records in {DateTime}", count, DateTime.Now - n);
                        }
                    }
                    catch (TaskMigratedException e)
                    {
                        HandleTaskMigrated(e);
                    }
                    catch (KafkaException e)
                    {
                        exception = e;
                        log.LogError(e,
                            
                                "{LogPrefix}Encountered the following unexpected Kafka exception during processing, this usually indicate Streams internal errors:",
                                logPrefix);
                    }
                    catch (Exception e)
                    {
                        exception = e;
                        log.LogError(exception, "{LogPrefix}Encountered the following error during processing:", logPrefix);
                    }
                }

                while (IsRunning)
                {
                    // Use for waiting end of disposing
                    Thread.Sleep(100);
                }

                // Dispose consumer
                try
                {
                    consumer.Dispose();
                }
                catch (Exception e)
                {
                    log.LogError(e, "{LogPrefix}Failed to close consumer due to the following error:", logPrefix);
                }
            }
        }

        private TimeSpan GetTimeout()
        {
            if (State == ThreadState.PARTITIONS_ASSIGNED || State == ThreadState.PARTITIONS_REVOKED)
                return TimeSpan.Zero;
            if (State == ThreadState.RUNNING || State == ThreadState.STARTING)
                return consumeTimeout;

            log.LogError("{LogPrefix}Unexpected state {State} during normal iteration", logPrefix, State);
            throw new StreamsException($"Unexpected state {State} during normal iteration");
        }

        private int AddToTasks(IEnumerable<ConsumeResult<byte[], byte[]>> records)
        {
            int count = 0;
            foreach (var record in records)
            {
                count++;
                var task = manager.ActiveTaskFor(record.TopicPartition);
                if(task != null)
                {
                    if (task.IsClosed)
                    {
                        log.LogInformation(
                            
                                "Stream task {TaskId} is already closed, probably because it got unexpectedly migrated to another thread already. Notifying the thread to trigger a new rebalance immediately",
                                task.Id);
                        // TODO gesture this behaviour
                        //throw new TaskMigratedException(task);
                    }
                    else
                        task.AddRecord(record);
                }
                else if (consumer.Assignment.Contains(record.TopicPartition))
                {
                    log.LogError(
                        "Unable to locate active task for received-record partition {TopicPartition}. Current tasks: {TaskIDs}. Current Consumer Assignment : {Assignment}",
                        record.TopicPartition, string.Join(",", manager.ActiveTaskIds),
                        string.Join(",", consumer.Assignment.Select(t => $"{t.Topic}-[{t.Partition}]")));
                    throw new NullReferenceException($"Task was unexpectedly missing for partition {record.TopicPartition}");
                }                
            }
            return count;
        }

        private ExceptionHandlerResponse TreatException(Exception exception)
        {
            if (exception is DeserializationException || exception is ProductionException)
            {
                Close(false);
                return ExceptionHandlerResponse.FAIL;
            }
            var response = streamConfig.InnerExceptionHandler(exception);
            if (response == ExceptionHandlerResponse.FAIL)
            {
                Close(false);
            }
            return response;
        }

        public void Start(CancellationToken token)
        {
            log.LogInformation("{LogPrefix}Starting", logPrefix);
            if (SetState(ThreadState.STARTING) == null)
            {
                log.LogInformation("{LogPrefix}StreamThread already shutdown. Not running", logPrefix);
                IsRunning = false;
                return;
            }

            this.token = token;
            IsRunning = true;
            consumer.Subscribe(builder.GetSourceTopics());
            thread.Start();
        }

        public IEnumerable<ITask> ActiveTasks => manager.ActiveTasks;

        #endregion

        private void HandleTaskMigrated(TaskMigratedException e)
        {
            log.LogWarning(e,
                
                    "{LogPrefix}Detected that the thread is being fenced. This implies that this thread missed a rebalance and dropped out of the consumer group. Will close out all assigned tasks and rejoin the consumer group",
                    logPrefix);

            manager.HandleLostAll();
            consumer.Unsubscribe();
            consumer.Subscribe(builder.GetSourceTopics());
        }

        private void HandleInnerException()
        {
            log.LogWarning(
                
                    "{LogPrefix}Detected that the thread throw an inner exception. Your configuration manager has decided to continue running stream processing. So will close out all assigned tasks and rejoin the consumer group",
                    logPrefix);

            manager.HandleLostAll();
            consumer.Unsubscribe();
            consumer.Subscribe(builder.GetSourceTopics());
        }

        private bool MaybeCommit()
        {
            int committed = 0;
            if (DateTime.Now - lastCommit > TimeSpan.FromMilliseconds(commitTimeMs))
            {
                DateTime beginCommit = DateTime.Now;
                log.LogDebug(
                    "Committing all active tasks {TaskIDs} since {DateTime}ms has elapsed (commit interval is {CommitTime}ms)",
                    string.Join(",", manager.ActiveTaskIds), (DateTime.Now - lastCommit).TotalMilliseconds,
                    commitTimeMs);
                committed = manager.CommitAll();
                if (committed > 0)
                    log.LogDebug("Committed all active tasks {TaskIDs} in {TimeElapsed}ms",
                        string.Join(",", manager.ActiveTaskIds), (DateTime.Now - beginCommit).TotalMilliseconds);

                if (committed == -1)
                {
                    log.LogDebug("Unable to commit as we are in the middle of a rebalance, will try again when it completes");
                }
                else
                {
                    lastCommit = DateTime.Now;
                }
            }
            return committed > 0;
        }

        private void Close(bool cleanUp = true)
        {
            try
            {
                if (!IsDisposable)
                {
                    log.LogInformation("{LogPrefix}Shutting down", logPrefix);

                    SetState(ThreadState.PENDING_SHUTDOWN);

                    manager.Close();
                    consumer.Unsubscribe();
                    IsRunning = false;

                    if (cleanUp)
                        thread.Join();
                    else
                        consumer.Dispose();

                    SetState(ThreadState.DEAD);
                    log.LogInformation("{LogPrefix}Shutdown complete", logPrefix);
                    IsDisposable = true;
                }
            }
            catch (Exception e)
            {
                log.LogError(e,
                    "{LogPrefix}Failed to close stream thread due to the following error:", logPrefix);
            }
        }

        private IEnumerable<ConsumeResult<byte[], byte[]>> PollRequest(TimeSpan ts)
        {
            lastPollMs = DateTime.Now.GetMilliseconds();
            return consumer.ConsumeRecords(ts, streamConfig.MaxPollRecords);
        }

        internal ThreadState SetState(ThreadState newState)
        {
            ThreadState oldState;

            lock (stateLock)
            {
                oldState = State;

                if (State == ThreadState.PENDING_SHUTDOWN && newState != ThreadState.DEAD)
                {
                    log.LogDebug(
                        "{LogPrefix}Ignoring request to transit from PENDING_SHUTDOWN to {NewState}: only DEAD state is a valid next state",
                        logPrefix, newState);
                    // when the state is already in PENDING_SHUTDOWN, all other transitions will be
                    // refused but we do not throw exception here
                    return null;
                }
                else if (State == ThreadState.DEAD)
                {
                    log.LogDebug(
                        "{LogPrefix}Ignoring request to transit from DEAD to {NewState}: no valid next state after DEAD", logPrefix,
                        newState);
                    // when the state is already in NOT_RUNNING, all its transitions
                    // will be refused but we do not throw exception here
                    return null;
                }
                else if (!State.IsValidTransition(newState))
                {
                    string logPrefix = "";
                    log.LogError("{LogPrefix}Unexpected state transition from {OldState} to {NewState}", logPrefix, oldState,
                        newState);
                    throw new StreamsException($"{logPrefix}Unexpected state transition from {oldState} to {newState}");
                }
                else
                {
                    log.LogInformation("{LogPrefix}State transition from {OldState} to {NewState}", logPrefix, oldState,
                        newState);
                }

                State = newState;
            }

            StateChanged?.Invoke(this, oldState, State);

            return oldState;
        }

        // FOR TEST
        internal IEnumerable<TopicPartitionOffset> GetCommittedOffsets(IEnumerable<TopicPartition> partitions, TimeSpan timeout)
            => consumer.Committed(partitions, timeout);
    }
}