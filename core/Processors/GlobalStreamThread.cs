using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.Processors
{
    internal class GlobalStreamThread : IDisposable
    {
        private class StateConsumer
        {
            private readonly IConsumer<byte[], byte[]> globalConsumer;
            private readonly ILogger log = Logger.GetLogger(typeof(StateConsumer));
            private readonly IGlobalStateMaintainer globalStateMaintainer;
            private readonly TimeSpan pollTime;
            private readonly TimeSpan flushInterval;
            private readonly long maxPollRecords;
            private DateTime lastFlush;

            public StateConsumer(
                IConsumer<byte[], byte[]> globalConsumer,
                IGlobalStateMaintainer globalStateMaintainer,
                TimeSpan pollTime,
                TimeSpan flushInterval, 
                long maxPollRecords)
            {
                this.globalConsumer = globalConsumer;
                this.globalStateMaintainer = globalStateMaintainer;
                this.pollTime = pollTime;
                this.flushInterval = flushInterval;
                this.maxPollRecords = maxPollRecords;
            }

            public void Initialize()
            {
                IDictionary<TopicPartition, long> partitionOffsets = globalStateMaintainer.Initialize();
                globalConsumer.Assign(partitionOffsets.Keys.Select(x => new TopicPartitionOffset(x, Offset.Beginning)));

                lastFlush = DateTime.Now;
            }

            public void PollAndUpdate()
            {
                try
                {
                    var received = globalConsumer.ConsumeRecords(pollTime, maxPollRecords);
                    foreach (var record in received)
                    {
                        globalStateMaintainer.Update(record);
                    }

                    DateTime dt = DateTime.Now;
                    if (dt >= lastFlush.Add(flushInterval))
                    {
                        globalStateMaintainer.FlushState();
                        lastFlush = DateTime.Now;
                    }
                }
                catch (Exception e)
                {
                    log.LogError(e, "Updating global state failed");
                    throw new StreamsException("Updating global state failed.", e);
                }
            }

            public void Close()
            {
                try
                {
                    globalConsumer.Close();
                }
                catch (Exception e)
                {
                    // just log an error if the consumer throws an exception during close
                    // so we can always attempt to close the state stores.
                    log.LogError(e, "Failed to close global consumer due to the following error:");
                }

                globalStateMaintainer.Close();
            }
        }

        public GlobalThreadState State { get; private set; }

        public event GlobalThreadStateListener StateChanged;

        private readonly ILogger log = Logger.GetLogger(typeof(GlobalStreamThread));
        private readonly Thread thread;
        private readonly string logPrefix;
        private readonly IConsumer<byte[], byte[]> globalConsumer;
        private CancellationToken token;
        private readonly object stateLock = new object();
        private readonly IStreamConfig configuration;
        private StateConsumer stateConsumer;
        private readonly IGlobalStateMaintainer globalStateMaintainer;

        public GlobalStreamThread(string threadClientId,
            IConsumer<byte[], byte[]> globalConsumer,
            IStreamConfig configuration,
            IGlobalStateMaintainer globalStateMaintainer)
        {
            logPrefix = $"global-stream-thread {threadClientId} ";

            this.globalConsumer = globalConsumer;
            this.configuration = configuration;
            this.globalStateMaintainer = globalStateMaintainer;

            thread = new Thread(Run);
            State = GlobalThreadState.CREATED;
        }

        private void Run()
        {
            SetState(GlobalThreadState.RUNNING);
            try
            {
                while (!token.IsCancellationRequested && State.IsRunning())
                {
                    stateConsumer.PollAndUpdate();
                }
            }
            finally
            {
                Dispose(true, false);
            }
        }

        public void Start(CancellationToken token)
        {
            log.LogInformation("{LogPrefix}Starting", logPrefix);

            try
            {
                stateConsumer = InitializeStateConsumer();
            }
            catch
            {
                SetState(GlobalThreadState.PENDING_SHUTDOWN);
                SetState(GlobalThreadState.DEAD);

                log.LogWarning(
                    "{LogPrefix}Error happened during initialization of the global state store; this thread has shutdown",
                    logPrefix);

                throw;
            }

            this.token = token;

            thread.Start();
        }

        private StateConsumer InitializeStateConsumer()
        {
            try
            {
                var stateConsumer = new StateConsumer(
                    globalConsumer,
                    globalStateMaintainer,
                    // if poll time is bigger than int allows something is probably wrong anyway
                    new TimeSpan(0, 0, 0, 0, (int)configuration.PollMs),
                    new TimeSpan(0, 0, 0, 0, (int)configuration.CommitIntervalMs),
                    configuration.MaxPollRecords);
                stateConsumer.Initialize();
                return stateConsumer;
            }
            catch (StreamsException)
            {
                throw;
            }
            catch (Exception e)
            {
                throw new StreamsException("Exception caught during initialization of GlobalStreamThread", e);
            }
        }

        private void SetState(GlobalThreadState newState)
        {
            GlobalThreadState oldState;

            lock (stateLock)
            {
                oldState = State;

                if (oldState == GlobalThreadState.PENDING_SHUTDOWN && newState == GlobalThreadState.PENDING_SHUTDOWN)
                {
                    // when the state is already in PENDING_SHUTDOWN, its transition to itself
                    // will be refused but we do not throw exception here
                    return;
                }
                else if (oldState == GlobalThreadState.DEAD)
                {
                    // when the state is already in NOT_RUNNING, all its transitions
                    // will be refused but we do not throw exception here
                    return;
                }
                else if (!oldState.IsValidTransition(newState))
                {
                    log.LogError("{LogPrefix}Unexpected state transition from {OldState} to {NewState}", logPrefix, oldState,
                        newState);
                    throw new StreamsException($"Unexpected state transition from {oldState} to {newState}");
                }
                else
                {
                    log.LogInformation("{LogPrefix}State transition from {OldState} to {NewState}", logPrefix, oldState,
                        newState);
                }

                State = newState;
            }

            StateChanged?.Invoke(this, oldState, State);
        }

        #region IDisposable

        private bool disposed = false;

        public void Dispose()
        {
            Dispose(true, true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing, bool waitForThread)
        {
            if (!disposed)
            {
                // we don't have any unmanaged resources to dispose of so we can ignore value of `disposing`

                SetState(GlobalThreadState.PENDING_SHUTDOWN);
                log.LogInformation("{LogPrefix}Shutting down", logPrefix);

                if (waitForThread)
                {
                    thread.Join();
                }

                try
                {
                    stateConsumer.Close();
                }
                catch (Exception e)
                {
                    log.LogError(
                        e, "{LogPrefix}exception caught during disposing of GlobalStreamThread", logPrefix);
                    // ignore exception
                    // https://docs.microsoft.com/en-us/visualstudio/code-quality/ca1065
                }

                SetState(GlobalThreadState.DEAD);
                log.LogInformation("{LogPrefix}Shutdown complete", logPrefix);

                disposed = true;
            }
        }

        #endregion
    }
}
