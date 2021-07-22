using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors.Internal;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.Processors
{
    internal class StreamStateManager
    {
        private readonly ILogger log = Logger.GetLogger(typeof(StreamStateManager));
        private readonly Dictionary<long, ThreadState> threadState;
        private readonly KafkaStream stream;
        private GlobalThreadState globalThreadState;
        private static readonly object threadStatesLock = new object();

        public StreamStateManager(KafkaStream stream, Dictionary<long, ThreadState> threadState, GlobalThreadState globalThreadState)
        {
            this.globalThreadState = globalThreadState;
            this.threadState = threadState;
            this.stream = stream;
        }

        private void MaybeSetError()
        {
            // check if we have at least one thread running
            foreach (var state in threadState.Values)
            {
                if (state != ThreadState.DEAD)
                {
                    return;
                }
            }

            if (stream.SetState(KafkaStream.State.ERROR))
            {
                log.LogError("All stream threads have died. The instance will be in error state and should be closed");
            }
        }

        /**
         * If all threads are up, including the global thread, set to RUNNING
         */
        private void MaybeSetRunning()
        {
            // state can be transferred to RUNNING if all threads are either RUNNING or DEAD
            foreach ( var state in threadState.Values)
            {
                if (state != ThreadState.RUNNING && state != ThreadState.DEAD)
                {
                    return;
                }
            }

            // the global state thread is relevant only if it is started. There are cases
            // when we don't have a global state thread at all, e.g., when we don't have global KTables
            if (globalThreadState != null && globalThreadState != GlobalThreadState.RUNNING)
            {
                return;
            }

            stream.SetState(KafkaStream.State.RUNNING);
        }


        internal void OnChange(IThread thread, ThreadStateTransitionValidator old, ThreadStateTransitionValidator @new)
        {
            lock (threadStatesLock)
            {
                if (thread is StreamThread)
                {
                    ThreadState newState = (ThreadState)@new;
                    threadState.AddOrUpdate(thread.Id, newState);

                    if (newState == ThreadState.PARTITIONS_REVOKED || newState == ThreadState.PARTITIONS_ASSIGNED)
                    {
                        stream.SetState(KafkaStream.State.REBALANCING);
                    }
                    else if (newState == ThreadState.RUNNING)
                    {
                        MaybeSetRunning();
                    }
                    else if (newState == ThreadState.DEAD)
                    {
                        MaybeSetError();
                    }
                }
            }
        }

        internal void OnGlobalThreadStateChange(GlobalStreamThread thread, ThreadStateTransitionValidator old, ThreadStateTransitionValidator @new)
        {
            lock (threadStatesLock)
            {
                if (thread is GlobalStreamThread)
                {
                    GlobalThreadState newState = (GlobalThreadState)@new;
                    globalThreadState = newState;

                    if (newState == GlobalThreadState.RUNNING)
                    {
                        MaybeSetRunning();
                    }
                    else if (newState == GlobalThreadState.DEAD)
                    {
                        if (stream.SetState(KafkaStream.State.ERROR))
                        {
                            log.LogError("Global thread has died. The instance will be in error state and should be closed");
                        }
                    }
                }
            }
        }
    }
}