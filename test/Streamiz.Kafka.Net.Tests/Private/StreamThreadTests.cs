using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock.Kafka;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class StreamThreadTests
    {
        #region State Test

        [Test]
        public void TestCreatedState()
        {
            var state = ThreadState.CREATED;
            Assert.AreEqual(0, state.Ordinal);
            Assert.AreEqual("CREATED", state.Name);
            Assert.AreEqual(new HashSet<int> { 1, 5 }, state.Transitions);
            Assert.IsFalse(state.IsRunning());
            Assert.IsFalse(state.IsValidTransition(ThreadState.DEAD));
            Assert.IsTrue(state.IsValidTransition(ThreadState.STARTING));
        }

        [Test]
        public void TestStartingState()
        {
            var state = ThreadState.STARTING;
            Assert.AreEqual(1, state.Ordinal);
            Assert.AreEqual("STARTING", state.Name);
            Assert.AreEqual(new HashSet<int> { 2, 3, 5 }, state.Transitions);
            Assert.IsTrue(state.IsRunning());
            Assert.IsFalse(state.IsValidTransition(ThreadState.CREATED));
            Assert.IsTrue(state.IsValidTransition(ThreadState.PARTITIONS_ASSIGNED));
        }

        [Test]
        public void TestPartitionsRevokedState()
        {
            var state = ThreadState.PARTITIONS_REVOKED;
            Assert.AreEqual(2, state.Ordinal);
            Assert.AreEqual("PARTITIONS_REVOKED", state.Name);
            Assert.AreEqual(new HashSet<int> { 2,3,5}, state.Transitions);
            Assert.IsTrue(state.IsRunning());
            Assert.IsFalse(state.IsValidTransition(ThreadState.DEAD));
            Assert.IsTrue(state.IsValidTransition(ThreadState.PARTITIONS_ASSIGNED));
        }        
        
        [Test]
        public void TestPartitionsAssignedState()
        {
            var state = ThreadState.PARTITIONS_ASSIGNED;
            Assert.AreEqual(3, state.Ordinal);
            Assert.AreEqual("PARTITIONS_ASSIGNED", state.Name);
            Assert.AreEqual(new HashSet<int> { 2,3,4,5}, state.Transitions);
            Assert.IsTrue(state.IsRunning());
            Assert.IsFalse(state.IsValidTransition(ThreadState.DEAD));
            Assert.IsTrue(state.IsValidTransition(ThreadState.RUNNING));
        }

        [Test]
        public void TestRunningState()
        {
            var state = ThreadState.RUNNING;
            Assert.AreEqual(4, state.Ordinal);
            Assert.AreEqual("RUNNING", state.Name);
            Assert.AreEqual(new HashSet<int> { 2, 3, 5 }, state.Transitions);
            Assert.IsTrue(state.IsRunning());
            Assert.IsFalse(state.IsValidTransition(ThreadState.CREATED));
            Assert.IsTrue(state.IsValidTransition(ThreadState.PARTITIONS_ASSIGNED));
        }

        [Test]
        public void TestPendingShutdownState()
        {
            var state = ThreadState.PENDING_SHUTDOWN;
            Assert.AreEqual(5, state.Ordinal);
            Assert.AreEqual("PENDING_SHUTDOWN", state.Name);
            Assert.AreEqual(new HashSet<int> { 6 }, state.Transitions);
            Assert.IsFalse(state.IsRunning());
            Assert.IsFalse(state.IsValidTransition(ThreadState.CREATED));
            Assert.IsTrue(state.IsValidTransition(ThreadState.DEAD));
        }

        [Test]
        public void TestDeadState()
        {
            var state = ThreadState.DEAD;
            Assert.AreEqual(6, state.Ordinal);
            Assert.AreEqual("DEAD", state.Name);
            Assert.AreEqual(new HashSet<int> { }, state.Transitions);
            Assert.IsFalse(state.IsRunning());
            Assert.IsFalse(state.IsValidTransition(ThreadState.CREATED));
        }

        #endregion

        #region StreamThread Id

        [Test]
        public void GetConsumerClientIdTest()
        {
            var result = StreamThread.GetConsumerClientId("thread-client");
            Assert.AreEqual($"thread-client-consumer", result);
        }

        [Test]
        public void GetRestoreConsumerClientIdTest()
        {
            var result = StreamThread.GetRestoreConsumerClientId("thread-client");
            Assert.AreEqual($"thread-client-restore-consumer", result);
        }

        [Test]
        public void GetSharedAdminClientIdTest()
        {
            var result = StreamThread.GetSharedAdminClientId("thread-client");
            Assert.AreEqual($"thread-client-admin", result);
        }

        [Test]
        public void GetTaskProducerClientIdTest()
        {
            var taskId = new TaskId { Id = 0, Partition = 0 };
            var result = StreamThread.GetTaskProducerClientId("thread-client", taskId);
            Assert.AreEqual($"thread-client-0-0-producer", result);
        }

        [Test]
        public void GetThreadProducerClientIdTest()
        {
            var result = StreamThread.GetThreadProducerClientId("thread-client");
            Assert.AreEqual($"thread-client-producer", result);
        }

        #endregion

        #region StreamThread Workflow

        [Test]
        public void CreateStreamThread()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";

            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic").To("topic2");

            var topo = builder.Build();
            
            var supplier = new SyncKafkaSupplier();
            var thread = StreamThread.Create(
                "thread-0", "c0",
                topo.Builder, config,
                supplier, supplier.GetAdmin(config.ToAdminConfig("admin")),
                0) as StreamThread;

            Assert.AreEqual("thread-0", thread.Name);
        }

        [Test]
        public void StreamThreadNormalWorkflow()
        {
            List<ThreadState> allStates = new List<ThreadState>();
            var expectedStates = new List<ThreadState>
            {
                ThreadState.CREATED,
                ThreadState.STARTING,
                ThreadState.PARTITIONS_ASSIGNED,
                ThreadState.RUNNING,
                ThreadState.PENDING_SHUTDOWN,
                ThreadState.DEAD
            };

            var source = new System.Threading.CancellationTokenSource();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 1;

            var serdes = new StringSerDes();
            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic").To("topic2");

            var topo = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(config.ToConsumerConfig("test-consum"), null);
            consumer.Subscribe("topic2");
            var thread = StreamThread.Create(
                "thread-0", "c0",
                topo.Builder, config,
                supplier, supplier.GetAdmin(config.ToAdminConfig("admin")),
                0) as StreamThread;
            allStates.Add(thread.State);
            thread.StateChanged += (t, o, n) =>
            {
                Assert.IsInstanceOf<ThreadState>(n);
                allStates.Add(n as ThreadState);
            };

            thread.Start(source.Token);
            producer.Produce("topic", new Confluent.Kafka.Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key1", new SerializationContext()),
                Value = serdes.Serialize("coucou", new SerializationContext())
            });
            //WAIT STREAMTHREAD PROCESS MESSAGE
            System.Threading.Thread.Sleep(100);
            var message = consumer.Consume(100);

            source.Cancel();
            thread.Dispose();

            Assert.AreEqual("key1", serdes.Deserialize(message.Message.Key, new SerializationContext()));
            Assert.AreEqual("coucou", serdes.Deserialize(message.Message.Value, new SerializationContext()));
            Assert.AreEqual(expectedStates, allStates);
        }

        [Test]
        public void StreamThreadNormalWorkflowWithRebalancing()
        {
            List<ThreadState> allStates = new List<ThreadState>();
            var expectedStates = new List<ThreadState>
            {
                ThreadState.CREATED,
                ThreadState.STARTING,
                ThreadState.PARTITIONS_ASSIGNED,
                ThreadState.RUNNING,
                ThreadState.PARTITIONS_REVOKED,
                ThreadState.PARTITIONS_ASSIGNED,
                ThreadState.RUNNING,
                ThreadState.PENDING_SHUTDOWN,
                ThreadState.DEAD
            };

            var source = new System.Threading.CancellationTokenSource();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 1;

            var consumeConfig = config.Clone();
            consumeConfig.ApplicationId = "consume-test";

            var serdes = new StringSerDes();
            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic").To("topic2");

            var topo = builder.Build();

            var supplier = new MockKafkaSupplier(4);
            var producer = supplier.GetProducer(consumeConfig.ToProducerConfig());
            var consumer = supplier.GetConsumer(consumeConfig.ToConsumerConfig("test-consum"), null);
            consumer.Subscribe("topic2");

            var thread = StreamThread.Create(
                "thread-0", "c0",
                topo.Builder, config,
                supplier, supplier.GetAdmin(config.ToAdminConfig("admin")),
                0) as StreamThread;
            allStates.Add(thread.State);
            thread.StateChanged += (t, o, n) =>
            {
                Assert.IsInstanceOf<ThreadState>(n);
                allStates.Add(n as ThreadState);
            };

            thread.Start(source.Token);
            // WAIT PARTITONS ASSIGNED
            System.Threading.Thread.Sleep(50);

            var thread2 = StreamThread.Create(
                "thread-1", "c1",
                topo.Builder, config,
                supplier, supplier.GetAdmin(config.ToAdminConfig("admin")),
                1) as StreamThread;
            thread2.Start(source.Token);
            // WAIT PARTITONS REBALANCING
            System.Threading.Thread.Sleep(50);

            producer.Produce("topic", new Confluent.Kafka.Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key1", new SerializationContext()),
                Value = serdes.Serialize("coucou", new SerializationContext())
            });
            //WAIT STREAMTHREAD PROCESS MESSAGE
            System.Threading.Thread.Sleep(100);
            var message = consumer.Consume(100);

            // 2 CONSUMER FOR THE SAME GROUP ID => TOPIC WITH 4 PARTITIONS
            Assert.AreEqual(2, thread.ActiveTasks.Count());
            Assert.AreEqual(2, thread2.ActiveTasks.Count());

            source.Cancel();
            thread.Dispose();
            thread2.Dispose();

            Assert.AreEqual("key1", serdes.Deserialize(message.Message.Key, new SerializationContext()));
            Assert.AreEqual("coucou", serdes.Deserialize(message.Message.Value, new SerializationContext()));
            Assert.AreEqual(expectedStates, allStates);
            // Destroy in memory cluster
            supplier.Destroy();
        }

        [Test]
        public void StreamThreadCommitIntervalWorkflow()
        {
            var source = new System.Threading.CancellationTokenSource();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 1;
            config.CommitIntervalMs = 1;

            var serdes = new StringSerDes();
            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic").To("topic2");

            var topo = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(config.ToConsumerConfig("test-consum"), null);
            consumer.Subscribe("topic2");
            var thread = StreamThread.Create(
                "thread-0", "c0",
                topo.Builder, config,
                supplier, supplier.GetAdmin(config.ToAdminConfig("admin")),
                0) as StreamThread;

            thread.Start(source.Token);
            producer.Produce("topic", new Confluent.Kafka.Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key1", new SerializationContext()),
                Value = serdes.Serialize("coucou", new SerializationContext())
            });
            //WAIT STREAMTHREAD PROCESS MESSAGE
            System.Threading.Thread.Sleep(100);
            var message = consumer.Consume(100);

            Assert.AreEqual("key1", serdes.Deserialize(message.Message.Key, new SerializationContext()));
            Assert.AreEqual("coucou", serdes.Deserialize(message.Message.Value, new SerializationContext()));

            var offsets = thread.GetCommittedOffsets(new List<TopicPartition> { new TopicPartition("topic", 0) }, TimeSpan.FromSeconds(10)).ToList();
            Assert.AreEqual(1, offsets.Count);
            Assert.AreEqual(1, offsets[0].Offset.Value);
            Assert.AreEqual(0, offsets[0].TopicPartition.Partition.Value);
            Assert.AreEqual("topic", offsets[0].Topic);

            source.Cancel();
            thread.Dispose();
        }

        #endregion

        #region StreamThread SetState

        [Test]
        public void CheckIncorrectStateTransition()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";

            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic").To("topic2");

            var topo = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var thread = StreamThread.Create(
                "thread-0", "c0",
                topo.Builder, config,
                supplier, supplier.GetAdmin(config.ToAdminConfig("admin")),
                0) as StreamThread;

            // MUST BE IN CREATED STATE
            Assert.AreEqual(ThreadState.CREATED, thread.State);
            Assert.Throws<StreamsException>(() => thread.SetState(ThreadState.DEAD));
        }

        [Test]
        public void CheckSetStateWithoutStateChangedHandler()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";

            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic").To("topic2");

            var topo = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var thread = StreamThread.Create(
                "thread-0", "c0",
                topo.Builder, config,
                supplier, supplier.GetAdmin(config.ToAdminConfig("admin")),
                0) as StreamThread;

            // MUST BE IN CREATED STATE
            Assert.AreEqual(ThreadState.CREATED, thread.State);
            thread.SetState(ThreadState.STARTING);
            Assert.AreEqual(ThreadState.STARTING, thread.State);
        }


        [Test]
        public void CheckSetStateStartingWithDeadThread()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";

            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic").To("topic2");

            var topo = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var thread = StreamThread.Create(
                "thread-0", "c0",
                topo.Builder, config,
                supplier, supplier.GetAdmin(config.ToAdminConfig("admin")),
                0) as StreamThread;

            // MUST BE IN CREATED STATE
            Assert.AreEqual(ThreadState.CREATED, thread.State);
            thread.SetState(ThreadState.STARTING);
            thread.SetState(ThreadState.PENDING_SHUTDOWN);
            thread.SetState(ThreadState.DEAD);
            thread.Start(default);
            Assert.IsFalse(thread.IsRunning);
        }

        #endregion

        #region StreamThread Commit Requested TODO

        #endregion
    }
}
