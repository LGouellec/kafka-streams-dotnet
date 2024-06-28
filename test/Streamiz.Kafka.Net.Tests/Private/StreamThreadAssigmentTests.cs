using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock.Kafka;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Supplier;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Tests.Helpers;
using ThreadState = Streamiz.Kafka.Net.Processors.ThreadState;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class StreamThreadAssigmentTests
    {
        private class StoreThrowExceptionStore : IKeyValueStore<Bytes, byte[]>
        {
            public string Name { get; set; }
            public bool Persistent { get; }
            public bool IsLocally => true;
            public bool IsOpen { get; }
            public void Init(ProcessorContext context, IStateStore root)
            {
                throw new NotImplementedException();
            }

            public void Flush()
            {
                throw new NotImplementedException();
            }

            public void Close()
            {
                throw new NotImplementedException();
            }

            public byte[] Get(Bytes key)
            {
                throw new NotImplementedException();
            }

            public IKeyValueEnumerator<Bytes, byte[]> Range(Bytes @from, Bytes to)
            {
                throw new NotImplementedException();
            }

            public IKeyValueEnumerator<Bytes, byte[]> ReverseRange(Bytes @from, Bytes to)
            {
                throw new NotImplementedException();
            }

            public IEnumerable<KeyValuePair<Bytes, byte[]>> All()
            {
                throw new NotImplementedException();
            }

            public IEnumerable<KeyValuePair<Bytes, byte[]>> ReverseAll()
            {
                throw new NotImplementedException();
            }

            public long ApproximateNumEntries()
            {
                throw new NotImplementedException();
            }

            public void Put(Bytes key, byte[] value)
            {
                throw new NotImplementedException();
            }

            public byte[] PutIfAbsent(Bytes key, byte[] value)
            {
                throw new NotImplementedException();
            }

            public void PutAll(IEnumerable<KeyValuePair<Bytes, byte[]>> entries)
            {
                throw new NotImplementedException();
            }

            public byte[] Delete(Bytes key)
            {
                throw new NotImplementedException();
            }
        }

        private class StoreThrowExceptionSupplier : IKeyValueBytesStoreSupplier
        {
            public string Name { get; set; } = "test";

            public IKeyValueStore<Bytes, byte[]> Get()
                => new StoreThrowExceptionStore();

            public string MetricsScope => "test";
        }

        private class StoreThrowException : Materialized<String, String, IKeyValueStore<Bytes, byte[]>>
        {
            public StoreThrowException(string storeName, IStoreSupplier<IKeyValueStore<Bytes, byte[]>> storeSupplier) 
                : base(storeName, storeSupplier)
            {
            }
        }

        private readonly CancellationTokenSource token1 = new System.Threading.CancellationTokenSource();

        private readonly StreamConfig<StringSerDes, StringSerDes> config =
            new();

        private MockKafkaSupplier mockKafkaSupplier;
        private StreamThread thread1;

        [SetUp]
        public void Init()
        {
            config.ApplicationId = "test-stream-thread";
            config.StateDir = Guid.NewGuid().ToString();
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 10;

            mockKafkaSupplier = new MockKafkaSupplier(4);

            var builder = new StreamBuilder();
            
            builder.Table("table",
                new StoreThrowException("store", new StoreThrowExceptionSupplier()));

            var topo = builder.Build();
            

            thread1 = StreamThread.Create(
                "thread-0", "c0",
                topo.Builder, new StreamMetricsRegistry(), config,
                mockKafkaSupplier, mockKafkaSupplier.GetAdmin(config.ToAdminConfig("admin")),
                0) as StreamThread;
        }

        [TearDown]
        public void Dispose()
        {
            token1.Cancel();
            thread1.Dispose();
            mockKafkaSupplier.Destroy();
        }
        
        [Test]
        public void StreamThreadThrowExceptionDuringAssigment()
        {
            thread1.Start(token1.Token);
            
            AssertExtensions.WaitUntil(() => thread1.State == ThreadState.DEAD, TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(10));
        }
    }
}