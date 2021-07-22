using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class TaskMigratedExceptionTests
    {
        #region Inner class

        internal class ProducerSyncExceptionOptions
        {
            public bool IsFatal { get; set; } = false;
            public bool IsRecoverable { get; set; } = false;
            public bool IsProductionException { get; set; } = false;
        }

        internal class ProducerSyncException : SyncKafkaSupplier
        {
            private KafkaProducerException producerException = null;
            private readonly ProducerSyncExceptionOptions options = null;

            public ProducerSyncException(ProducerSyncExceptionOptions options)
            {
                this.options = options;
            }

            public override IProducer<byte[], byte[]> GetProducer(ProducerConfig config)
            {
                if (producerException == null)
                {
                    var p = base.GetProducer(config) as SyncProducer;
                    producerException = new KafkaProducerException(p, options);
                }
                return producerException;
            }
        }

        internal class KafkaProducerException : IProducer<byte[], byte[]>
        {
            private SyncProducer innerProducer;
            private ProducerSyncExceptionOptions options;
            private bool handleError = true;

            public KafkaProducerException(SyncProducer syncProducer)
            {
                this.innerProducer = syncProducer;
            }

            public KafkaProducerException(SyncProducer syncProducer, ProducerSyncExceptionOptions options) 
                : this(syncProducer)
            {
                this.options = options;
            }

            public Handle Handle => throw new NotImplementedException();

            public string Name => "";

            public void AbortTransaction(TimeSpan timeout)
            {
            }

            public int AddBrokers(string brokers)
            {
                return 0;
            }

            public void BeginTransaction()
            {
            }

            public void CommitTransaction(TimeSpan timeout)
            {
            }

            public void Dispose()
            {
            }

            public int Flush(TimeSpan timeout)
            {
                return 0;
            }

            public void Flush(CancellationToken cancellationToken = default)
            {
            }

            public void InitTransactions(TimeSpan timeout)
            {

            }

            public int Poll(TimeSpan timeout)
            {
                return 0;
            }

            private void HandleError(Action<DeliveryReport<byte[], byte[]>> deliveryHandler)
            {
                handleError = false;
                if (options.IsProductionException)
                {
                    if (options.IsRecoverable)
                    {
                        throw new ProduceException<byte[], byte[]>(
                            new Error(ErrorCode.TransactionCoordinatorFenced, "TransactionCoordinatorFenced", false),
                            new DeliveryResult<byte[], byte[]>());
                    }
                    else
                    {
                        throw new ProduceException<byte[], byte[]>(
                            new Error(ErrorCode.Local_InvalidArg, "Invalid arg", false),
                            new DeliveryResult<byte[], byte[]>());
                    }
                }
                else
                {
                    if (options.IsFatal)
                    {
                        deliveryHandler(new DeliveryReport<byte[], byte[]>()
                        {
                            Error = new Error(ErrorCode.TopicAuthorizationFailed, "TopicAuthorizationFailed", true)
                        });
                    }
                    else if (options.IsRecoverable)
                    {
                        deliveryHandler(new DeliveryReport<byte[], byte[]>()
                        {
                            Error = new Error(ErrorCode.TransactionCoordinatorFenced, "TransactionCoordinatorFenced", false)
                        });
                    }
                    else
                    {
                        deliveryHandler(new DeliveryReport<byte[], byte[]>()
                        {
                            Error = new Error(ErrorCode.Local_InvalidArg, "Invalid arg", false)
                        });
                    }
                }

            }

            public void Produce(string topic, Message<byte[], byte[]> message, Action<DeliveryReport<byte[], byte[]>> deliveryHandler = null)
            {
                if (topic == "test" || !handleError)
                    innerProducer.Produce(topic, message, deliveryHandler);
                else
                    HandleError(deliveryHandler);
            }

            public void Produce(TopicPartition topicPartition, Message<byte[], byte[]> message, Action<DeliveryReport<byte[], byte[]>> deliveryHandler = null)
            {
                if (topicPartition.Topic == "test" || !handleError)
                    innerProducer.Produce(topicPartition, message, deliveryHandler);
                else
                    HandleError(deliveryHandler);
            }

            public async Task<DeliveryResult<byte[], byte[]>> ProduceAsync(string topic, Message<byte[], byte[]> message, CancellationToken cancellationToken = default)
            {
                if (topic == "test")
                    return await innerProducer.ProduceAsync(topic, message, cancellationToken);
                else
                    throw new NotImplementedException();
            }

            public async Task<DeliveryResult<byte[], byte[]>> ProduceAsync(TopicPartition topicPartition, Message<byte[], byte[]> message, CancellationToken cancellationToken = default)
            {
                if (topicPartition.Topic == "test")
                    return await innerProducer.ProduceAsync(topicPartition, message, cancellationToken);
                else
                    throw new NotImplementedException();
            }

            public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout)
            {
                throw new NotImplementedException();
            }

            public void CommitTransaction()
            {
                throw new NotImplementedException();
            }

            public void AbortTransaction()
            {
                throw new NotImplementedException();
            }
        }

        #endregion

        [Test]
        public void ProductionExceptionFatalHandlerFailTest()
        {
            bool errorState = false;
            var _return = new List<KeyValuePair<string, string>>();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            var dt = DateTime.Now;
            var timeout = TimeSpan.FromSeconds(10);

            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;
            config.ProductionExceptionHandler += (r) => ExceptionHandlerResponse.FAIL;

            var options = new ProducerSyncExceptionOptions { IsFatal = true };
            var supplier = new ProducerSyncException(options);

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("test")
                .To("test-output");

            builder.Stream<string, string>("test-output")
                .Peek((k, v) => _return.Add(KeyValuePair.Create(k, v)));

            var t = builder.Build();

            using (var driver = new TopologyTestDriver(t.Builder, config, TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY, supplier))
            {
                var inputtopic = driver.CreateInputTopic<string, string>("test");
                inputtopic.PipeInput("coucou");
                while (!errorState)
                {
                    errorState = driver.IsError;
                    Thread.Sleep(10);
                    if (DateTime.Now > dt + timeout)
                    {
                        break;
                    }
                }
                Assert.IsTrue(driver.IsError);
            }

            Assert.AreEqual(new List<KeyValuePair<string, string>>(), _return);
        }

        [Test]
        public void ProductionExceptionRecoverableHandlerFailTest()
        {
            var _return = new List<KeyValuePair<string, string>>();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            var dt = DateTime.Now;
            var timeout = TimeSpan.FromSeconds(10);

            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;
            config.ProductionExceptionHandler += (r) => ExceptionHandlerResponse.FAIL;

            var options = new ProducerSyncExceptionOptions { IsRecoverable = true };
            var supplier = new ProducerSyncException(options);

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("test")
                .To("test-output");

            builder.Stream<string, string>("test-output")
                .Peek((k, v) => _return.Add(KeyValuePair.Create(k, v)));

            var t = builder.Build();

            using (var driver = new TopologyTestDriver(t.Builder, config, TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY, supplier))
            {
                var inputtopic = driver.CreateInputTopic<string, string>("test");
                var outputTopic = driver.CreateOuputTopic<string, string>("test-output");
                inputtopic.PipeInput("coucou");
                while (_return.Count == 0) ;
                var expected = new List<KeyValuePair<string, string>>();
                expected.Add(KeyValuePair.Create<string, string>(null, "coucou"));
                Assert.AreEqual(expected, _return);
            }
        }

        [Test]
        public void ProduceExceptionRecoverableHandlerFailTest()
        {
            var _return = new List<KeyValuePair<string, string>>();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            var dt = DateTime.Now;
            var timeout = TimeSpan.FromSeconds(10);

            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;
            config.ProductionExceptionHandler += (r) => ExceptionHandlerResponse.FAIL;

            var options = new ProducerSyncExceptionOptions { IsRecoverable = true, IsProductionException = true };
            var supplier = new ProducerSyncException(options);

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("test")
                .To("test-output");

            builder.Stream<string, string>("test-output")
                .Peek((k, v) => _return.Add(KeyValuePair.Create(k, v)));

            var t = builder.Build();

            using (var driver = new TopologyTestDriver(t.Builder, config, TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY, supplier))
            {
                var inputtopic = driver.CreateInputTopic<string, string>("test");
                var outputTopic = driver.CreateOuputTopic<string, string>("test-output");
                inputtopic.PipeInput("coucou");
                while (_return.Count == 0) ;
                var expected = new List<KeyValuePair<string, string>>();
                expected.Add(KeyValuePair.Create<string, string>(null, "coucou"));
                Assert.AreEqual(expected, _return);
            }
        }

        [Test]
        public void ProduceExceptionNotRecoverableHandlerFailTest()
        {
            bool errorState = false;
            var _return = new List<KeyValuePair<string, string>>();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            var dt = DateTime.Now;
            var timeout = TimeSpan.FromSeconds(10);

            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;
            config.ProductionExceptionHandler += (r) => ExceptionHandlerResponse.FAIL;

            var options = new ProducerSyncExceptionOptions { IsRecoverable = false, IsProductionException = true };
            var supplier = new ProducerSyncException(options);

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("test")
                .To("test-output");

            builder.Stream<string, string>("test-output")
                .Peek((k, v) => _return.Add(KeyValuePair.Create(k, v)));

            var t = builder.Build();

            using (var driver = new TopologyTestDriver(t.Builder, config, TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY, supplier))
            {
                var inputtopic = driver.CreateInputTopic<string, string>("test");
                inputtopic.PipeInput("coucou");
                while (!errorState)
                {
                    errorState = driver.IsError;
                    Thread.Sleep(10);
                    if (DateTime.Now > dt + timeout)
                    {
                        break;
                    }
                }
                Assert.IsTrue(driver.IsError);
            }
        }
    }
}
