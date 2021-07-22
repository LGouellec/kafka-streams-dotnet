using Streamiz.Kafka.Net.Mock.Pipes;
using Streamiz.Kafka.Net.Processors;
using System;
using System.Threading;

namespace Streamiz.Kafka.Net.Mock.Sync
{
    internal class SyncPipeBuilder : IPipeBuilder
    {
        private readonly StreamTask task;
        private readonly SyncProducer mockProducer;

        public SyncPipeBuilder(StreamTask task)
        {
            this.task = task;
        }

        public SyncPipeBuilder(StreamTask task, SyncProducer mockProducer)
        {
            this.mockProducer = mockProducer;
            this.task = task;
        }

        public IPipeInput Input(string topic, IStreamConfig configuration)
        {
            return new SyncPipeInput(task, topic);
        }

        public IPipeOutput Output(string topic, TimeSpan consumeTimeout, IStreamConfig configuration, CancellationToken token = default)
        {
            return new SyncPipeOutput(topic, consumeTimeout, configuration, mockProducer, token);
        }
    }
}