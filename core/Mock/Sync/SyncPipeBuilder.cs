﻿using Streamiz.Kafka.Net.Mock.Pipes;
using Streamiz.Kafka.Net.Processors;
using System;
using System.Threading;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Mock.Sync
{
    internal class SyncPipeBuilder : IPipeBuilder
    {
        private class StreamTaskPublisher : ISyncPublisher
        {
            private readonly StreamTask task;

            public StreamTaskPublisher(StreamTask task)
            {
                this.task = task;
            }

            private int offset = 0;

            public void PublishRecord(string topic, byte[] key, byte[] value, DateTime timestamp, Headers headers)
                => task.AddRecord(new ConsumeResult<byte[], byte[]>
                {
                    Topic = topic,
                    TopicPartitionOffset = new TopicPartitionOffset(new TopicPartition(topic, task.Id.Partition), offset++),
                    Message = new Message<byte[], byte[]> { Key = key, Value = value, Timestamp = new Timestamp(timestamp), Headers = headers }
                });

            public void Flush()
            {
                long now = DateTime.Now.GetMilliseconds();
                while (task.CanProcess(now))
                    task.Process();
            }
        }

        private class GlobalTaskPublisher : ISyncPublisher
        {
            private readonly GlobalStateUpdateTask globalTask;
            private int offset = 0;
            
            public GlobalTaskPublisher(GlobalStateUpdateTask globalTask)
            {
                this.globalTask = globalTask;
            }

            public void PublishRecord(string topic, byte[] key, byte[] value, DateTime timestamp, Headers headers)
            {
                globalTask.Update(new ConsumeResult<byte[], byte[]>
                {
                    Topic = topic,
                    TopicPartitionOffset = new TopicPartitionOffset(new TopicPartition(topic, 0), offset++),
                    Message = new Message<byte[], byte[]> { Key = key, Value = value, Timestamp = new Timestamp(timestamp), Headers = headers }
                });
            }

            public void Flush()
            {
                globalTask.FlushState();
            }
        }
        
        private readonly ISyncPublisher publisher;
        private readonly SyncProducer mockProducer;

        public SyncPipeBuilder(StreamTask task)
        {
            publisher = new StreamTaskPublisher(task);
        }
        
        public SyncPipeBuilder(GlobalStateUpdateTask globalTask)
        {
            publisher = new GlobalTaskPublisher(globalTask);
        }

        public SyncPipeBuilder(SyncProducer mockProducer)
        {
            this.mockProducer = mockProducer;
        }

        public IPipeInput Input(string topic, IStreamConfig configuration)
        {
            return new SyncPipeInput(publisher, topic);
        }

        public IPipeOutput Output(string topic, TimeSpan consumeTimeout, IStreamConfig configuration, CancellationToken token = default)
        {
            return new SyncPipeOutput(topic, consumeTimeout, configuration, mockProducer, token);
        }
    }
}