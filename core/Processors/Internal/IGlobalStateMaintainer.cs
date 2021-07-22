using Confluent.Kafka;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal interface IGlobalStateMaintainer
    {
        public void Update(ConsumeResult<byte[], byte[]> record);

        public void FlushState();

        public void Close();

        public IDictionary<TopicPartition, long> Initialize();
    }
}
