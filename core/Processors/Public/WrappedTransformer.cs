using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors.Public
{
    public class WrappedTransformer<K, V, K1, V1> : ITransformer<K, V, K1, V1>
    {
        private readonly Func<Record<K, V>, KeyValuePair<K1, V1>> transformer;

        public WrappedTransformer(Func<Record<K, V>, KeyValuePair<K1, V1>> transformer)
        {
            this.transformer = transformer;
        }

        public void Init(ProcessorContext context)
        { }

        public KeyValuePair<K1, V1> Process(Record<K, V> record)
            => transformer.Invoke(record);

        public void Close()
        { }
    }
}