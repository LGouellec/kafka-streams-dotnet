using Confluent.Kafka;
using Streamiz.Kafka.Net.SerDes;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors
{
    internal interface IProcessor
    {
        string Name { get; set; }
        IList<string> StateStores {get;}
        ISerDes Key { get; }
        ISerDes Value { get; }
        void Init(ProcessorContext context);
        IList<IProcessor> Next { get; }
        void AddNextProcessor(IProcessor next);
        void Close();
        void Process(ConsumeResult<byte[], byte[]> record);
        void Process(object key, object value);
        void Forward<K1, V1>(K1 key, V1 value);
        void Forward<K1, V1>(K1 key, V1 value, string name);
        void Forward<K1, V1>(K1 key, V1 value, long ts);
    }

    internal interface IProcessor<K,V> : IProcessor
    {
        ISerDes<K> KeySerDes { get; }
        ISerDes<V> ValueSerDes { get; }
        void Process(K key, V value);
        void Forward(K key, V value);
        void Forward(K key, V value, string name);
    }
}
