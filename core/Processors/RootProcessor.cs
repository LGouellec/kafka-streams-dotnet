using System.Collections.Generic;
using Confluent.Kafka;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Processors
{
    internal class RootProcessor : IProcessor
    {
        public string Name
        {
            get => "ROOT-OPERATOR";
            set { }
        }

        public IList<IProcessor> Previous { get; } = new List<IProcessor>();

        public IList<IProcessor> Next { get; } = new List<IProcessor>();

        public ISerDes Key => null;

        public ISerDes Value => null;

        public IList<string> StateStores => new List<string>();

        public void Close()
        {
            
        }

        public void Init(ProcessorContext context)
        {
            foreach (var n in Next)
                n.Init(context);
        }

        public void Process(ConsumeResult<byte[], byte[]> record)
        {
            foreach (var n in Next)
                n.Process(record);
        }

        public void Process(object key, object value)
        {
            foreach (var n in Next)
                n.Process(key, value);
        }

        public void AddNextProcessor(IProcessor next)
        {
            if (!Next.Contains(next) && next != null)
                Next.Add(next);
        }

        public void SetPreviousProcessor(IProcessor prev)
        {
            if (!Previous.Contains(prev) && prev != null)
                Previous.Add(prev);
        }

        public void Forward<K1, V1>(K1 key, V1 value)
        {
            foreach (var n in Next)
                if(n is IProcessor<K1, V1>)
                    n.Process(key, value);
        }

        public void Forward<K1, V1>(K1 key, V1 value, string name)
        {
            foreach (var n in Next)
                if (n is IProcessor<K1, V1> && n.Name.Equals(name))
                    n.Process(key, value);
        }

        public void Forward<K1, V1>(K1 key, V1 value, long ts)
        {
            Forward(key, value);
        }
    }
}
