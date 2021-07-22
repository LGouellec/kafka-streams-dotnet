using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamJoinWindowProcessor<K, V> : AbstractProcessor<K, V>
    {
        private readonly string storeName;
        private IWindowStore<K, V> window;

        public KStreamJoinWindowProcessor(string storeName)
        {
            this.storeName = storeName;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);

            window = (IWindowStore<K, V>)context.GetStateStore(storeName);
        }

        public override void Process(K key, V value)
        {
            if(key != null)
            {
                Forward(key, value);
                window.Put(key, value, Context.Timestamp);
            }
        }
    }
}
