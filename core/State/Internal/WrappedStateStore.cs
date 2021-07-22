using Confluent.Kafka;
using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class WrappedStateStore<S> : IStateStore
        where S : IStateStore
    {
        protected ProcessorContext context;
        protected readonly S wrapped;

        public WrappedStateStore(S wrapped)
        {
            this.wrapped = wrapped;
        }

        #region StateStore Impl

        public string Name => wrapped.Name;

        public bool Persistent => wrapped.Persistent;

        public bool IsOpen => wrapped.IsOpen;

        public void Close() => wrapped.Close();

        public void Flush() => wrapped.Flush();

        public virtual void Init(ProcessorContext context, IStateStore root)
        {
            this.context = context;
            wrapped.Init(context, root);
        }

        #endregion

        protected SerializationContext GetSerializationContext(bool isKey)
        {
            return new SerializationContext(isKey ? MessageComponentType.Key : MessageComponentType.Value,
                context?.RecordContext?.Topic,
                context?.RecordContext?.Headers);
        }
    }
}
