using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Table.Internal.Graph
{
    internal class KTableMapValues<K, V, VR> : IKTableProcessorSupplier<K, V, VR>
    {
        internal class KTableMapValuesValueGetter : IKTableValueGetter<K, VR>
        {
            private readonly IKTableValueGetter<K, V> ktablegetter;
            private readonly IValueMapperWithKey<K, V, VR> mapper;

            public KTableMapValuesValueGetter(IValueMapperWithKey<K, V, VR> mapper, IKTableValueGetter<K, V> getter)
            {
                this.ktablegetter = getter;
                this.mapper = mapper;
            }

            public void Close() => ktablegetter.Close();

            public ValueAndTimestamp<VR> Get(K key) => ComputeValue(key, ktablegetter.Get(key));

            public void Init(ProcessorContext context) => ktablegetter.Init(context);

            private ValueAndTimestamp<VR> ComputeValue(K key, ValueAndTimestamp<V> valueAndTimestamp)
            {
                VR newValue = default(VR);
                long timestamp = 0;

                if (valueAndTimestamp != null)
                {
                    newValue = mapper.Apply(key, valueAndTimestamp.Value);
                    timestamp = valueAndTimestamp.Timestamp;
                }

                return ValueAndTimestamp<VR>.Make(newValue, timestamp);
            }
        }

        private readonly IKTableGetter<K, V> parentTable;
        private readonly IValueMapperWithKey<K, V, VR> mapper;
        private readonly string queryableName;
        private bool sendOldValues = false;

        public KTableMapValues(IKTableGetter<K, V> parent, IValueMapperWithKey<K, V, VR> mapper, string queryableName)
        {
            this.parentTable = parent;
            this.mapper = mapper;
            this.queryableName = queryableName;
        }

        public IKTableValueGetterSupplier<K, VR> View
        {
            get
            {
                // if the KTable is materialized, use the materialized store to return getter value;
                // otherwise rely on the parent getter and apply filter on-the-fly
                if (this.queryableName != null)
                {
                    return new KTableMaterializedValueGetterSupplier<K, VR>(queryableName);
                }
                else
                {
                    var supplier = parentTable.ValueGetterSupplier;
                    return new GenericKTableValueGetterSupplier<K, VR>(
                        supplier.StoreNames,
                        new KTableMapValuesValueGetter(this.mapper, supplier.Get()));
                }
            }
        }

        public void EnableSendingOldValues()
        {
            parentTable.EnableSendingOldValues();
            sendOldValues = true;
        }

        public IProcessor<K, Change<V>> Get() => new KTableMapValuesProcessor<K, V, VR>(mapper, sendOldValues, queryableName);
    }
}
