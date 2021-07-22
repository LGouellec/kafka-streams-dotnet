using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Table.Internal.Graph
{
    internal abstract class AbstractKTableKTableJoin<K, VR, V1, V2> : IKTableProcessorSupplier<K, V1, VR>
    {
        protected bool sendOldValues = false;
        protected readonly IKTableGetter<K, V1> table1;
        protected readonly IKTableGetter<K, V2> table2;
        protected readonly IValueJoiner<V1, V2, VR> valueJoiner;

        public abstract IKTableValueGetterSupplier<K, VR> View { get; }
        public abstract IProcessor<K, Change<V1>> Get();


        public AbstractKTableKTableJoin(
            IKTableGetter<K, V1> table1,
            IKTableGetter<K, V2> table2,
            IValueJoiner<V1, V2, VR> valueJoiner)
        {
            this.table1 = table1;
            this.table2 = table2;
            this.valueJoiner = valueJoiner;
        }


        public void EnableSendingOldValues()
        {
            table1.EnableSendingOldValues();
            table2.EnableSendingOldValues();
            sendOldValues = true;
        }
    }
}
