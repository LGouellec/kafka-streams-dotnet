﻿using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Logging;
using Streamiz.Kafka.Net.State.Metered;
using Streamiz.Kafka.Net.State.Supplier;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// Timestamp window store builder
    /// </summary>
    /// <typeparam name="K"></typeparam>
    /// <typeparam name="V"></typeparam>
    public class TimestampedWindowStoreBuilder<K, V>
        : AbstractStoreBuilder<K, ValueAndTimestamp<V>, ITimestampedWindowStore<K, V>>
    {
        private readonly IWindowBytesStoreSupplier supplier;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="supplier"></param>
        /// <param name="keySerde"></param>
        /// <param name="valueSerde"></param>
        public TimestampedWindowStoreBuilder(IWindowBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde)
            : base(supplier.Name, keySerde, valueSerde == null ? null : new ValueAndTimestampSerDes<V>(valueSerde))
        {
            this.supplier = supplier;
        }

        /// <summary>
        /// True everytime
        /// </summary>
        public override bool IsWindowStore => true;
        
        /// <summary>
        /// Retention of the window store
        /// </summary>
        public override long RetentionMs => supplier.Retention;

        /// <summary>
        /// Build the state store
        /// </summary>
        /// <returns></returns>
        public override ITimestampedWindowStore<K, V> Build()
        {
            var store = supplier.Get();
            return new MeteredTimestampedWindowStore<K, V>(
                WrapLogging(store),
                supplier.WindowSize.Value,
                keySerdes,
                valueSerdes,
                supplier.MetricsScope);
        }

        private IWindowStore<Bytes, byte[]> WrapLogging(IWindowStore<Bytes, byte[]> inner)
        {
            if (!LoggingEnabled)
                return inner;

            return new ChangeLoggingTimestampedWindowBytesStore(inner);
        }
    }
}
