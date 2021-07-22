using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Supplier;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class TimestampedWindowStoreMaterializer<K, V, W>
        where W : Window
    {
        private readonly WindowOptions<W> windowsOptions;
        private readonly Materialized<K, V, IWindowStore<Bytes, byte[]>> materializedInternal;

        public TimestampedWindowStoreMaterializer(WindowOptions<W> windowsOptions, Materialized<K, V, IWindowStore<Bytes, byte[]>> materializedInternal)
        {
            this.windowsOptions = windowsOptions;
            this.materializedInternal = materializedInternal;
        }

        public StoreBuilder<ITimestampedWindowStore<K, V>> Materialize()
        {
            IWindowBytesStoreSupplier supplier = (IWindowBytesStoreSupplier)materializedInternal.StoreSupplier;
            if (supplier == null)
            {
                if (windowsOptions.Size + windowsOptions.GracePeriodMs > materializedInternal.Retention.TotalMilliseconds)
                    throw new ArgumentException($"The retention period of the window store { materializedInternal.StoreName } must be no smaller than its window size plus the grace period. Got size=[{windowsOptions.Size}], grace=[{windowsOptions.GracePeriodMs}], retention=[{materializedInternal.Retention.TotalMilliseconds}].");

                supplier = Stores.DefaultWindowStore(
                    materializedInternal.StoreName,
                    materializedInternal.Retention,
                    TimeSpan.FromMilliseconds(windowsOptions.Size));
            }
            else
                supplier.WindowSize = !supplier.WindowSize.HasValue ? windowsOptions.Size : supplier.WindowSize.Value;

            var builder = Stores.TimestampedWindowStoreBuilder(supplier, materializedInternal.KeySerdes, materializedInternal.ValueSerdes);

            if (materializedInternal.LoggingEnabled)
                builder.WithLoggingEnabled(materializedInternal.TopicConfig);
            else
                builder.WithLoggingDisabled();

            if (materializedInternal.CachingEnabled)
                builder.WithCachingEnabled();

            return builder;
        }
    }
}
