using System;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream.Internal.Graph;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    internal class TimeWindowedKStream<K, V, W> : AbstractStream<K, V>, ITimeWindowedKStream<K, V>
        where W : Window
    {
        private readonly WindowOptions<W> windowOptions;
        private readonly GroupedStreamAggregateBuilder<K, V> aggBuilder;

        public TimeWindowedKStream(WindowOptions<W> windowOptions, GroupedStreamAggregateBuilder<K, V> aggBuilder, string name, ISerDes<K> keySerde, ISerDes<V> valSerde, List<string> sourceNodes, StreamGraphNode streamsGraphNode, InternalStreamBuilder builder) 
            : base(name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder)
        {
            CheckIfParamNull(windowOptions, "windowOptions");

            this.windowOptions = windowOptions;
            this.aggBuilder = aggBuilder;
        }

        #region ITimeWindowedKStream Impl

        #region Count

        public IKTable<Windowed<K>, long> Count()
             => Count((string)null);

        public IKTable<Windowed<K>, long> Count(string named)
            => Count(Materialized<K, long, IWindowStore<Bytes, byte[]>>.Create(), named);

        public IKTable<Windowed<K>, long> Count(Materialized<K, long, IWindowStore<Bytes, byte[]>> materialized, string named = null)
        {
            materialized = materialized ?? Materialized<K, long, IWindowStore<Bytes, byte[]>>.Create();

            return DoCount(materialized, named);
        }

        #endregion

        #region Aggregate

        public IKTable<Windowed<K>, VR> Aggregate<VR>(Func<VR> initializer, Func<K, V, VR, VR> aggregator)
            => Aggregate(new WrappedInitializer<VR>(initializer), new WrappedAggregator<K, V, VR>(aggregator));

        public IKTable<Windowed<K>, VR> Aggregate<VR>(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator)
            => Aggregate(initializer, aggregator, null);

        public IKTable<Windowed<K>, VR> Aggregate<VR, VRS>(Func<VR> initializer, Func<K, V, VR, VR> aggregator) where VRS : ISerDes<VR>, new()
            => Aggregate<VR, VRS>(new WrappedInitializer<VR>(initializer), new WrappedAggregator<K, V, VR>(aggregator));

        public IKTable<Windowed<K>, VR> Aggregate<VR, VRS>(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator) where VRS : ISerDes<VR>, new()
        {
            var materialized = Materialized<K, VR, IWindowStore<Bytes, byte[]>>.Create().WithValueSerdes(new VRS()).WithKeySerdes(KeySerdes);
            return Aggregate(initializer, aggregator, materialized);
        }

        public IKTable<Windowed<K>, VR> Aggregate<VR>(Func<VR> initializer, Func<K, V, VR, VR> aggregator, Materialized<K, VR, IWindowStore<Bytes, byte[]>> materialized, string named = null)
            => Aggregate(new WrappedInitializer<VR>(initializer), new WrappedAggregator<K, V, VR>(aggregator), materialized);

        public IKTable<Windowed<K>, VR> Aggregate<VR>(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator, Materialized<K, VR, IWindowStore<Bytes, byte[]>> materialized, string named = null)
        {
            CheckIfParamNull(initializer, "initializer");
            CheckIfParamNull(aggregator, "aggregator");

            materialized = materialized ?? Materialized<K, VR, IWindowStore<Bytes, byte[]>>.Create();

            if (materialized.KeySerdes == null)
                materialized.WithKeySerdes(KeySerdes);

            string name = new Named(named).OrElseGenerateWithPrefix(builder, KGroupedStream.AGGREGATE_NAME);
            materialized.UseProvider(builder, KGroupedStream.AGGREGATE_NAME);

            var aggSupplier = new KStreamWindowAggregate<K, V, VR, W>(
                windowOptions,
                materialized.StoreName,
                initializer,
                aggregator);

            ISerDes<Windowed<K>> windowSerdes = materialized.KeySerdes != null ? new TimeWindowedSerDes<K>(materialized.KeySerdes, windowOptions.Size) : null;

            return aggBuilder.BuildWindow(name,
                                    new TimestampedWindowStoreMaterializer<K, VR, W>(windowOptions, materialized).Materialize(),
                                    aggSupplier,
                                    materialized.QueryableStoreName,
                                    windowSerdes,
                                    materialized.ValueSerdes);
        }

        #endregion

        #region Reduce

        public IKTable<Windowed<K>, V> Reduce(Reducer<V> reducer)
            => Reduce(reducer, null);

        public IKTable<Windowed<K>, V> Reduce(Func<V, V, V> reducer)
            => Reduce(new WrappedReducer<V>(reducer));

        public IKTable<Windowed<K>, V> Reduce(Reducer<V> reducer, Materialized<K, V, IWindowStore<Bytes, byte[]>> materialized, string named = null)
        {
            CheckIfParamNull(reducer, "reducer");

            materialized = materialized ?? Materialized<K, V, IWindowStore<Bytes, byte[]>>.Create();

            if (materialized.KeySerdes == null)
                materialized.WithKeySerdes(KeySerdes);

            if (materialized.ValueSerdes == null)
                materialized.WithValueSerdes(ValueSerdes);

            string name = new Named(named).OrElseGenerateWithPrefix(builder, KGroupedStream.REDUCE_NAME);
            materialized.UseProvider(builder, KGroupedStream.REDUCE_NAME);

            var aggSupplier = new KStreamWindowAggregate<K, V, V, W>(
                windowOptions,
                materialized.StoreName,
                () => default,
                (aggKey, value, agg) => agg == null ? value : reducer.Apply(agg, value));

            ISerDes<Windowed<K>> windowSerdes = materialized.KeySerdes != null ? new TimeWindowedSerDes<K>(materialized.KeySerdes, windowOptions.Size) : null;

            return aggBuilder.BuildWindow(name,
                                    new TimestampedWindowStoreMaterializer<K, V, W>(windowOptions, materialized).Materialize(),
                                    aggSupplier,
                                    materialized.QueryableStoreName,
                                    windowSerdes,
                                    materialized.ValueSerdes);
        }

        public IKTable<Windowed<K>, V> Reduce(Func<V, V, V> reducer, Materialized<K, V, IWindowStore<Bytes, byte[]>> materialized, string named = null)
            => Reduce(new WrappedReducer<V>(reducer), materialized);


        #endregion

        #endregion

        #region Privates

        private IKTable<Windowed<K>, long> DoCount(Materialized<K, long, IWindowStore<Bytes, byte[]>> materialized, string named = null)
        {
            if (materialized.KeySerdes == null)
                materialized.WithKeySerdes(KeySerdes);

            if (materialized.ValueSerdes == null)
                materialized.WithValueSerdes(new Int64SerDes());

            string name = new Named(named).OrElseGenerateWithPrefix(builder, KGroupedStream.AGGREGATE_NAME);
            materialized.UseProvider(builder, KGroupedStream.AGGREGATE_NAME);

            var aggSupplier = new KStreamWindowAggregate<K, V, long, W>(
                windowOptions,
                materialized.StoreName,
                () => 0L,
                (aggKey, value, aggregate) => aggregate + 1);

            ISerDes<Windowed<K>> windowSerdes = materialized.KeySerdes != null ? new TimeWindowedSerDes<K>(materialized.KeySerdes, windowOptions.Size) : null;

            return aggBuilder.BuildWindow(name,
                                    new TimestampedWindowStoreMaterializer<K, long, W>(windowOptions, materialized).Materialize(),
                                    aggSupplier,
                                    materialized.QueryableStoreName,
                                    windowSerdes,
                                    materialized.ValueSerdes);
        }

        #endregion
    }
}
