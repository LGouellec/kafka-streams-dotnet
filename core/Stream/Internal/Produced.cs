﻿using System;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    internal class Produced<K, V>
    {
        internal ISerDes<K> KeySerdes { get; }
        internal ISerDes<V> ValueSerdes { get; }
        internal string Named { get; private set; }
        
        internal Func<string, K, V, int> Partitioner { get; private set; }
        
        internal Produced(ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            KeySerdes = keySerdes;
            ValueSerdes = valueSerdes;
        }

        public static Produced<K, V> Create<KS, VS>()
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            return new Produced<K, V>(new KS(), new VS());
        }

        internal static Produced<K, V> Create(ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            return new Produced<K, V>(keySerdes, valueSerdes);
        }

        internal Produced<K, V> WithName(string named)
        {
            Named = named;
            return this;
        }

        internal Produced<K, V> WithPartitioner(Func<string, K, V, int> partitioner)
        {
            this.Partitioner = partitioner;
            return this;
        }
    }
}
