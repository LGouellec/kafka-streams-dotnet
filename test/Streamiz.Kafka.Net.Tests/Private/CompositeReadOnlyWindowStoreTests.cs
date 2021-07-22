using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.InMemory;
using Streamiz.Kafka.Net.State.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class CompositeReadOnlyWindowStoreTests
    {
        class MockStateProvider<K, V> : IStateStoreProvider<IReadOnlyWindowStore<K, V>, K, V>
        {
            private readonly List<ITimestampedWindowStore<K, V>> stores = new List<ITimestampedWindowStore<K, V>>();

            public MockStateProvider(long windowSize, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, params InMemoryWindowStore[] stores)
            {
                this.stores = stores
                                .Select(s => new TimestampedWindowStore<K, V>(s, windowSize, keySerdes, new ValueAndTimestampSerDes<V>(valueSerdes)))
                                .Cast<ITimestampedWindowStore<K, V>>()
                                .ToList();
            }

            public IEnumerable<IReadOnlyWindowStore<K, V>> Stores(string storeName, IQueryableStoreType<IReadOnlyWindowStore<K, V>, K, V> queryableStoreType)
            {
                return
                    stores
                        .Where(s => s.Name.Equals(storeName))
                        .Select<ITimestampedWindowStore<K, V>, IReadOnlyWindowStore<K, V>>(s =>
                        {
                            if (s is IReadOnlyWindowStore<K, V>)
                            {
                                return s as IReadOnlyWindowStore<K, V>;
                            }
                            else if (s is ITimestampedWindowStore<K, V>)
                            {
                                return new ReadOnlyWindowStoreFacade<K, V>(s);
                            }
                            else
                            {
                                return null;
                            }
                        });
            }
        }

        [Test]
        public void FetchTest()
        {
            InMemoryWindowStore store1 = new InMemoryWindowStore("store", TimeSpan.FromDays(1), 1000 * 10);
            InMemoryWindowStore store2 = new InMemoryWindowStore("store", TimeSpan.FromDays(1), 1000 * 10);
            var dt = DateTime.Now.GetMilliseconds();
            var valueSerdes = new ValueAndTimestampSerDes<string>(new StringSerDes());
            var bytes = new Bytes(Encoding.UTF8.GetBytes("test"));
            var provider = new MockStateProvider<string, string>(1000 * 10, new StringSerDes(), new StringSerDes(), store1, store2);
            var composite = new CompositeReadOnlyWindowStore<string, string>(provider, new WindowStoreType<string, string>(), "store");
            store1.Put(bytes, valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou1", dt), new SerializationContext()), dt);
            var result = composite.Fetch("test", dt);
            Assert.IsNotNull(result);
            Assert.AreEqual("coucou1", result);
        }

        [Test]
        public void Fetch2Test()
        {
            InMemoryWindowStore store1 = new InMemoryWindowStore("store", TimeSpan.FromDays(1), 1000 * 10);
            InMemoryWindowStore store2 = new InMemoryWindowStore("store", TimeSpan.FromDays(1), 1000 * 10);
            var dt = DateTime.Now;
            var valueSerdes = new ValueAndTimestampSerDes<string>(new StringSerDes());
            var bytes = new Bytes(Encoding.UTF8.GetBytes("test"));
            var provider = new MockStateProvider<string, string>(1000 * 10, new StringSerDes(), new StringSerDes(), store1, store2);
            var composite = new CompositeReadOnlyWindowStore<string, string>(provider, new WindowStoreType<string, string>(), "store");
            store1.Put(bytes, valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou1", dt.GetMilliseconds()), new SerializationContext()), dt.GetMilliseconds());
            var result = composite.Fetch("test", dt.AddSeconds(-2), dt.AddSeconds(2));
            Assert.IsNotNull(result);
            var items = result.ToList();
            Assert.AreEqual(1, items.Count);
            var c1 = items.FirstOrDefault(kp => kp.Value.Equals("coucou1"));
            Assert.IsNotNull(c1);
        }

        [Test]
        public void FetchAllTest()
        {
            InMemoryWindowStore store1 = new InMemoryWindowStore("store", TimeSpan.FromDays(1), 1000 * 10);
            InMemoryWindowStore store2 = new InMemoryWindowStore("store", TimeSpan.FromDays(1), 1000 * 10);
            var dt = DateTime.Now;
            var valueSerdes = new ValueAndTimestampSerDes<string>(new StringSerDes());
            var bytes = new Bytes(Encoding.UTF8.GetBytes("test"));
            var bytes2 = new Bytes(Encoding.UTF8.GetBytes("test2"));
            var provider = new MockStateProvider<string, string>(1000 * 10, new StringSerDes(), new StringSerDes(), store1, store2);
            var composite = new CompositeReadOnlyWindowStore<string, string>(provider, new WindowStoreType<string, string>(), "store");
            store1.Put(bytes, valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou1", dt.GetMilliseconds()), new SerializationContext()), dt.GetMilliseconds());
            store1.Put(bytes2, valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou2", dt.GetMilliseconds()), new SerializationContext()), dt.GetMilliseconds());
            var result = composite.FetchAll(dt.AddSeconds(-2), dt.AddSeconds(2));
            Assert.IsNotNull(result);
            var items = result.ToList();
            Assert.AreEqual(2, items.Count);
            var c1 = items.FirstOrDefault(kp => kp.Key.Key.Equals("test"));
            var c2 = items.FirstOrDefault(kp => kp.Key.Key.Equals("test2"));
            Assert.IsNotNull(c1);
            Assert.IsNotNull(c2);
            Assert.AreEqual("coucou1", c1.Value);
            Assert.AreEqual("coucou2", c2.Value);
        }

        [Test]
        public void AllTest()
        {
            InMemoryWindowStore store1 = new InMemoryWindowStore("store", TimeSpan.FromDays(1), 1000 * 10);
            InMemoryWindowStore store2 = new InMemoryWindowStore("store", TimeSpan.FromDays(1), 1000 * 10);
            var dt = DateTime.Now;
            var valueSerdes = new ValueAndTimestampSerDes<string>(new StringSerDes());
            var bytes = new Bytes(Encoding.UTF8.GetBytes("test"));
            var bytes2 = new Bytes(Encoding.UTF8.GetBytes("test2"));
            var provider = new MockStateProvider<string, string>(1000 * 10, new StringSerDes(), new StringSerDes(), store1, store2);
            var composite = new CompositeReadOnlyWindowStore<string, string>(provider, new WindowStoreType<string, string>(), "store");
            store1.Put(bytes, valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou1", dt.GetMilliseconds()), new SerializationContext()), dt.GetMilliseconds());
            store1.Put(bytes2, valueSerdes.Serialize(ValueAndTimestamp<string>.Make("coucou2", dt.GetMilliseconds()), new SerializationContext()), dt.GetMilliseconds());
            var result = composite.All();
            Assert.IsNotNull(result);
            var items = result.ToList();
            Assert.AreEqual(2, items.Count);
            var c1 = items.FirstOrDefault(kp => kp.Key.Key.Equals("test"));
            var c2 = items.FirstOrDefault(kp => kp.Key.Key.Equals("test2"));
            Assert.IsNotNull(c1);
            Assert.IsNotNull(c2);
            Assert.AreEqual("coucou1", c1.Value);
            Assert.AreEqual("coucou2", c2.Value);
        }
    }
}
