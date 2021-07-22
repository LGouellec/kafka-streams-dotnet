using System;
using System.Collections.Generic;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.InMemory;

namespace Streamiz.Kafka.Net.Tests.Stores
{
    public class InMemoryKeyValueStoreTests
    {
        [Test]
        public void CreateInMemoryKeyValueStore()
        {
            var store = new InMemoryKeyValueStore("store");
            Assert.IsFalse(store.Persistent);
            Assert.AreEqual("store", store.Name);
            Assert.AreEqual(0, store.ApproximateNumEntries());
        }

        [Test]
        public void PutKeyNotExist()
        {
            var serdes = new StringSerDes();
            byte[] key = serdes.Serialize("key", new SerializationContext()), value = serdes.Serialize("value", new SerializationContext());
            var store = new InMemoryKeyValueStore("store");
            store.Put(new Bytes(key), value);
            Assert.AreEqual(1, store.ApproximateNumEntries());
        }

        [Test]
        public void PutKeyExist()
        {
            var serdes = new StringSerDes();
            byte[] key = serdes.Serialize("key", new SerializationContext()),
                value = serdes.Serialize("value", new SerializationContext()),
                value2 = serdes.Serialize("value2", new SerializationContext());

            var store = new InMemoryKeyValueStore("store");
            store.Put(new Bytes(key), value);
            store.Put(new Bytes(key), value2);
            Assert.AreEqual(1, store.ApproximateNumEntries());
            var v = store.Get(new Bytes(key));
            Assert.AreEqual("value2", serdes.Deserialize(v, new SerializationContext()));
        }

        [Test]
        public void DeletKeyNotExist()
        {
            var serdes = new StringSerDes();
            byte[] key = serdes.Serialize("key", new SerializationContext());

            var store = new InMemoryKeyValueStore("store");
            var r = store.Delete(new Bytes(key));
            Assert.IsNull(r);
            Assert.AreEqual(0, store.ApproximateNumEntries());
        }

        [Test]
        public void DeleteKeyExist()
        {
            var serdes = new StringSerDes();
            byte[] key = serdes.Serialize("key", new SerializationContext()),
                value = serdes.Serialize("value", new SerializationContext());

            var store = new InMemoryKeyValueStore("store");
            store.Put(new Bytes(key), value);
            Assert.AreEqual(1, store.ApproximateNumEntries());
            var v = store.Delete(new Bytes(key));
            Assert.AreEqual(0, store.ApproximateNumEntries());
            Assert.AreEqual("value", serdes.Deserialize(v, new SerializationContext()));
        }

        [Test]
        public void PutAll()
        {
            var serdes = new StringSerDes();
            byte[] key = serdes.Serialize("key", new SerializationContext()), value = serdes.Serialize("value", new SerializationContext());
            byte[] key1 = serdes.Serialize("key1", new SerializationContext()), value1 = serdes.Serialize("value1", new SerializationContext());
            byte[] key2 = serdes.Serialize("key2", new SerializationContext()), value2 = serdes.Serialize("value2", new SerializationContext());
            byte[] key3 = serdes.Serialize("key3", new SerializationContext()), value3 = serdes.Serialize("value3", new SerializationContext());

            var store = new InMemoryKeyValueStore("store");

            var items = new List<KeyValuePair<Bytes, byte[]>>();
            items.Add(KeyValuePair.Create(new Bytes(key), value));
            items.Add(KeyValuePair.Create(new Bytes(key1), value1));
            items.Add(KeyValuePair.Create(new Bytes(key2), value2));
            items.Add(KeyValuePair.Create(new Bytes(key3), value3));

            store.PutAll(items);

            Assert.AreEqual(4, store.ApproximateNumEntries());
        }

        [Test]
        public void PutIfAbsent()
        {
            var serdes = new StringSerDes();
            byte[] key3 = serdes.Serialize("key3", new SerializationContext()), value3 = serdes.Serialize("value3", new SerializationContext());

            var store = new InMemoryKeyValueStore("store");

            store.PutIfAbsent(new Bytes(key3), value3);
            store.PutIfAbsent(new Bytes(key3), value3);

            Assert.AreEqual(1, store.ApproximateNumEntries());
        }

        [Test]
        public void EmptyEnumerator()
        {
            var store = new InMemoryKeyValueStore("store");

            var enumerator = store.All().GetEnumerator();
            Assert.IsFalse(enumerator.MoveNext());
            enumerator.Dispose();
        }

        [Test]
        public void EnumeratorReset()
        {
            var store = new InMemoryKeyValueStore("store");

            var serdes = new StringSerDes();
            byte[] key = serdes.Serialize("key", new SerializationContext()), value = serdes.Serialize("value", new SerializationContext());

            store.Put(new Bytes(key), value);

            var enumerator = store.All().GetEnumerator();
            Assert.IsTrue(enumerator.MoveNext());
            Assert.IsFalse(enumerator.MoveNext());
            Assert.Throws<NotSupportedException>(() => enumerator.Reset());
        }

        [Test]
        public void EnumeratorReverseAll()
        {
            var store = new InMemoryKeyValueStore("store");

            var serdes = new StringSerDes();

            string deserialize(byte[] bytes)
            {
                return serdes.Deserialize(bytes, new SerializationContext());
            }

            byte[] key = serdes.Serialize("key", new SerializationContext()), value = serdes.Serialize("value", new SerializationContext());
            byte[] key2 = serdes.Serialize("key2", new SerializationContext()), value2 = serdes.Serialize("value2", new SerializationContext());

            store.Put(new Bytes(key), value);
            store.Put(new Bytes(key2), value2);

            var enumerator = store.ReverseAll().GetEnumerator();
            Assert.IsTrue(enumerator.MoveNext());
            Assert.AreEqual("key2", deserialize(enumerator.Current.Key.Get));
            Assert.AreEqual("value2", deserialize(enumerator.Current.Value));
            Assert.IsTrue(enumerator.MoveNext());
            Assert.AreEqual("key", deserialize(enumerator.Current.Key.Get));
            Assert.AreEqual("value", deserialize(enumerator.Current.Value));
            Assert.IsFalse(enumerator.MoveNext());
            enumerator.Dispose();
        }

        [Test]
        public void EnumeratorRangeAll()
        {
            var store = new InMemoryKeyValueStore("store");

            var serdes = new StringSerDes();

            string deserialize(byte[] bytes)
            {
                return serdes.Deserialize(bytes, new SerializationContext());
            }

            byte[] key = serdes.Serialize("key", new SerializationContext()), value = serdes.Serialize("value", new SerializationContext());
            byte[] key2 = serdes.Serialize("key2", new SerializationContext()), value2 = serdes.Serialize("value2", new SerializationContext());
            byte[] key3 = serdes.Serialize("key3", new SerializationContext()), value3 = serdes.Serialize("value3", new SerializationContext());

            store.Put(new Bytes(key), value);
            store.Put(new Bytes(key2), value2);
            store.Put(new Bytes(key3), value3);

            var enumerator = store.Range(new Bytes(key), new Bytes(key2));
            Assert.IsTrue(enumerator.MoveNext());
            Assert.AreEqual("key", deserialize(enumerator.Current.Value.Key.Get));
            Assert.AreEqual("value", deserialize(enumerator.Current.Value.Value));
            Assert.IsTrue(enumerator.MoveNext());
            Assert.AreEqual("key2", deserialize(enumerator.Current.Value.Key.Get));
            Assert.AreEqual("value2", deserialize(enumerator.Current.Value.Value));
            Assert.IsFalse(enumerator.MoveNext());
            enumerator.Dispose();
        }

        [Test]
        public void EnumeratorIncorrectRange()
        {
            var store = new InMemoryKeyValueStore("store");

            var serdes = new StringSerDes();

            string deserialize(byte[] bytes)
            {
                return serdes.Deserialize(bytes, new SerializationContext());
            }

            byte[] key = serdes.Serialize("key", new SerializationContext()), value = serdes.Serialize("value", new SerializationContext());
            byte[] key2 = serdes.Serialize("key2", new SerializationContext()), value2 = serdes.Serialize("value2", new SerializationContext());
            byte[] key3 = serdes.Serialize("key3", new SerializationContext()), value3 = serdes.Serialize("value3", new SerializationContext());

            store.Put(new Bytes(key), value);
            store.Put(new Bytes(key2), value2);
            store.Put(new Bytes(key3), value3);

            var enumerator = store.Range(new Bytes(key2), new Bytes(key));
            Assert.IsFalse(enumerator.MoveNext());
            enumerator.Dispose();
        }

        [Test]
        public void EnumeratorReverseRangeAll()
        {
            var store = new InMemoryKeyValueStore("store");

            var serdes = new StringSerDes();

            string deserialize(byte[] bytes)
            {
                return serdes.Deserialize(bytes, new SerializationContext());
            }

            byte[] key = serdes.Serialize("key", new SerializationContext()), value = serdes.Serialize("value", new SerializationContext());
            byte[] key2 = serdes.Serialize("key2", new SerializationContext()), value2 = serdes.Serialize("value2", new SerializationContext());
            byte[] key3 = serdes.Serialize("key3", new SerializationContext()), value3 = serdes.Serialize("value3", new SerializationContext());

            store.Put(new Bytes(key), value);
            store.Put(new Bytes(key2), value2);
            store.Put(new Bytes(key3), value3);

            var enumerator = store.ReverseRange(new Bytes(key), new Bytes(key2));
            Assert.IsTrue(enumerator.MoveNext());
            Assert.AreEqual("key2", deserialize(enumerator.Current.Value.Key.Get));
            Assert.AreEqual("value2", deserialize(enumerator.Current.Value.Value));
            Assert.IsTrue(enumerator.MoveNext());
            Assert.AreEqual("key", deserialize(enumerator.Current.Value.Key.Get));
            Assert.AreEqual("value", deserialize(enumerator.Current.Value.Value));
            Assert.IsFalse(enumerator.MoveNext());
            enumerator.Dispose();
        }

    }
}
