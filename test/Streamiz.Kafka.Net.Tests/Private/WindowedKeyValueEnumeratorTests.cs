using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.InMemory;
using System;
using System.Text;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class WindowedKeyValueEnumeratorTests
    {
        [Test]
        public void WindowedKeyValueEnumeratorWithSerdes()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("key"));
            var store = new InMemoryWindowStore("store", TimeSpan.FromSeconds(10), (long)TimeSpan.FromSeconds(1).TotalMilliseconds);
            store.Put(key, Encoding.UTF8.GetBytes("value"), date.GetMilliseconds());

            var enumerator = new WindowedKeyValueEnumerator<string, string>(
                store.All(), new StringSerDes(), new StringSerDes());
            var items = enumerator.ToList();
            Assert.AreEqual(1, items.Count);
            Assert.AreEqual("value", items[0].Value);
            Assert.AreEqual("key", items[0].Key.Key);
            Assert.AreEqual(TimeSpan.FromSeconds(1), items[0].Key.Window.TotalTime);
        }

        [Test]
        public void WindowedKeyValueEnumeratorTestNext()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("key"));
            var store = new InMemoryWindowStore("store", TimeSpan.FromSeconds(10), (long)TimeSpan.FromSeconds(1).TotalMilliseconds);
            store.Put(key, Encoding.UTF8.GetBytes("value"), date.GetMilliseconds());

            var enumerator = new WindowedKeyValueEnumerator<string, string>(
                store.All(), new StringSerDes(), new StringSerDes());
            int i = 0;
            while (enumerator.MoveNext())
            {
                Assert.AreEqual("key", enumerator.Current.Value.Key.Key);
                Assert.AreEqual("value", enumerator.Current.Value.Value);
                ++i;
            }
            Assert.AreEqual(1, i);
        }

        [Test]
        public void WindowedKeyValueEnumeratorTestReset()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("key"));
            var store = new InMemoryWindowStore("store", TimeSpan.FromSeconds(10), (long)TimeSpan.FromSeconds(1).TotalMilliseconds);
            store.Put(key, Encoding.UTF8.GetBytes("value"), date.GetMilliseconds());

            var enumerator = new WindowedKeyValueEnumerator<string, string>(
                store.All(), new StringSerDes(), new StringSerDes());
            int i = 0;
            while (enumerator.MoveNext())
            {
                Assert.AreEqual("key", enumerator.Current.Value.Key.Key);
                Assert.AreEqual("value", enumerator.Current.Value.Value);
                ++i;
            }
            Assert.AreEqual(1, i);
            enumerator.Reset();
            Assert.IsTrue(enumerator.MoveNext());
            Assert.AreEqual("key", enumerator.Current.Value.Key.Key);
            Assert.AreEqual("value", enumerator.Current.Value.Value);
        }

        [Test]
        public void WindowedKeyValueEnumeratorTestDispose()
        {
            var date = DateTime.Now;
            var key = new Bytes(Encoding.UTF8.GetBytes("key"));
            var store = new InMemoryWindowStore("store", TimeSpan.FromSeconds(10), (long)TimeSpan.FromSeconds(1).TotalMilliseconds);
            store.Put(key, Encoding.UTF8.GetBytes("value"), date.GetMilliseconds());

            var enumerator = new WindowedKeyValueEnumerator<string, string>(
                store.All(), new StringSerDes(), new StringSerDes());
            enumerator.Dispose();
            Assert.Throws<ObjectDisposedException>(() => enumerator.MoveNext());
        }
    }
}
