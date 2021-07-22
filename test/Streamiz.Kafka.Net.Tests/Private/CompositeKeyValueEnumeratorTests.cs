using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.InMemory.Internal;
using System.Collections.Generic;


namespace Streamiz.Kafka.Net.Tests.Private
{
    public class CompositeKeyValueEnumeratorTests
    {
        private CompositeKeyValueEnumerator<Bytes, byte[]> compositeEnumerator = null;
        private readonly StringSerDes serdes = new StringSerDes();

        string Deserialize(byte[] data) => serdes.Deserialize(data, new SerializationContext());
        byte[] Serialize(string data) => serdes.Serialize(data, new SerializationContext());

        KeyValuePair<Bytes, byte[]> Create(string k, string v)
            => KeyValuePair.Create(Bytes.Wrap(Serialize(k)), Serialize(v));

        KeyValuePair<string, string> Create(Bytes k, byte[] v)
            => KeyValuePair.Create(Deserialize(k.Get), Deserialize(v));


        [SetUp]
        public void Init()
        {
            CreateCompositeKeyValueEnumerator();
        }

        private void CreateCompositeKeyValueEnumerator(bool forward = true)
        {
            var list1 = new List<KeyValuePair<Bytes, byte[]>>
            {
                Create("key1", "value1"),
                Create("key2", "value2"),
            };
            var list2 = new List<KeyValuePair<Bytes, byte[]>>
            {
                Create("key3", "value3"),
                Create("key4", "value4"),
            };
            InMemoryKeyValueEnumerator enumerator1 = new InMemoryKeyValueEnumerator(list1, forward);
            InMemoryKeyValueEnumerator enumerator2 = new InMemoryKeyValueEnumerator(list2, forward);
            compositeEnumerator
                = new CompositeKeyValueEnumerator<Bytes, byte[]>(
                    new List<IKeyValueEnumerator<Bytes, byte[]>> { enumerator1, enumerator2 });

        }

        [TearDown]
        public void Dispose()
        {
            compositeEnumerator?.Dispose();
        }

        [Test]
        public void GetAllElements()
        {
            Assert.IsNotNull(compositeEnumerator);
            var list = compositeEnumerator.ToList();
            Assert.AreEqual(4, list.Count);
        }

        [Test]
        public void MoveNext()
        {
            Assert.IsNotNull(compositeEnumerator);
            int i = 0;
            int count = 2, j = 1;
            while (j < count)
            {
                i = 0;
                while (compositeEnumerator.MoveNext())
                {
                    ++i;
                    Assert.IsTrue(compositeEnumerator.Current.HasValue);
                    Assert.AreEqual($"key{i}", Deserialize(compositeEnumerator.Current.Value.Key.Get));
                    Assert.AreEqual($"key{i}", Deserialize(compositeEnumerator.PeekNextKey().Get));
                    Assert.AreEqual($"value{i}", Deserialize(compositeEnumerator.Current.Value.Value));
                }
                Assert.AreEqual(4, i);
                compositeEnumerator.Reset();
                ++j;
            }            
        }
    }
}
