using NUnit.Framework;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Tests.Private.SerDes
{
    public class StringSerDesTests
    {
        [Test]
        public void SerializeNullData()
        {
            var serdes = new StringSerDes();
            var r = serdes.Serialize(null, new Confluent.Kafka.SerializationContext());
            Assert.IsNull(r);
        }

        [Test]
        public void SerializeData()
        {
            string s = "coucou";
            byte[] b = new byte[] { 99, 111, 117, 99, 111, 117 };

            var serdes = new StringSerDes();
            var r = serdes.Serialize(s, new Confluent.Kafka.SerializationContext());
            Assert.IsNotNull(r);
            Assert.Greater(r.Length, 0);
            Assert.AreEqual(b, r);
        }


        [Test]
        public void DeserializeNullData()
        {
            var serdes = new StringSerDes();
            var r = serdes.Deserialize(null, new Confluent.Kafka.SerializationContext());
            Assert.IsNull(r);
        }

        [Test]
        public void DeserializeData()
        {
            string s = "test";

            var serdes = new StringSerDes();
            var r = serdes.Deserialize(serdes.Serialize(s, new Confluent.Kafka.SerializationContext()), new Confluent.Kafka.SerializationContext());
            Assert.IsNotNull(r);
            Assert.AreEqual(s, r);
        }

    }
}
