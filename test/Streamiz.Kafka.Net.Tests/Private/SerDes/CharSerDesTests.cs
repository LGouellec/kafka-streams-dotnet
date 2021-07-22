using NUnit.Framework;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Tests.Private.SerDes
{
    public class CharSerDesTests
    {
        [Test]
        public void SerializeData()
        {
            char i = 'b';
            byte[] b = new byte[] { 98, 0};
            var serdes = new CharSerDes();
            var r = serdes.Serialize(i, new Confluent.Kafka.SerializationContext());
            Assert.IsNotNull(r);
            Assert.AreEqual(b, r);
        }

        [Test]
        public void DeserializeData()
        {
            char i = 'p';
            var serdes = new CharSerDes();
            var r = serdes.Deserialize(serdes.Serialize(i, new Confluent.Kafka.SerializationContext()), new Confluent.Kafka.SerializationContext());
            Assert.IsNotNull(r);
            Assert.AreEqual(i, r);
        }
    }
}
