using NUnit.Framework;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Tests.Private.SerDes
{
    public class Int32SerDesTests
    {
        [Test]
        public void SerializeData()
        {
            int i = 100;
            byte[] b = new byte[] { 100, 0, 0, 0 };
            var serdes = new Int32SerDes();
            var r = serdes.Serialize(i, new Confluent.Kafka.SerializationContext());
            Assert.IsNotNull(r);
            Assert.AreEqual(b, r);
        }

        [Test]
        public void DeserializeData()
        {
            int i = 300;
            var serdes = new Int32SerDes();
            var r = serdes.Deserialize(serdes.Serialize(i, new Confluent.Kafka.SerializationContext()), new Confluent.Kafka.SerializationContext());
            Assert.IsNotNull(r);
            Assert.AreEqual(i, r);
        }
    }
}
