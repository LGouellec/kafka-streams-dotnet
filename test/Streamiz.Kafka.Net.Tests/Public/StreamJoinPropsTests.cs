using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Supplier;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Tests.Public
{
    public class TestSupplier : IWindowBytesStoreSupplier
    {
        public long? WindowSize { get; set; }

        public long Retention => 100;

        public string Name => "TEST";

        public IWindowStore<Bytes, byte[]> Get()
            => null;
    }

    public class StreamJoinPropsTests
    {
        [Test]
        public void TestJoinPropsSupplier()
        {
            var props = StreamJoinProps.With(new TestSupplier(), new TestSupplier());
            Assert.IsNotNull(props);
            Assert.IsAssignableFrom<TestSupplier>(props.LeftStoreSupplier);
            Assert.IsAssignableFrom<TestSupplier>(props.RightStoreSupplier);
            Assert.IsNull(props.Name);
            Assert.IsNull(props.StoreName);
        }

        [Test]
        public void TestJoinPropsSupplier2()
        {
            var props = StreamJoinProps.With<string, string, string>(new TestSupplier(), new TestSupplier());
            Assert.IsNotNull(props);
            Assert.IsAssignableFrom<TestSupplier>(props.LeftStoreSupplier);
            Assert.IsAssignableFrom<TestSupplier>(props.RightStoreSupplier);
            Assert.IsNull(props.Name);
            Assert.IsNull(props.StoreName);
            Assert.IsNull(props.KeySerdes);
            Assert.IsNull(props.LeftValueSerdes);
            Assert.IsNull(props.RightValueSerdes);
        }

        [Test]
        public void TestJoinPropsName()
        {
            var props = StreamJoinProps.As("store1");
            Assert.IsNotNull(props);
            Assert.AreEqual("store1", props.StoreName);
            Assert.IsNull(props.Name);
        }

        [Test]
        public void TestJoinPropsName2()
        {
            var props = StreamJoinProps.As<string, string, string>("store1");
            Assert.IsNotNull(props);
            Assert.AreEqual("store1", props.StoreName);
            Assert.IsNull(props.Name);
            Assert.IsNull(props.KeySerdes);
            Assert.IsNull(props.LeftValueSerdes);
            Assert.IsNull(props.RightValueSerdes);
        }

        [Test]
        public void TestJoinPropsNames()
        {
            var props = StreamJoinProps.As("name1", "store1");
            Assert.IsNotNull(props);
            Assert.AreEqual("store1", props.StoreName);
            Assert.AreEqual("name1", props.Name);
        }

        [Test]
        public void TestJoinPropsNames2()
        {
            var props = StreamJoinProps.As<string, string, string>("name1", "store1");
            Assert.IsNotNull(props);
            Assert.AreEqual("store1", props.StoreName);
            Assert.AreEqual("name1", props.Name);
            Assert.IsNull(props.KeySerdes);
            Assert.IsNull(props.LeftValueSerdes);
            Assert.IsNull(props.RightValueSerdes);
        }

        [Test]
        public void TestJoinPropsFrom()
        {
            var p = StreamJoinProps.With(new TestSupplier(), new TestSupplier());
            var props = StreamJoinProps.From<string, string, string>(p);
            Assert.IsNotNull(props);
            Assert.IsAssignableFrom<TestSupplier>(props.LeftStoreSupplier);
            Assert.IsAssignableFrom<TestSupplier>(props.RightStoreSupplier);
            Assert.IsNull(props.Name);
            Assert.IsNull(props.StoreName);
            Assert.IsNull(props.KeySerdes);
            Assert.IsNull(props.LeftValueSerdes);
            Assert.IsNull(props.RightValueSerdes);
        }

        [Test]
        public void TestJoinPropsWithSerdes()
        {
            var props = StreamJoinProps.With<string, int, long>(new StringSerDes(), new Int32SerDes(), new Int64SerDes());
            Assert.IsNotNull(props);
            Assert.IsNull(props.LeftStoreSupplier);
            Assert.IsNull(props.RightStoreSupplier);
            Assert.IsNull(props.Name);
            Assert.IsNull(props.StoreName);
            Assert.IsAssignableFrom<StringSerDes>(props.KeySerdes);
            Assert.IsAssignableFrom<Int32SerDes>(props.LeftValueSerdes);
            Assert.IsAssignableFrom<Int64SerDes>(props.RightValueSerdes);
        }
    }
}
