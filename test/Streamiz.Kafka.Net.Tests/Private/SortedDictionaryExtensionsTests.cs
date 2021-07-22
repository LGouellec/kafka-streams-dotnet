using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class SortedDictionaryExtensionsTests
    {
        private SortedDictionary<long, string> dico = null;

        [SetUp]
        public void Init()
        {
            dico = new SortedDictionary<long, string>(new LongComparer());
        }

        [TearDown]
        public void Close()
        {
            dico.Clear();
            dico = null;
        }

        [Test]
        public void HeadMapInclusive()
        {
            dico.Add(1, "test1");
            dico.Add(3, "test3");
            dico.Add(2, "test2");
            dico.Add(4, "test4");

            var values = dico.HeadMap(3, true).ToList();
            Assert.AreEqual(3, values.Count);
            Assert.AreEqual(1, values[0].Key);
            Assert.AreEqual(2, values[1].Key);
            Assert.AreEqual(3, values[2].Key);   
        }

        [Test]
        public void HeadMapEmpty()
        {
            dico.Add(4, "test4");

            var values = dico.HeadMap(2, true).ToList();
            Assert.AreEqual(0, values.Count);
        }

        [Test]
        public void HeadMapNotInclusive()
        {
            dico.Add(1, "test1");
            dico.Add(3, "test3");
            dico.Add(2, "test2");
            dico.Add(4, "test4");

            var values = dico.HeadMap(3, false).ToList();
            Assert.AreEqual(2, values.Count);
            Assert.AreEqual(1, values[0].Key);
            Assert.AreEqual(2, values[1].Key);
        }


        [Test]
        public void SubMap()
        {
            dico.Add(1, "test1");
            dico.Add(3, "test3");
            dico.Add(2, "test2");
            dico.Add(4, "test4");

            var values = dico.SubMap(1, 4, false, false).ToList();
            Assert.AreEqual(2, values.Count);
            Assert.AreEqual(2, values[0].Key);
            Assert.AreEqual(3, values[1].Key);
        }

        [Test]
        public void SubMapInclusiveFrom()
        {
            dico.Add(1, "test1");
            dico.Add(3, "test3");
            dico.Add(2, "test2");
            dico.Add(4, "test4");

            var values = dico.SubMap(1, 4, true, false).ToList();
            Assert.AreEqual(3, values.Count);
            Assert.AreEqual(1, values[0].Key);
            Assert.AreEqual(2, values[1].Key);
            Assert.AreEqual(3, values[2].Key);
        }

        [Test]
        public void SubMapInclusiveTo()
        {
            dico.Add(1, "test1");
            dico.Add(3, "test3");
            dico.Add(2, "test2");
            dico.Add(4, "test4");

            var values = dico.SubMap(1, 4, false, true).ToList();
            Assert.AreEqual(3, values.Count);
            Assert.AreEqual(2, values[0].Key);
            Assert.AreEqual(3, values[1].Key);
            Assert.AreEqual(4, values[2].Key);
        }

        [Test]
        public void SubMapInclusiveBoth()
        {
            dico.Add(1, "test1");
            dico.Add(3, "test3");
            dico.Add(2, "test2");
            dico.Add(4, "test4");

            var values = dico.SubMap(1, 4, true, true).ToList();
            Assert.AreEqual(4, values.Count);
            Assert.AreEqual(1, values[0].Key);
            Assert.AreEqual(2, values[1].Key);
            Assert.AreEqual(3, values[2].Key);
            Assert.AreEqual(4, values[3].Key);
        }

        [Test]
        public void SubMapEmpty()
        {
            dico.Add(1, "test1");
            dico.Add(3, "test3");
            dico.Add(2, "test2");
            dico.Add(4, "test4");

            var values = dico.SubMap(10, 40, true, true).ToList();
            Assert.AreEqual(0, values.Count);
        }
    }
}
