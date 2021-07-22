using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using System;
using System.Diagnostics.CodeAnalysis;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class PriorityQueueTests
    {
        public class Item : IComparable<Item>
        {
            public int Id { get; set; }
            public string Value { get; set; }

            public int CompareTo([AllowNull] Item other)
                => Id.CompareTo(other.Id);
        }

        [Test]
        public void CreatePriorityQueue()
        {
            var queue = new PriorityQueue<Item>(10);
            Assert.IsNotNull(queue);
            Assert.AreEqual(0, queue.Count);
        }

        [Test]
        public void EnqueueInOrderTest()
        {
            var item = new Item { Id = 0, Value = "value0" };
            var item1 = new Item { Id = 1, Value = "value1" };
            var item2 = new Item { Id = 2, Value = "value2" };

            var queue = new PriorityQueue<Item>(10);
            queue.Enqueue(item);
            queue.Enqueue(item1);
            queue.Enqueue(item2);
            Assert.AreEqual(3, queue.Count);
        }

        [Test]
        public void PeekTest()
        {
            var item = new Item { Id = 0, Value = "value0" };
            var item1 = new Item { Id = 1, Value = "value1" };
            var item2 = new Item { Id = 2, Value = "value2" };

            var queue = new PriorityQueue<Item>(10);
            queue.Enqueue(item);
            queue.Enqueue(item1);
            queue.Enqueue(item2);
            var i = queue.Peek();
            Assert.AreEqual("value0", i.Value);
            Assert.AreEqual(0, i.Id);
        }

        [Test]
        public void EnqueueInOrderAndDequeueTest()
        {
            var item = new Item { Id = 0, Value = "value0" };
            var item1 = new Item { Id = 1, Value = "value1" };
            var item2 = new Item { Id = 2, Value = "value2" };

            var queue = new PriorityQueue<Item>(10);
            queue.Enqueue(item);
            queue.Enqueue(item1);
            queue.Enqueue(item2);
            var i = queue.Dequeue();
            Assert.AreEqual("value0", i.Value);
            Assert.AreEqual(0, i.Id);
            Assert.AreEqual(2, queue.Count);
        }

        [Test]
        public void EnqueueDifferentOrderTest()
        {
            var item = new Item { Id = 3, Value = "value0" };
            var item1 = new Item { Id = 0, Value = "value1" };
            var item2 = new Item { Id = 1, Value = "value2" };

            var queue = new PriorityQueue<Item>(10);
            queue.Enqueue(item);
            queue.Enqueue(item1);
            queue.Enqueue(item2);
            var i = queue.Dequeue();
            Assert.AreEqual("value1", i.Value);
            Assert.AreEqual(0, i.Id);
            Assert.AreEqual(2, queue.Count);
            i = queue.Dequeue();
            Assert.AreEqual("value2", i.Value);
            Assert.AreEqual(1, i.Id);
            Assert.AreEqual(1, queue.Count);
            i = queue.Dequeue();
            Assert.AreEqual("value0", i.Value);
            Assert.AreEqual(3, i.Id);
            Assert.AreEqual(0, queue.Count);
            i = queue.Dequeue();
            Assert.IsNull(i);
        }
    }
}
