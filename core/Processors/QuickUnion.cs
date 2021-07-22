using Streamiz.Kafka.Net.Crosscutting;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Streamiz.Kafka.Net.Processors
{
    internal class QuickUnion<T>
    {
        private readonly IDictionary<T, T> ids = new Dictionary<T, T>();

        public IReadOnlyDictionary<T, T> Ids => new ReadOnlyDictionary<T, T>(ids);

        internal void Add(T id)
        {
            ids.Add(id, id);
        }

        internal bool Exists(T id)
        {
            return ids.ContainsKey(id);
        }

        internal T Root(T id)
        {
            T current = id;
            T parent = ids[current];

            if (parent == null)
            {
                throw new ArgumentException($"Element with id: {id.ToString()} not found");
            }

            while (!parent.Equals(current))
            {
                // do the path splitting
                T grandparent = ids[parent];
                ids.AddOrUpdate(current, grandparent);

                current = parent;
                parent = grandparent;
            }
            return current;
        }

        internal void Unite( T id1,  params T[] idList)
        {
            foreach (T id2 in idList)
            {
                UnitePair(id1, id2);
            }
        }

        private void UnitePair( T id1,  T id2)
        {
             T root1 = Root(id1);
             T root2 = Root(id2);

            if (!root1.Equals(root2))
            {
                ids.AddOrUpdate(root1, root2);
            }
        }

    }
}
