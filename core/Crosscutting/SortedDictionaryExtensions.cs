using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Crosscutting
{
    internal static class SortedDictionaryExtensions
    {
        internal static IEnumerable<KeyValuePair<K, V>> HeadMap<K, V>(this SortedDictionary<K, V> sortedDic,  K key, bool inclusive)
        {
            foreach (K k in sortedDic.Keys) {
                int r = sortedDic.Comparer.Compare(key, k);
                if ((inclusive && r >= 0) || (!inclusive && r > 0))
                    yield return KeyValuePair.Create(k, sortedDic[k]);
                else
                    break;
            }
        }

        internal static IEnumerable<KeyValuePair<K, V>> SubMap<K, V>(this SortedDictionary<K, V> sortedDic, K keyFrom, K keyTo , bool inclusiveFrom, bool inclusiveTo)
        {
            foreach (K k in sortedDic.Keys)
            {
                int rF = sortedDic.Comparer.Compare(keyFrom, k);
                int rT = sortedDic.Comparer.Compare(keyTo, k);

                if((inclusiveFrom && rF <= 0) || (!inclusiveFrom && rF < 0))
                {
                    if ((inclusiveTo && rT >= 0) || (!inclusiveTo && rT > 0))
                    {
                        yield return KeyValuePair.Create(k, sortedDic[k]);
                    }
                    else
                        break;
                }
            }
        }
    }
}
