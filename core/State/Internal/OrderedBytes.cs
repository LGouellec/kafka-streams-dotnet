using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class OrderedBytes
    {
        private const int MIN_KEY_LENGTH = 1;

        internal static Bytes UpperRange(Bytes key,  byte[] maxSuffix)
        {
             byte[] bytes = key.Get;
             ByteBuffer rangeEnd = ByteBuffer.Build(bytes.Length + maxSuffix.Length);

            int i = 0;
            while (i < bytes.Length && (
                i < MIN_KEY_LENGTH // assumes keys are at least one byte long
                || (bytes[i] & 0xFF) >= (maxSuffix[0] & 0xFF)
                ))
            {
                rangeEnd.Put(bytes[i++]);
            }

            rangeEnd.Put(maxSuffix);

            byte[] res = rangeEnd.ToArray();
            return Bytes.Wrap(res);
        }

        internal static Bytes LowerRange( Bytes key,  byte[] minSuffix)
        {
             byte[] bytes = key.Get;
             ByteBuffer rangeStart = ByteBuffer.Build(bytes.Length + minSuffix.Length);
            // any key in the range would start at least with the given prefix to be
            // in the range, and have at least SUFFIX_SIZE number of trailing zero bytes.

            // unless there is a maximum key length, you can keep appending more zero bytes
            // to keyFrom to create a key that will match the range, yet that would precede
            // KeySchema.toBinaryKey(keyFrom, from, 0) in byte order
            rangeStart.Put(bytes);
            rangeStart.Put(minSuffix);

            return Bytes.Wrap(rangeStart.ToArray());
        }
    }
}
