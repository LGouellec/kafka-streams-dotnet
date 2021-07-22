using Streamiz.Kafka.Net.Table;
using System;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// The <see cref="IValueJoiner{V1, V2, VR}"/> interface for joining two values into a new value of arbitrary type.
    /// This is a stateless operation, i.e, <see cref="IValueJoiner{V1, V2, VR}.Apply(V1, V2)"/> is invoked individually for each joining
    /// record-pair of a <see cref="IKStream{K, V1}"/>-<see cref="IKStream{K, V2}"/>, <see cref="IKStream{K, V1}"/>-<see cref="IKTable{K, V2}"/>, or <see cref="IKTable{K, V1}"/>-<see cref="IKTable{K, V2}"/>
    /// join.
    /// </summary>
    /// <typeparam name="V1">first value type</typeparam>
    /// <typeparam name="V2">second value type</typeparam>
    /// <typeparam name="VR">joined value type</typeparam>
    public interface IValueJoiner<in V1, in V2, out VR>
    {
        /// <summary>
        /// Return a joined value consisting of {@code value1} and {@code value2}.
        /// </summary>
        /// <param name="value1">the first value for joining</param>
        /// <param name="value2">the second value for joining</param>
        /// <returns>the joined value</returns>
        VR Apply(V1 value1, V2 value2);
    }

    internal class WrappedValueJoiner<V1, V2, VR> : IValueJoiner<V1, V2, VR>
    {
        private readonly Func<V1, V2, VR> function;

        public WrappedValueJoiner(Func<V1, V2,VR> function)
        {
            this.function = function;
        }

        public VR Apply(V1 value1, V2 value2) => function.Invoke(value1, value2);
    }
}
