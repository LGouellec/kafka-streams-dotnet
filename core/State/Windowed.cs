using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System.Text;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// The result key type of a windowed stream aggregation.
    /// If a <see cref="IKStream{K, V}"/> gets grouped and aggregated using a window-aggregation the resulting <see cref="IKTable{K, V}"/>is a
    /// so-called "windowed <see cref="IKTable{K, V}"/>" with a combined key type that encodes the corresponding aggregation window and
    /// the original record key.
    /// Thus, a windowed <see cref="IKTable{K, V}"/> has type <code><see cref="Windowed{K}"/>, V</code>.
    /// </summary>
    /// <typeparam name="K">type of key</typeparam>
    public class Windowed<K>
    {
        /// <summary>
        /// Constructor with Key and Window data
        /// </summary>
        /// <param name="key">Key value</param>
        /// <param name="window">Window value</param>
        public Windowed(K key, Window window)
        {
            Key = key;
            Window = window;
        }

        /// <summary>
        /// Return the key of the window.
        /// </summary>
        public K Key { get; }

        /// <summary>
        /// Return the window containing the values associated with this key.
        /// </summary>
        public Window Window { get; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
            => obj is Windowed<K>
                && Key.Equals(((Windowed<K>)obj).Key)
                && Window.Equals(((Windowed<K>)obj).Window);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
            => Window.GetHashCode() << 16 | Key.GetHashCode();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"Key : {Key}");
            sb.AppendLine($"Window : {Window}");
            return sb.ToString();
        }
    }
}
