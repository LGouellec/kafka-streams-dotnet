using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// NOT IMPLEMENTED FOR MOMENT
    /// </summary>
    /// <typeparam name="K"></typeparam>
    /// <typeparam name="AGG"></typeparam>
    public interface ISessionStore<K,AGG> : IStateStore, IReadOnlySessionStore<K,AGG>
    {
    }
}
