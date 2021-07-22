using Confluent.Kafka;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Kafka
{
    /// <summary>
    /// <see cref="IKafkaSupplier"/> can be used to provide custom Kafka clients to a <see cref="KafkaStream"/> instance.
    /// </summary>
    public interface IKafkaSupplier
    {
        /// <summary>
        /// Build a kafka consumer with <see cref="ConsumerConfig"/> instance and <see cref="IConsumerRebalanceListener"/> listener.
        /// </summary>
        /// <param name="config">Consumer configuration can't be null</param>
        /// <param name="rebalanceListener">Rebalance listener (Nullable)</param>
        /// <returns>Return a kafka consumer built</returns>
        IConsumer<byte[], byte[]> GetConsumer(ConsumerConfig config, IConsumerRebalanceListener rebalanceListener);

        /// <summary>
        /// Build a kafka producer with <see cref="ProducerConfig"/> instance.
        /// </summary>
        /// <param name="config">Producer configuration can't be null</param>
        /// <returns>Return a kafka producer built</returns>
        IProducer<byte[], byte[]> GetProducer(ProducerConfig config);

        /// <summary>
        /// Build a kafka restore consumer with <see cref="ConsumerConfig"/> instance for read record to restore statestore.
        /// </summary>
        /// <param name="config">Restore consumer configuration can't be null</param>
        /// <returns>Return a kafka restore consumer built</returns>
        IConsumer<byte[], byte[]> GetRestoreConsumer(ConsumerConfig config);

        /// <summary>
        /// Create an admin kafka client which is used for internal topic management.
        /// </summary>
        /// <param name="config">Admin configuration can't be null</param>
        /// <returns>Return an admin client instance</returns>
        IAdminClient GetAdmin(AdminClientConfig config);

        /// <summary>
        /// Build a kafka global consumer with <see cref="ConsumerConfig"/> which is used to consume records for <see cref="IGlobalKTable{K, V}"/>.
        /// </summary>
        /// <param name="config">Global consumer configuration can't be null</param>
        /// <returns>Return a kafka global consumer built</returns>
        IConsumer<byte[], byte[]> GetGlobalConsumer(ConsumerConfig config);
    }
}
