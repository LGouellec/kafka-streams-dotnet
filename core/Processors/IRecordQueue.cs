namespace Streamiz.Kafka.Net.Processors
{
    /// <summary>
    /// RecordQueue is a FIFO queue of (ConsumerRecord + timestamp). It also keeps track of the
    /// partition timestamp defined as the largest timestamp seen on the partition so far; this is passed to the
    /// timestamp extractor.
    /// </summary>
    /// <typeparam name="T">The type of elements in the queue.</typeparam>
    public interface IRecordQueue<T>
    {
        /// <summary>
        /// queue one element
        /// </summary>
        /// <param name="item">item</param>
        /// <returns>return new size of queue</returns>
        int Queue(T item);

        /// <summary>
        /// Get the first element in the queue
        /// </summary>
        /// <returns>return the first element in the queue</returns>
        T Poll();

        /// <summary>
        /// Actual size of the queue
        /// </summary>
        int Size { get; }

        /// <summary>
        /// Get if the queue is empty or not
        /// </summary>
        bool IsEmpty { get; }

        /// <summary>
        /// Clear the queue
        /// </summary>
        void Clear();
    }
}
