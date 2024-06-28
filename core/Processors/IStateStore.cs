using System;

namespace Streamiz.Kafka.Net.Processors
{
    /// <summary>
    /// A storage engine for managing state maintained by a stream processor.
    /// </summary>
    public interface IStateStore
    {
        /// <summary>
        /// Name of this store
        /// </summary>
        String Name { get; }

        /// <summary>
        /// Return if the storage is persistent or not.
        /// </summary>
        bool Persistent { get; }
        
        /// <summary>
        /// Return if the storage is present locally or not.
        /// If the store is local, an internal kafka topic will be created to offload the data
        /// </summary>
        bool IsLocally { get; }

        /// <summary>
        /// Is this store open for reading and writing
        /// </summary>
        bool IsOpen { get; }

        /// <summary>
        /// Initializes this state store.
        /// The implementation of this function must register the root store in the context via the
        /// <see cref="ProcessorContext.Register(IStateStore, Internal.StateRestoreCallback)"/> function, where the
        /// first <see cref="IStateStore"/> parameter should always be the passed-in <code>root</code> object, and
        /// the second parameter should be an object of user's implementation
        /// of the <see cref="Internal.StateRestoreCallback"/> interface used for restoring the state store from the changelog.
        /// </summary>
        /// <param name="context">Processor context</param>
        /// <param name="root">Root state (always itself)</param>
        void Init(ProcessorContext context, IStateStore root);

        /// <summary>
        /// Flush any cached data
        /// </summary>
        void Flush();

        /// <summary>
        /// Close the storage engine.
        /// Note that this function needs to be idempotent since it may be called
        /// several times on the same state store
        /// Users only need to implement this function but should NEVER need to call this api explicitly
        /// as it will be called by the library automatically when necessary
        /// </summary>
        void Close();
    }
}
