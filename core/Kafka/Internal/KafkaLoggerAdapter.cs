using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using System;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.Kafka.Internal
{
    internal class KafkaLoggerAdapter
    {
        private readonly ILogger log;

        public KafkaLoggerAdapter(IStreamConfig configuration)
            : this(configuration, Logger.GetLogger(typeof(KafkaLoggerAdapter)))
        {
        }

        public KafkaLoggerAdapter(IStreamConfig configuration, ILogger log)
        {
            this.log = log;
        }

        #region Log Consumer

        internal void LogConsume(IConsumer<byte[], byte[]> consumer, LogMessage message)
        {
            string logPrefix = Thread.CurrentThread.Name != null ? $"stream-thread[{Thread.CurrentThread.Name}] " : "";
            log.LogDebug("{LogPrefix}Log consumer {ConsumerName} - {Message}", logPrefix, GetName(consumer), message.Message);
        }

        internal void ErrorConsume(IConsumer<byte[], byte[]> consumer, Error error)
        {
            string logPrefix = Thread.CurrentThread.Name != null ? $"stream-thread[{Thread.CurrentThread.Name}] " : "";
            log.LogError("{LogPrefix}Error consumer {ConsumerName} - {ErrorReason}", logPrefix, GetName(consumer), error.Reason);
        }

        #endregion

        #region Log Producer

        internal void LogProduce(IProducer<byte[], byte[]> producer, LogMessage message)
        {
            string logPrefix = Thread.CurrentThread.Name != null ? $"stream-thread[{Thread.CurrentThread.Name}] " : "";
            log.LogDebug("{LogPrefix}Log producer {ProducerName} - {Message}", logPrefix, GetName(producer), message.Message);
        }

        internal void ErrorProduce(IProducer<byte[], byte[]> producer, Error error)
        {
            string logPrefix = Thread.CurrentThread.Name != null ? $"stream-thread[{Thread.CurrentThread.Name}] " : "";
            log.LogError("{LogPrefix}Error producer {ProducerName} - {ErrorReason}", logPrefix, GetName(producer), error.Reason);
        }

        #endregion

        #region Log Admin

        internal void ErrorAdmin(IAdminClient admin, Error error)
        {
            string logPrefix = Thread.CurrentThread.Name != null ? $"stream-thread[{Thread.CurrentThread.Name}] " : "";
            log.LogError("{LogPrefix}Error admin {ClientName} - {ErrorReason}", logPrefix, GetName(admin), error.Reason);
        }

        internal void LogAdmin(IAdminClient admin, LogMessage message)
        {
            string logPrefix = Thread.CurrentThread.Name != null ? $"stream-thread[{Thread.CurrentThread.Name}] " : "";
            log.LogDebug("{LogPrefix}Log admin {ClientName} - {Message}", logPrefix, GetName(admin), message.Message);
        }

        #endregion

        private string GetName(IClient client)
        {
            // FOR FIX
            string name = "";
            try
            {
                name = client.Name;
            }
            catch (NullReferenceException)
            {
                name = "Unknown";
            }

            return name;
        }
    }
}