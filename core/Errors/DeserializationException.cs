using System;

namespace Streamiz.Kafka.Net.Errors
{
    /// <summary>
    /// Deserialization exception throw when deserialization input message is in error and <see cref="IStreamConfig.DeserializationExceptionHandler"/> return <see cref="ExceptionHandlerResponse.FAIL"/>.
    /// </summary>
    public class DeserializationException : Exception
    {
        /// <summary>
        /// Deserialization exception throw when deserialization input message is in error and <see cref="IStreamConfig.DeserializationExceptionHandler"/> return <see cref="ExceptionHandlerResponse.FAIL"/>.
        /// </summary>
        /// <param name="message">Exception message</param>
        /// <param name="innerException">Inner deserialization exception</param>
        public DeserializationException(string message, Exception innerException) :
            base(message, innerException)
        {
        }
    }
}
