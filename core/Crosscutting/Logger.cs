using System;
using Microsoft.Extensions.Logging;
using ILogger = Microsoft.Extensions.Logging.ILogger;
using ILoggerFactory = Microsoft.Extensions.Logging.ILoggerFactory;

namespace Streamiz.Kafka.Net.Crosscutting
{
    /// <summary>
    /// Helper logger to get logger from type class
    /// </summary>
    public static class Logger
    {
        private static ILoggerFactory _factory;

        /// <summary>
        /// If logger factory is not set by the project using the library
        /// it will create a factory with console logger configured.
        /// </summary>
        public static ILoggerFactory LoggerFactory
        {
            get => _factory ??= Microsoft.Extensions.Logging.LoggerFactory.Create(builder => builder.AddConsole());
            set => _factory = value;
        }

        /// <summary>   
        /// Get logger from type class.
        /// </summary>
        /// <param name="type">Class type which call logger</param>
        /// <returns>Return logger configured</returns>
        public static ILogger GetLogger(Type type)
        {
            return LoggerFactory.CreateLogger(type);
        }
    }
}