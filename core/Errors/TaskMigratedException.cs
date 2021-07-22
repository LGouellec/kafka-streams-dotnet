using System;

namespace Streamiz.Kafka.Net.Errors
{
    internal class TaskMigratedException : Exception
    {
        public TaskMigratedException(string message) 
            : base(message)
        {
        }
    }
}
