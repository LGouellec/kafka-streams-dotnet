using System;
using System.Threading;
using System.Threading.Tasks;

namespace Streamiz.Kafka.Net.Tests.Helpers
{
    internal class ExecutorService
    {
        private Func<Task> action;
        private Thread _thread;

        public ExecutorService(Func<Task> action)
        {
            this.action = action;
            _thread = new Thread(() =>
            {
                try
                {
                    action().GetAwaiter().GetResult();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }
            });
        }

        public static ExecutorService NewSingleThreadExecutor(Func<Task> action)
        {
            return new ExecutorService(action);
        }

        public void Start()
        {
            _thread.Start();
        }

        public void Stop()
        {
            _thread.Join();
        }
    }
}
