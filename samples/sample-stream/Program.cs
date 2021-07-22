using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;

namespace sample_stream
{
    /// <summary>
    /// Sample program with a passtrought stream, instanciate and dispose with CTRL+ C console event.
    /// If you want an example with token source passed to startasync, see <see cref="ProgramToken"/> class.
    /// </summary>
    internal class Program
    {
        static async System.Threading.Tasks.Task Main(string[] args)
        {
            ConfigureLog4Net();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app";
            config.BootstrapServers = "localhost:9092";
            config.AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest;
          
            StreamBuilder builder = new StreamBuilder();

            builder.Table("topic-test", RocksDb<string, string>.As("state-store"));

            Topology t = builder.Build();
            KafkaStream stream = new KafkaStream(t, config);

            Console.CancelKeyPress += (o, e) => stream.Dispose();

            bool isRunningState = false;
            DateTime dt = DateTime.Now;
            var timeout = TimeSpan.FromSeconds(100);
            stream.StateChanged += (old, @new) =>
            {
                if (@new.Equals(KafkaStream.State.RUNNING))
                {
                    isRunningState = true;
                }
            };

            await stream.StartAsync();

            while (!isRunningState)
            {
                Thread.Sleep(100);
                if (DateTime.Now > dt + timeout)
                {
                    break;
                }
            }

            while (true)
            {
                var store = stream.Store(StoreQueryParameters.FromNameAndType("state-store", QueryableStoreTypes.KeyValueStore<string, string>()));
                var elements = store.All().ToList();
                foreach (var s in elements)
                    Console.WriteLine($"{s.Key}|{s.Value}");
                Thread.Sleep(5000);
            }
        }

        private static void ConfigureLog4Net()
        {
            var loggerFactory =  LoggerFactory.Create(builder => builder.AddLog4Net());
            Logger.LoggerFactory = loggerFactory;
        }
    }
}