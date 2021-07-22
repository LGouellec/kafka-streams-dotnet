using Confluent.Kafka;

namespace Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ConsumerConfig
            {
                GroupId = "test-app",
                BootstrapServers = "192.168.56.1:9092",
                SecurityProtocol = SecurityProtocol.SaslPlaintext,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "admin",
                SaslPassword = "admin",
                Debug = "generic, broker, topic, metadata, consumer"
            };

            var builder = new ConsumerBuilder<string, string>(config);
            using (var consumer = builder.Build())
            {
                var water = consumer.GetWatermarkOffsets(new TopicPartition("test", 0));
                consumer.Assign(new TopicPartition("test", 0));
                //Thread.Sleep(5000);
                consumer.Seek(new TopicPartitionOffset("test", 0, water.Low));
                while (true)
                {
                    var r = consumer.Consume(1000);
                }
            }
        }
    }
}