using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace app_producer_kafka
{
    static class Program
    {
        public static async Task Main(string[] args)
        {
            var config = new ProducerConfig { BootstrapServers = "127.0.0.1:9092" };
            using var p = new ProducerBuilder<Null, string>(config).Build();

            try
            {
                int cont = 0;

                while (true)
                {
                    var dr = await p.ProduceAsync("test-topic",
                            new Message<Null, string> { Value = $"test:  {cont++}" });


                    Console.WriteLine($"A mensagem '{dr.Value}' entrega no topico '{dr.TopicPartitionOffset} | {cont}'");

                    Thread.Sleep(2000);
                }
            }
            catch (ProduceException<Null, string> e)
            {
                Console.WriteLine($"Delivery failed: {e.Error.Reason}");
            }
        }
    }
}
