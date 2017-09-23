using System;
using System.Threading;
using System.Threading.Tasks;

using DemoApp.Kafka;

using Microsoft.Extensions.Logging;

namespace DemoApp.Demos
{
    public sealed class ProducerDemo : IDisposable
    {
        private readonly string _topic;
        private readonly KafkaProducer _kafkaProducer;

        public ProducerDemo(ILogger logger, string brokerEndpoints, string topic)
        {
            _topic = topic;
            _kafkaProducer = new KafkaProducer(logger, brokerEndpoints);
        }

        public async Task RunAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var value = Guid.NewGuid().ToString();
                var message = await _kafkaProducer.ProduceAsync(value, _topic);

                Console.WriteLine($"Message '{message.Value}' produced to '{message.Topic}/{message.Partition} @{message.Offset}'");
            }
        }

        public void Dispose()
        {
            _kafkaProducer.Dispose();
        }
    }
}