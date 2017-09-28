using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using DemoApp.Kafka;

using Microsoft.Extensions.Logging;

namespace DemoApp.Demos
{
    public sealed class NonEfficientConsumerDemo : IDisposable
    {
        private const string GroupId = "non-efficient-consumer";

        private readonly IEnumerable<string> _topics;
        private readonly NonEfficientKafkaConsumer _kafkaConsumer;
        private readonly int _batchSize;

        public NonEfficientConsumerDemo(ILogger logger, string brokerEndpoints, IEnumerable<string> topics, int batchSize)
        {
            _topics = topics;
            _batchSize = batchSize;
            _kafkaConsumer = new NonEfficientKafkaConsumer(logger, brokerEndpoints, GroupId);
        }

        public async Task RunAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var messages = _kafkaConsumer.Consume(_topics, _batchSize, cancellationToken);
                foreach (var message in messages)
                {
                    Console.WriteLine($"{message.Topic}/{message.Partition} @{message.Offset}: '{message.Value}'");
                    await _kafkaConsumer.CommitAsync(message);
                }
            }
        }

        public void Dispose()
        {
            _kafkaConsumer.Dispose();
        }
    }
}