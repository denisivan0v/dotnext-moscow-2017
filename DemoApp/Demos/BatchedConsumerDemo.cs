using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using DemoApp.Kafka;

using Microsoft.Extensions.Logging;

namespace DemoApp.Demos
{
    public sealed class BatchedConsumerDemo : IDisposable
    {
        private const string GroupId = "batched-consumer";

        private readonly IEnumerable<string> _topics;
        private readonly KafkaConsumer _kafkaConsumer;
        private readonly int _batchSize;

        public BatchedConsumerDemo(ILogger logger, string brokerEndpoints, IEnumerable<string> topics, int batchSize)
        {
            _topics = topics;
            _batchSize = batchSize;
            _kafkaConsumer = new KafkaConsumer(logger, brokerEndpoints, GroupId);
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