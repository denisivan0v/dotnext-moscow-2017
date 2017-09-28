using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka;

using DemoApp.Kafka;

using Microsoft.Extensions.Logging;

namespace DemoApp.Demos
{
    public sealed class ManualConsumerDemo : IDisposable
    {
        private const string GroupId = "manual-consumer";

        private readonly IEnumerable<TopicPartition> _partitions;
        private readonly ManualKafkaConsumer _kafkaConsumer;
        private readonly int _batchSize;

        public ManualConsumerDemo(ILogger logger, string brokerEndpoints, IEnumerable<TopicPartition> partitions, int batchSize)
        {
            _partitions = partitions;
            _batchSize = batchSize;
            _kafkaConsumer = new ManualKafkaConsumer(logger, brokerEndpoints, GroupId);
        }

        public async Task RunAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var messages = _kafkaConsumer.Consume(_partitions, _batchSize, cancellationToken);
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