using System;
using System.Collections.Generic;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka;

using DemoApp.Kafka;

using Microsoft.Extensions.Logging;

namespace DemoApp.Demos
{
    public sealed class RxConsumerDemo : IDisposable
    {
        private const string GroupId = "rx-consumer";

        private readonly RxKafkaConsumer _kafkaConsumer;
        private readonly int _batchSize;

        private IDisposable _registration;

        public RxConsumerDemo(ILogger logger, string brokerEndpoints, IEnumerable<string> topics, int batchSize)
        {
            _batchSize = batchSize;
            _kafkaConsumer = new RxKafkaConsumer(logger, brokerEndpoints, GroupId, topics);
        }

        public async Task RunAsync(CancellationToken cancellationToken)
        {
            var observable = _kafkaConsumer.Consume(cancellationToken);

            observable.Buffer(_batchSize)
                      .Subscribe(
                          messages =>
                              {
                                  foreach (var message in messages)
                                  {
                                      Console.WriteLine($"{message.Topic}/{message.Partition} @{message.Offset}: '{message.Value}'");
                                  }

                                  _kafkaConsumer.CommitAsync(messages[messages.Count - 1]).GetAwaiter().GetResult();
                              });

            var taskCompletionSource = new TaskCompletionSource<object>();
            _registration = cancellationToken.Register(() => taskCompletionSource.SetResult(null));

            await taskCompletionSource.Task;
        }

        public void Dispose()
        {
            _registration.Dispose();
            _kafkaConsumer.Dispose();
        }
    }
}