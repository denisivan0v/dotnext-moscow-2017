using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka;
using Confluent.Kafka.Serialization;

using Microsoft.Extensions.Logging;

namespace DemoApp.Kafka
{
    public sealed class RxKafkaConsumer : IDisposable
    {
        private readonly ILogger _logger;
        private readonly IEnumerable<string> _topics;
        private readonly Consumer<Null, string> _consumer;

        public RxKafkaConsumer(ILogger logger, string brokerEndpoints, string groupId, IEnumerable<string> topics)
        {
            _logger = logger;
            _topics = topics;

            var config = new Dictionary<string, object>
                {
                    { "bootstrap.servers", brokerEndpoints },
                    { "api.version.request", true },
                    { "group.id", !string.IsNullOrEmpty(groupId) ? groupId : Guid.NewGuid().ToString() },
                    { "socket.blocking.max.ms", 1 },
                    { "enable.auto.commit", false },
                    { "fetch.wait.max.ms", 5 },
                    { "fetch.error.backoff.ms", 5 },
                    { "fetch.message.max.bytes", 10240 },
                    { "queued.min.messages", 1000 },
#if DEBUG
                    { "debug", "msg" },
#endif
                    {
                        "default.topic.config",
                        new Dictionary<string, object>
                            {
                                { "auto.offset.reset", "beginning" }
                            }
                    }
                };
            _consumer = new Consumer<Null, string>(config, new NullDeserializer(), new StringDeserializer(Encoding.UTF8));
            _consumer.OnLog += OnLog;
            _consumer.OnError += OnError;
            _consumer.OnConsumeError += OnConsumeError;
        }

        public IObservable<Message<Null, string>> Consume(CancellationToken cancellationToken)
        {
            var observable = Observable.FromEventPattern<Message<Null, string>>(
                                 x =>
                                     {
                                         _consumer.OnMessage += x;
                                         _consumer.Subscribe(_topics);
                                     },
                                 x =>
                                     {
                                         _consumer.Unsubscribe();
                                         _consumer.OnMessage -= x;
                                     })
                             .Select(x => x.EventArgs);

            Task.Factory.StartNew(
                    () =>
                        {
                            while (!cancellationToken.IsCancellationRequested)
                            {
                                _consumer.Poll(TimeSpan.FromMilliseconds(100));
                            }
                        },
                    cancellationToken,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default)
                .ConfigureAwait(false);

            return observable;
        }

        public async Task CommitAsync(Message<Null, string> message) => await _consumer.CommitAsync(message);

        public void Dispose()
        {
            if (_consumer != null)
            {
                _consumer.OnLog -= OnLog;
                _consumer.OnError -= OnError;
                _consumer.OnConsumeError -= OnConsumeError;
                _consumer.Dispose();
            }
        }

        private void OnLog(object sender, LogMessage logMessage)
            => _logger.LogInformation(
                "Consuming from Kafka. Client: '{client}', syslog level: '{logLevel}', message: '{logMessage}'.",
                logMessage.Name,
                logMessage.Level,
                logMessage.Message);

        private void OnError(object sender, Error error)
            => _logger.LogInformation("Consumer error: {error}. No action required.", error);

        private void OnConsumeError(object sender, Message message)
        {
            _logger.LogError(
                "Error consuming from Kafka. Topic/partition/offset: '{topic}/{partition}/{offset}'. Error: '{error}'.",
                message.Topic,
                message.Partition,
                message.Offset,
                message.Error);
            throw new KafkaException(message.Error);
        }
    }
}