using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka;
using Confluent.Kafka.Serialization;

using Microsoft.Extensions.Logging;

namespace DemoApp.Kafka
{
    public sealed class ManualKafkaConsumer : IDisposable
    {
        private readonly ILogger _logger;
        private readonly Consumer<Null, string> _consumer;

        public ManualKafkaConsumer(ILogger logger, string brokerEndpoints, string groupId)
        {
            _logger = logger;

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

        public IReadOnlyCollection<Message<Null, string>> Consume(IEnumerable<TopicPartition> partitions, int batchSize, CancellationToken cancellationToken)
        {
            var messages = new List<Message<Null, string>>();
            var isEof = false;

            void OnMessage(object sender, Message<Null, string> message) => messages.Add(message);

            void OnEof(object sender, TopicPartitionOffset offset) => isEof = true;

            try
            {
                _consumer.OnMessage += OnMessage;
                _consumer.OnPartitionEOF += OnEof;
                _consumer.Assign(partitions);

                while (messages.Count < batchSize && !isEof && !cancellationToken.IsCancellationRequested)
                {
                    _consumer.Poll(TimeSpan.FromMilliseconds(100));
                }

                return messages;
            }
            finally
            {
                _consumer.OnMessage -= OnMessage;
                _consumer.OnPartitionEOF -= OnEof;
            }
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