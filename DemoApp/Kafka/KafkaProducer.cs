using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

using Confluent.Kafka;
using Confluent.Kafka.Serialization;

using Microsoft.Extensions.Logging;

namespace DemoApp.Kafka
{
    public class KafkaProducer : IDisposable
    {
        private readonly ILogger _logger;
        private readonly Producer<Null, string> _producer;

        public KafkaProducer(ILogger logger, string brokerEndpoints)
        {
            _logger = logger;

            var config = new Dictionary<string, object>
                {
                    { "bootstrap.servers", brokerEndpoints },
                    { "api.version.request", true },
                    { "socket.blocking.max.ms", 1 },
                    { "queue.buffering.max.ms", 5 },
                    { "queue.buffering.max.kbytes", 10240 },
#if DEBUG
                    { "debug", "msg" },
#endif
                    {
                        "default.topic.config",
                        new Dictionary<string, object>
                            {
                                { "message.timeout.ms", 3000 },
                                { "request.required.acks", -1 }
                            }
                    }
                };
            _producer = new Producer<Null, string>(config, new NullSerializer(), new StringSerializer(Encoding.UTF8));
            _producer.OnLog += OnLog;
            _producer.OnError += OnError;
        }

        public async Task<Message<Null, string>> ProduceAsync(string value, string topic, int partition = -1)
        {
            Message<Null, string> message = null;
            try
            {
                if (partition < 0)
                {
                    message = await _producer.ProduceAsync(topic, null, value);
                }
                else
                {
                    message = await _producer.ProduceAsync(topic, null, value, partition);
                }

                if (message.Error.HasError)
                {
                    throw new KafkaException(message.Error);
                }

                return message;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    new EventId(),
                    ex,
                    "Error producing to Kafka. Topic/partition: '{topic}/{partition}'. Message: {message}'.",
                    topic,
                    partition,
                    message?.Value ?? "N/A");
                throw;
            }
        }

        public void Dispose()
        {
            if (_producer != null)
            {
                _producer.OnLog -= OnLog;
                _producer.OnError -= OnError;
                _producer.Dispose();
            }
        }

        private void OnLog(object sender, LogMessage logMessage)
            => _logger.LogInformation(
                "Producing to Kafka. Client: {client}, syslog level: '{logLevel}', message: {logMessage}.",
                logMessage.Name,
                logMessage.Level,
                logMessage.Message);

        private void OnError(object sender, Error error)
            => _logger.LogInformation("Producer error: {error}. No action required.", error);
    }
}