using System;
using System.Threading;
using System.Threading.Tasks;

using Company.Kafka.TestHost.Configuration;
using Company.Kafka.TestHost.Enums;
using Company.Kafka.TestHost.Messages;

using Confluent.Kafka;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Company.Kafka.TestHost.HostedServices
{
    public class TestMessageService : IHostedService
    {
        private readonly CancellationTokenSource _cancellationTokenSource;

        private readonly IProducer<string, TestMessage> _producer;

        private readonly ILogger<TestMessageService> _logger;

        private Task _testMessageTask;

        public TestMessageService(IProducer<string, TestMessage> producer, ILogger<TestMessageService> logger)
        {
            _logger = logger;
            _cancellationTokenSource = new CancellationTokenSource();
            _producer = producer;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _testMessageTask = Task.Run(TestLoop, cancellationToken);

            return Task.CompletedTask;
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            _cancellationTokenSource?.Cancel();
            await _testMessageTask;
        }

        private async Task TestLoop()
        {
            try
            {
                while (!_cancellationTokenSource.IsCancellationRequested)
                {
                    var timestamp = DateTime.UtcNow.ToString("u");
                    var nextMessage = new Message<string, TestMessage>
                    {
                        Key = timestamp,
                        Value = new TestMessage
                        {
                            Amount = 2.718281828M,
                            CreatedAt = DateTime.UtcNow,
                            CreatedAtOffset = DateTimeOffset.UtcNow,
                            IsActive = true,
                            Name = Guid.NewGuid().ToString(),
                            Radius = 1.12345,
                            SmallNumber = 931,
                            Thing = 101,
                            ThingType = ThingType.Grumpy
                        }
                    };

                    _logger.LogInformation($"Producing - Key: {nextMessage.Key} Message: {nextMessage.Value}");
                    await _producer.ProduceAsync(ProducerTopics.TestTopic, nextMessage);
                    await Task.Delay(TimeSpan.FromSeconds(1), _cancellationTokenSource.Token);
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("service task cancelled");
            }
        }
    }
}
