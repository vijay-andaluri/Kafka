using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Company.Kafka.Services.Configuration;
using Company.Kafka.Services.Factories.Interfaces;
using Company.Kafka.TestHost.Configuration;
using Company.Kafka.TestHost.Messages;

using Confluent.Kafka;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Company.Kafka.TestHost.HostedServices
{
    public class FanOutConsumerService : IHostedService
    {
        private static readonly CancellationTokenSource Cts = new CancellationTokenSource();

        private readonly IConsumerFactory _consumerFactory;

        private readonly ILogger _messageLogger;

        private readonly ConsumerInstanceSettings _baseSettings;

        private readonly List<IConsumer<string, TestMessage>> _consumers = new List<IConsumer<string, TestMessage>>(3);

        private readonly List<Task> _pollingTasks = new List<Task>(3);

        public FanOutConsumerService(ILoggerFactory loggerFactory, IConsumerFactory consumerFactory, Consumers consumerSettings)
        {
            _consumerFactory = consumerFactory;
            _messageLogger = loggerFactory.CreateLogger("Company.ConsumerMessageLogger");
            _baseSettings = consumerSettings.BaseFanOutConsumer;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            for (int i = 0; i < 3; i++)
            {
                var consumerIndex = i;
                var consumer = _consumerFactory.GetConsumer<string, TestMessage>(
                    _baseSettings,
                    options => options
                        .SetLogHandler((c, l) => { })
                        .SetPartitionsAssignedHandler((c, l) => _messageLogger.LogInformation($"FanoutConsumer: {consumerIndex} assigned {string.Join(",", l.Select(p => p.Partition))}")));
                consumer.Subscribe(_baseSettings.TopicName);

                _consumers.Add(consumer);
                _pollingTasks.Add(Task.Run(() => PollForMessages(consumerIndex, consumer, _messageLogger)));
            }

            return Task.CompletedTask;
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            Cts.Cancel();
            await Task.WhenAll(_pollingTasks);
            _consumers.ForEach(c =>
            {
                c.Unsubscribe();
                c.Dispose();
            });
        }

        private static void PollForMessages(int consumerNumber, IConsumer<string, TestMessage> consumer, ILogger logger)
        {
            while (!Cts.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(Cts.Token);
                    logger.LogInformation(
                        $"FanoutConsumer: {consumerNumber} received Key: {result.Message.Key} Message: {result.Message.Value}");
                }
                catch (OperationCanceledException)
                {
                }
            }
        }
    }
}
