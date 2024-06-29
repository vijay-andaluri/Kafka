using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Company.Kafka.Services;
using Company.Kafka.Services.Factories.Interfaces;
using Company.Kafka.TestHost.Configuration;
using Company.Kafka.TestHost.Messages;

using Confluent.Kafka;

using Microsoft.Extensions.Logging;

namespace Company.Kafka.TestHost.HostedServices
{
    public class MessageConsumerService : BatchConsumerService<string, TestMessage>
    {
        private readonly ILogger _messageLogger;

        public MessageConsumerService(ILoggerFactory loggerFactory, IConsumerFactory consumerFactory, Consumers consumers)
            : base(loggerFactory, consumerFactory, consumers.TestConsumer)
        {
            _messageLogger = loggerFactory.CreateLogger("Company.ConsumerMessageLogger");
        }

        protected override Task HandleMessageBatchAsync(List<ConsumeResult<string, TestMessage>> results, CancellationToken token)
        {
            _messageLogger.LogInformation($"Received batch of size: {results.Count}");

            foreach (var consumeResult in results)
            {
                _messageLogger.LogInformation($"Consumed - Key: {consumeResult.Message.Key} Message: {consumeResult.Message.Value}");
            }

            return Task.CompletedTask;
        }
    }
}
