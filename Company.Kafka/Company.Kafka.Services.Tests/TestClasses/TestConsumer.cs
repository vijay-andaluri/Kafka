using System;
using System.Threading;
using System.Threading.Tasks;

using Company.Kafka.Services.Configuration;
using Company.Kafka.Services.Factories.Interfaces;
using Confluent.Kafka;

using Microsoft.Extensions.Logging;

namespace Company.Kafka.Services.Tests.TestClasses
{
    public class TestConsumer : ConsumerService<string, string>
    {
        public TestConsumer(
            ILoggerFactory loggerFactory,
            IConsumerFactory consumerFactory,
            ConsumerInstanceSettings settings)
            : base(loggerFactory, consumerFactory, settings)
        {
        }

        public Action<ConsumeResult<string, string>, CancellationToken> MessageAction { get; set; }

        protected override Task HandleMessageAsync(
            ConsumeResult<string, string> message,
            CancellationToken token = default)
        {
            MessageAction?.Invoke(message, token);
            return Task.CompletedTask;
        }
    }
}
