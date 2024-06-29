using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Company.Kafka.Services.Configuration;
using Company.Kafka.Services.Factories.Interfaces;
using Confluent.Kafka;

using Microsoft.Extensions.Logging;

namespace Company.Kafka.Services.Tests.TestClasses
{
    public class TestBatchConsumer : BatchConsumerService<string, string>
    {
        public TestBatchConsumer(
            ILoggerFactory loggerFactory,
            IConsumerFactory consumerFactory,
            BatchConsumerSettings settings)
            : base(loggerFactory, consumerFactory, settings)
        {
        }

        public Action<List<ConsumeResult<string, string>>, CancellationToken> MessageAction { get; set; }

        protected override Task HandleMessageBatchAsync(List<ConsumeResult<string, string>> results, CancellationToken token)
        {
            MessageAction?.Invoke(results, token);
            return Task.CompletedTask;
        }
    }
}
