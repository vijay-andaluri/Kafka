using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Company.Kafka.Services;
using Company.Kafka.Services.Configuration;
using Company.Kafka.Services.Factories.Interfaces;
using Company.Kafka.TestHost.Configuration;
using Company.Kafka.TestHost.Enums;
using Company.Kafka.TestHost.Messages;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Company.Kafka.TestHost.HostedServices
{
    public class TestCacheMessageConsumer : CacheConsumerService<string, TestMessage, ThingType>
    {
        public TestCacheMessageConsumer(ILoggerFactory loggerFactory, IConsumerFactory consumerFactory, Consumers consumers, Func<ConsumeResult<string, TestMessage>, bool> ignoreMessagePredicate = default)
            : base(loggerFactory, consumerFactory, consumers.CacheConsumerSettings, ignoreMessagePredicate)
        {
        }

        protected override Func<string, TestMessage, ThingType> LoadMessageKeySelector =>
            (s, message) => message.ThingType;

        protected override Task OnMessagesLoadedAsync(Dictionary<ThingType, ConsumeResult<string, TestMessage>> valuesLoaded, CancellationToken token)
        {
            Logger.LogInformation($"Loaded {valuesLoaded.Count} distinct values based on key.");

            return Task.CompletedTask;
        }

        protected override Task OnNewMessageAsync(ConsumeResult<string, TestMessage> result, CancellationToken token)
        {
            Logger.LogInformation($"Processing new message with Key: '{result.Message.Key}'");

            return Task.CompletedTask;
        }
    }
}
