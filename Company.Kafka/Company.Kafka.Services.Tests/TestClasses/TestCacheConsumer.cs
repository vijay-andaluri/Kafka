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
    public class TestCacheConsumer<TKey> : CacheConsumerService<TKey, string, string>
    {
        public TestCacheConsumer(
            ILoggerFactory loggerFactory,
            IConsumerFactory consumerFactory,
            ConsumerInstanceSettings settings,
            Func<ConsumeResult<TKey, string>, bool> shouldIgnore = default)
            : base(loggerFactory, consumerFactory, settings, shouldIgnore)
        {
        }

        public Action<ConsumeResult<TKey, string>> NewMessageAction { get; set; }

        public Action<Dictionary<string, ConsumeResult<TKey, string>>> OnLoadingAction { get; set; }

        protected override Func<TKey, string, string> LoadMessageKeySelector => (k, v) => k?.ToString() ?? "ignored";

        protected override Task OnMessagesLoadedAsync(Dictionary<string, ConsumeResult<TKey, string>> valuesLoaded, CancellationToken token)
        {
            OnLoadingAction?.Invoke(valuesLoaded);
            return Task.CompletedTask;
        }

        protected override Task OnNewMessageAsync(ConsumeResult<TKey, string> result, CancellationToken token)
        {
            NewMessageAction?.Invoke(result);
            return Task.CompletedTask;
        }
    }
}
