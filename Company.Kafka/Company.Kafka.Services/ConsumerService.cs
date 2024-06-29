using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Company.Kafka.Services.Configuration;
using Company.Kafka.Services.Factories.Interfaces;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Company.Kafka.Services
{
    public abstract class ConsumerService<TKey, TValue> : BatchConsumerService<TKey, TValue>
    {
        /// <summary>
        /// Abstract class for implementing a one-at-a-time message consumer
        /// </summary>
        /// <param name="loggerFactory"></param>
        /// <param name="consumerFactory"></param>
        /// <param name="consumerInstanceSettings"></param>
        protected ConsumerService(ILoggerFactory loggerFactory, IConsumerFactory consumerFactory, ConsumerInstanceSettings consumerInstanceSettings)
            : base(loggerFactory, consumerFactory, SingleMessageModeSettings(consumerInstanceSettings))
        {
        }

        protected sealed override Task HandleMessageBatchAsync(List<ConsumeResult<TKey, TValue>> results, CancellationToken token)
        {
            return HandleMessageAsync(results.Single(), token);
        }

        /// <summary>
        /// Abstract method for performing logic to process a message.  For this consumer this should be done quickly.
        /// Otherwise, consider using a <see cref="BatchConsumerService{TKey,TValue}"/>
        /// </summary>
        /// <param name="message"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        protected abstract Task HandleMessageAsync(ConsumeResult<TKey, TValue> message, CancellationToken token = default);

        private static BatchConsumerSettings SingleMessageModeSettings(ConsumerInstanceSettings givenSettings)
        {
            var settings = new BatchConsumerSettings
            {
                MaxBatchWaitMilliseconds = 0,
                MaxBatchSize = 1
            };

            var properties = givenSettings.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);

            foreach (var property in properties.Where(p => p.CanWrite && p.CanRead))
            {
                property.SetValue(settings, property.GetValue(givenSettings));
            }

            return settings;
        }
    }
}
