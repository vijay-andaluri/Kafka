using System;

using Confluent.Kafka;

namespace Company.Kafka.Services.Factories.Interfaces
{
    public interface IConsumerFactory
    {
        IConsumer<TKey, TValue> GetConsumer<TKey, TValue>(ConsumerConfig consumerConfig, Action<ConsumerBuilderOptions<TKey, TValue>> configureAction);
    }
}
