using System;

using Confluent.Kafka;

namespace Company.Kafka.Services.Factories.Interfaces
{
    public interface IProducerFactory
    {
        IProducer<TKey, TValue> GetProducer<TKey, TValue>(ProducerConfig producerConfig, Action<ProducerBuilderOptions<TKey, TValue>> configureAction = null);
    }
}
