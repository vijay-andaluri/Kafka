using Company.Kafka.Services.Factories.Interfaces;
using Confluent.Kafka;

namespace Company.Kafka.Services.Factories
{
    public class ProducerBuilderProvider : IProducerBuilderProvider
    {
        public ProducerBuilder<TKey, TValue> GetBuilder<TKey, TValue>(ProducerConfig config) => new ProducerBuilder<TKey, TValue>(config);
    }
}
