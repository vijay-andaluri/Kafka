using Confluent.Kafka;

namespace Company.Kafka.Services.Factories.Interfaces
{
    public interface IProducerBuilderProvider
    {
        ProducerBuilder<TKey, TValue> GetBuilder<TKey, TValue>(ProducerConfig config);
    }
}
