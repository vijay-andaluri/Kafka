using System;

using Company.Kafka.Services.Factories.Interfaces;

using Chr.Avro.Confluent;

using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;

namespace Company.Kafka.Services.Factories
{
    public class SchemaRegistryConsumerFactory : IConsumerFactory, IDisposable
    {
        private readonly ISchemaRegistryClient _schemaRegistryClient;

        private readonly bool _shouldDisposeClient;

        public SchemaRegistryConsumerFactory(ISchemaRegistryClient schemaRegistryClient, bool shouldDisposeClient)
        {
            _schemaRegistryClient = schemaRegistryClient;
            _shouldDisposeClient = shouldDisposeClient;
        }

        public IConsumer<TKey, TValue> GetConsumer<TKey, TValue>(ConsumerConfig consumerConfig, Action<ConsumerBuilderOptions<TKey, TValue>> configureAction)
        {
            var builder = new ConsumerBuilder<TKey, TValue>(consumerConfig);

            builder.SetValueDeserializer(new AsyncSchemaRegistryDeserializer<TValue>(_schemaRegistryClient).AsSyncOverAsync());
            if (typeof(TKey) != typeof(Ignore) && typeof(TKey) != typeof(Null))
            {
                builder.SetKeyDeserializer(new AsyncSchemaRegistryDeserializer<TKey>(_schemaRegistryClient).AsSyncOverAsync());
            }

            var options = new ConsumerBuilderOptions<TKey, TValue>(builder);
            configureAction?.Invoke(options);

            return builder.Build();
        }

        public void Dispose()
        {
            if (!_shouldDisposeClient)
            {
                return;
            }

            _schemaRegistryClient?.Dispose();
        }
    }
}
