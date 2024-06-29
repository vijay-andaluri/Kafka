using System;
using System.Collections.Generic;

using Company.Kafka.Services.Factories.Interfaces;

using Chr.Avro.Confluent;

using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Microsoft.Extensions.Logging;

namespace Company.Kafka.Services.Factories
{
    public sealed class SchemaRegistryProducerFactory : IProducerFactory, IDisposable
    {
        private readonly ISchemaRegistryClient _schemaRegistryClient;

        private readonly bool _shouldDisposeClient;

        private readonly IProducerBuilderProvider _builderProvider;

        private readonly ILogger<SchemaRegistryProducerFactory> _logger;

        private readonly Dictionary<Type, IDisposable> _producers = new Dictionary<Type, IDisposable>();

        private readonly object _producerChangeLock = new object();

        private bool _isDisposed;

        public SchemaRegistryProducerFactory(
            ISchemaRegistryClient schemaRegistryClient,
            bool shouldDisposeClient,
            IProducerBuilderProvider builderProvider,
            ILogger<SchemaRegistryProducerFactory> logger)
        {
            _schemaRegistryClient = schemaRegistryClient;
            _shouldDisposeClient = shouldDisposeClient;
            _builderProvider = builderProvider;
            _logger = logger;
        }

        public IProducer<TKey, TValue> GetProducer<TKey, TValue>(ProducerConfig producerConfig, Action<ProducerBuilderOptions<TKey, TValue>> configureAction = null)
        {
            lock (_producerChangeLock)
            {
                if (_isDisposed)
                {
                    throw new InvalidOperationException("Factory has already been disposed.");
                }

                var producerType = typeof(IProducer<TKey, TValue>);

                if (_producers.ContainsKey(producerType))
                {
                    _logger.LogDebug($"Found cache key for IProducer<{typeof(TKey).Name}, {typeof(TValue).Name}>");
                    return _producers[producerType] as IProducer<TKey, TValue>
                           ?? throw new Exception($"Cached instance for IProducer<{typeof(TKey).Name}, {typeof(TValue).Name} is null");
                }

                _logger.LogDebug($"Creating instance of IProducer<{typeof(TKey).Name}, {typeof(TValue).Name}>");
                var builder = _builderProvider.GetBuilder<TKey, TValue>(producerConfig);

                builder.SetValueSerializer(new AsyncSchemaRegistrySerializer<TValue>(_schemaRegistryClient)
                    .AsSyncOverAsync());

                if (typeof(TKey) != typeof(Null))
                {
                    builder.SetKeySerializer(new AsyncSchemaRegistrySerializer<TKey>(_schemaRegistryClient)
                        .AsSyncOverAsync());
                }

                var options = new ProducerBuilderOptions<TKey, TValue>(builder);

                if (configureAction == default)
                {
                    // drop log noise when handler options are not set
                    options.SetLogHandler((p, m) => { })
                        .SetErrorHandler((p, e) => { })
                        .SetStatisticsHandler((p, s) => { });
                }
                else
                {
                    configureAction?.Invoke(options);
                }

                var producer = builder.Build();

                _producers[producerType] = producer;

                return producer;
            }
        }

        public void Dispose()
        {
            lock (_producerChangeLock)
            {
                _isDisposed = true;

                foreach (var producer in _producers.Values)
                {
                    producer?.Dispose();
                }
            }

            if (_shouldDisposeClient)
            {
                _schemaRegistryClient?.Dispose();
            }
        }
    }
}
