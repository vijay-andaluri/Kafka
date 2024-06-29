using System;

using Company.Kafka.Services.Factories;
using Company.Kafka.Services.Factories.Interfaces;

using Chr.Avro.Confluent;

using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Company.Kafka.Services.DependencyInjection
{
    public static class ServiceExtensions
    {
        /// <summary>
        /// Registers an instance of <see cref="ISchemaRegistryClient"/> using the provided configuration.
        /// </summary>
        /// <param name="services"></param>
        /// <param name="schemaRegistryConfig"></param>
        /// <returns></returns>
        public static IServiceCollection AddSchemaRegistryClient(
            this IServiceCollection services,
            SchemaRegistryConfig schemaRegistryConfig) =>
            services.AddSingleton<ISchemaRegistryClient>(new CachedSchemaRegistryClient(schemaRegistryConfig));

        /// <summary>
        /// Registers an implementation of <see cref="IConsumerFactory"/> with its own <see cref="ISchemaRegistryClient"/>
        /// using <see cref="AsyncSchemaRegistryDeserializer{T}"/> for key and value deserialization.
        /// </summary>
        /// <param name="services"></param>
        /// <param name="schemaRegistryConfig">Defaults to an existing registration in the service collection.</param>
        /// <returns></returns>
        public static IServiceCollection AddSchemaRegistryConsumerFactory(this IServiceCollection services, SchemaRegistryConfig schemaRegistryConfig) =>
            services.AddSingleton<IConsumerFactory>(c =>
                new SchemaRegistryConsumerFactory(new CachedSchemaRegistryClient(schemaRegistryConfig), true));

        /// <summary>
        /// Registers an instance of <see cref="IConsumerFactory"/> that uses an existing registration of <see cref="ISchemaRegistryClient"/>
        /// using <see cref="AsyncSchemaRegistryDeserializer{T}"/> for key and value deserialization.  This is useful when a single schema registry should be used.
        /// </summary>
        /// <param name="services"></param>
        /// <returns></returns>
        public static IServiceCollection AddSchemaRegistryConsumerFactory(this IServiceCollection services) =>
            services.AddSingleton<IConsumerFactory>(c =>
                new SchemaRegistryConsumerFactory(
                    c.GetService<ISchemaRegistryClient>() ??
                    throw new ArgumentException($"{nameof(ISchemaRegistryClient)} required.  Ensure a call to {nameof(AddSchemaRegistryClient)} or an equivalent registration has been made."),
                    false));

        /// <summary>
        /// Registers an implementation of <see cref="IProducerFactory"/> with its own <see cref="ISchemaRegistryClient"/>
        /// using <see cref="AsyncSchemaRegistrySerializer{T}"/> for key and value deserialization.
        /// </summary>
        /// <param name="services"></param>
        /// <param name="schemaRegistryConfig">Defaults to an existing registration in the service collection.</param>
        /// <returns></returns>
        public static IServiceCollection AddSchemaRegistryProducerFactory(this IServiceCollection services, SchemaRegistryConfig schemaRegistryConfig) =>
            services.AddSingleton<IProducerFactory>(c =>
                new SchemaRegistryProducerFactory(
                    new CachedSchemaRegistryClient(schemaRegistryConfig),
                    true,
                    new ProducerBuilderProvider(),
                    c.GetRequiredService<ILogger<SchemaRegistryProducerFactory>>()));

        /// <summary>
        /// Registers an instance of <see cref="IProducerFactory"/> that uses an existing registration of <see cref="ISchemaRegistryClient"/>
        /// using <see cref="AsyncSchemaRegistrySerializer{T}"/> for key and value deserialization.  This is useful when a single schema registry should be used.
        /// </summary>
        /// <param name="services"></param>
        /// <returns></returns>
        public static IServiceCollection AddSchemaRegistryProducerFactory(this IServiceCollection services) =>
            services.AddSingleton<IProducerFactory>(c =>
                new SchemaRegistryProducerFactory(
                    c.GetService<ISchemaRegistryClient>() ??
                    throw new ArgumentException($"{nameof(ISchemaRegistryClient)} required.  Ensure a call to {nameof(AddSchemaRegistryClient)} or an equivalent registration has been made."),
                    false,
                    new ProducerBuilderProvider(),
                    c.GetRequiredService<ILogger<SchemaRegistryProducerFactory>>()));

        /// <summary>
        /// Registers a single instance of <see cref="IProducer{TKey,TValue}"/> trying to find serialization registered in <see cref="IServiceCollection"/>
        /// and defaulting to <see cref="AsyncSchemaRegistrySerializer{T}"/> for key and value./>
        /// Has an affinity for Async serialization.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="services"></param>
        /// <param name="config"></param>
        /// <param name="configureAction"></param>
        /// <returns></returns>
        public static IServiceCollection AddKafkaProducer<TKey, TValue>(
            this IServiceCollection services,
            ProducerConfig config = null,
            Action<ProducerBuilderOptions<TKey, TValue>> configureAction = null)
        {
            services.AddSingleton(c =>
            {
                var builder = new ProducerBuilder<TKey, TValue>(config ?? c.GetRequiredService<ProducerConfig>());

                var valueSerializer = ResolveSerializer<TValue>(c);
                builder.SetValueSerializer(valueSerializer);

                if (TypeRequiresSerializer(typeof(TKey)))
                {
                    var keySerializer = ResolveSerializer<TKey>(c);

                    builder.SetKeySerializer(keySerializer);
                }

                if (configureAction != null)
                {
                    configureAction.Invoke(new ProducerBuilderOptions<TKey, TValue>(builder));
                }
                else
                {
                    builder.SetLogHandler((producer, message) => { });
                }

                return builder.Build();
            });

            return services;
        }

        /// <summary>
        /// Retrieves an instance of <see cref="T"/> from the configuration section specified.
        /// Defaults to the name of <see cref="T"/> as the configuration section name.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="config"></param>
        /// <param name="sectionName"></param>
        /// <returns></returns>
        public static T GetInstance<T>(this IConfiguration config, string sectionName = null)
            => config.GetSection(sectionName ?? typeof(T).Name).Get<T>();

        private static bool TypeRequiresSerializer(Type objectType) =>
            !(objectType.IsAssignableFrom(typeof(Ignore)) || objectType.IsAssignableFrom(typeof(Null)));

        private static ISerializer<T> ResolveSerializer<T>(IServiceProvider serviceProvider)
        {
            return serviceProvider.GetService<IAsyncSerializer<T>>()?.AsSyncOverAsync() ??
                   serviceProvider.GetService<ISerializer<T>>() ??
                   new AsyncSchemaRegistrySerializer<T>(
                       serviceProvider.GetService<ISchemaRegistryClient>()
                       ?? throw new InvalidOperationException($"Cannot resolve and instance of {nameof(ISchemaRegistryClient)} ensure one has been registered or a call to {nameof(AddSchemaRegistryClient)} has been made"))
                       .AsSyncOverAsync();
        }
    }
}
