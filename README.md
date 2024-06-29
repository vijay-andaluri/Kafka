
# Overview

This module uses Chr.Avro and Confluent.Kafka nuget packages.  **The default implementations leverage Confluent SchemaRegistry**.  However, the `IProducerFactory` and `IConsumerFactory` interfaces are public and can be implemented with any choice of serialization via `ISerializer` and `IDeserializer`.  Reference this project's source along with Chr.Avro and Confluent.Kafka for more information.

## Contents
1) Add a Singleton Producer
2) Consumer Factory
3) Producer Factory
4) Service Base Classes

## 1) Add a Singleton Producer

In `ConfigureServices` call to `IHost`:

* Using Confluent Schema Registry Serdes

```cs
ConfigureServices((context, serviceCollection) =>
                {
                    var config = context.Configuration;
                    serviceCollection
                        .AddSchemaRegistryClient(config.GetInstance<SchemaRegistryConfig>())
                        .AddKafkaProducer<string, TestMessage>(config.GetInstance<ProducerConfig>());
                })
```
* Using Existing DI Registration

```cs
ConfigureServices((context, serviceCollection) =>
                {
                    var config = context.Configuration;
                    serviceCollection
                        .AddTransient<IAsyncSerializer<string>(new MyKeySerializer())
                        .AddTransient<IAsyncSerializer<TestMessage>(new MyValueSerializer())
                        .AddKafkaProducer<string, TestMessage>(config.GetInstance<ProducerConfig>());
                })
```

### Using the added Producer instance
```cs
public class MessageService
    {
        public MessageConsumerService(IProducer<string, TestMessage> produer)
        {
        ...
    }
```
### Remarks
  * The producer instance attempts to resolve `IAsyncSerializer<T>`, then `ISerializer<T>`, and then finally resorts to creating an instance of `AsyncSchemaRegistrySerializer<T>`.  This allows for use of various serializers by message type.

## 2) Consumer Factory
In `ConfigureServices` call to `IHost`:

* Using a Schema Registry client from DI that can be shared for better performance.

```cs
ConfigureServices((context, serviceCollection) =>
                {
                    var config = context.Configuration;
                    serviceCollection
                        .AddSchemaRegistryClient(config.GetInstance<SchemaRegistryConfig>())
                        .AddSchemaRegistryConsumerFactory();
                })
```

* Using a Schema Registry client created and owned by the factory instance

```cs
ConfigureServices((context, serviceCollection) =>
                {
                    var config = context.Configuration;
                    serviceCollection
                        .AddSchemaRegistryConsumerFactory(config.GetInstance<SchemaRegistryConfig>());
                })
```

### Using the IConsumerFactory
```cs
public class MessageConsumerService
    {
        private static readonly Random RandomGen = new Random();

        private readonly IConsumerFactory _consumerFactory;

        public MessageConsumerService(IConsumerFactory consumerFactory)
        {
            _messageLogger = loggerFactory.CreateLogger("Company.ConsumerMessageLogger");
        }

        public Task WaitAMinuteForAMessageAsync()
        {
            var cts = new CancellationTokenSource(60000);
            var myConfig = new ConsumerConfig();
            var myConsumer = _consumerFactory.GetConsumer<string, TestMessage>(
                myConfig,
                options =>
                {
                    options.SetCommittedOffsetsHandler()
                        .SetErrorHandler()
                        .SetLogHandler()
                        .SetPartitionsAssignedHandler();
                });
            var result = myConsumer.Consume(cts.Token);
        }
    }
```
### Remarks
  * The Consumer Factory implements `IDisposable` and will be disposed during the application host shutdown.
  * The `IConsumerFactory` interface can be freely implemented if the Schema Registry default is not sufficient.
  * The factory approach allows for validation of configurations in the context of a given class usage of the consumer as the configuration of a `ConsumerBuilder` or 'IConsumer' cannot be retrieved once created.
  * Each call results in a new consumer instance.  For best performance there should only be a single consumer of a given type to a given topic in a single application instance.  As such, caching is ill-advised.

## 3) Producer Factory
In `ConfigureServices` call to `IHost`:

* Using a Schema Registry client from DI that can be shared for better performance.

```cs
ConfigureServices((context, serviceCollection) =>
                {
                    var config = context.Configuration;
                    serviceCollection
                        .AddSchemaRegistryClient(config.GetInstance<SchemaRegistryConfig>())
                        .AddSchemaRegistryProducerFactory();
                })
```

* Using a Schema Registry client created and owned by the factory instance

```cs
ConfigureServices((context, serviceCollection) =>
                {
                    var config = context.Configuration;
                    serviceCollection
                        .AddSchemaRegistryProducerFactory(config.GetInstance<SchemaRegistryConfig>());
                })
```

### Using the IProducerFactory
```cs
public class MessageConsumerService
    {
        private static readonly Random RandomGen = new Random();

        private readonly IProducerFactory _producerFactory;

        public MessageService(IProducerFactory producerFactory)
        {
            _producerFactory = producerFactory;
        }

        public async Task WaitAMinuteForAMessageAsync()
        {
            var producer = producerFactory.GetProducer<string, TestMessage>(new ProducerConfig(), options =>
            {
                options.SetLogHandler()
                    .SetErrorHandler()
                    .SetStatisticsHandler();
            });

            await producer.ProduceAsync(...);
        }
    }
```
### Remarks
  * The default IProducerFactory implementation caches instances of `IProducer<TKey, TValue>`.
  * The Producer Factory implements `IDisposable` and will be disposed during the application host shutdown.

## 4) Service Base Classes
#### `ConsumerService<TKey, TValue>`
This is the most basic service designed to process messages one-at-a-time from the underlying Kafka consumer.  It is a simple solution for services without much message processing overhead or with a very low message volume.
 1. Inherit `ConsumerService<TKey,TValue>`
   ```cs
public MessageConsumerService(ILoggerFactory loggerFactory, IConsumerFactory consumerFactory, ConsumerInstanceSettings settings)
            : base(loggerFactory, consumerFactory, settings)
   ```
 2. Implement the async    message handler.  
 ```cs
protected abstract Task HandleMessageAsync(ConsumeResult<TKey, TValue> message, CancellationToken token = default)
{
}
 ```
 3.  Run service.`BaseConsumerService<TKey,TValue>` implements `IHostedService` so it can be added for automatic start and stop in an `IHost`.
```cs
IServiceCollection.AddHostedService<MessageConsumerService>()
```
#### `BatchConsumerService<TKey, TValue>`
This base service abstracts batching of Kafka messages.  This is ideal for situations where message processing should be done in sets.  A principal use case is for interacting with relational databases or constructing sets for service calls.  Batching in this class **DOES NOT** pre-fetch messages.  The underlying librdkafka library is already doing this an much more for performance.  Batching in this class **IS** meant to improve throughput in the consuming application.
 1. Inherit `BatchConsumerService<TKey,TValue>`
   ```cs
public MessageConsumerService(ILoggerFactory loggerFactory, IConsumerFactory consumerFactory, BatchConsumerSettings settings)
            : base(loggerFactory, consumerFactory, settings)
   ```
 2. Implement the async    message handler.  
 ```cs
protected override Task HandleMessageBatchAsync(List<ConsumeResult<string, TestMessage>> results, CancellationToken token)
        {
        }
 ```
 3.  Run service.`BatchConsumerService<TKey,TValue>` implements `IHostedService` so it can be added for automatic start and stop in an `IHost`.
```cs
IServiceCollection.AddHostedService<MessageConsumerService>()
```
#### `CacheConsumerService<TKey, TValue>`

There are many instances where the entirety of a data set - or just updates - are stored in a Kafka topic.  This base class uses partition EoF to load all existing messages on start up and then process new messages as they are received.  The initial loading of data can be grouped by an arbitrary key.  Additionally, loaded message and new messages can be ignored based on a predicate function given via the base constructor.

  1. Inherit `CacheConsumerService<TKey,TValue>`

   ```cs
public MyCacheConsumer(
            ILoggerFactory loggerFactory,
            IConsumerFactory consumerFactory,
            ConsumerInstanceSettings settings,
            Func<ConsumeResult<string, string>, bool> shouldIgnore = r => r.Message.Key == "something")
            : base(loggerFactory, consumerFactory, settings, shouldIgnore)
   ```

  2. Implement the key provider.  

 ```cs
protected override Func<string, MyKey, MyMessage> LoadMessageKeySelector => (k, v) => k.Name;
 ```

  3.  Implement the loaded message handler.
  ```cs
protected override Task OnMessagesLoadedAsync(Dictionary<string, ConsumeResult<MyKey, MyMessage>> valuesLoaded, CancellationToken token)
  ```
  4.  Implement the new message handler.
  ```cs
protected override Task OnNewMessageAsync(ConsumeResult<MyKey, MyMessage> result, CancellationToken token)
  ```
  5.  Run service.`CacheConsumerService<TKey,TValue>` implements `IHostedService` so it can be added for automatic start and stop in an `IHost`.

```cs
IServiceCollection.AddHostedService<MyCacheConsumer>()
```

## 

## Troubleshooting

Ensure you have registered a means of serialization if not using the SchemaRegistry extension above.

Ensure you have a commit strategy.  None of the service implementations automatically commit after abstract method completion.  As such, this should be handled as needed in your implementation.  Also, be aware that `IConsumer` can throw exceptions during commit and these should be handled accordingly.

Ensure your commit strategy and logging are consistent.  When auto-committing, the `BaseConsumerService` will log commit info.  However, manual commits do not trigger the logging handler.  If your service has a mixture of automatic and manual commits then only the automatic ones will surface through logging.

