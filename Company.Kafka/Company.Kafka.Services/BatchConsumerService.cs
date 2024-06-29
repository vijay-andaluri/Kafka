using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Company.Kafka.Services.Configuration;
using Company.Kafka.Services.Factories.Interfaces;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Polly;

namespace Company.Kafka.Services
{
    public abstract class BatchConsumerService<TKey, TValue> : BaseConsumerService<TKey, TValue>
    {
        private readonly string _serviceName = $"{typeof(BaseConsumerService<TKey, TValue>).Name.Replace("`2", string.Empty)}<{typeof(TKey).Name}, {typeof(TValue).Name}>";

        private readonly CancellationTokenSource _cancellationTokenSource = new();

        private readonly ISyncPolicy<ConsumeResult<TKey, TValue>> _consumeRetryPolicy;

        private Task _consumerTask = Task.CompletedTask;

        protected BatchConsumerService(ILoggerFactory loggerFactory, IConsumerFactory consumerFactory, BatchConsumerSettings consumerInstanceSettings)
            : base(loggerFactory, consumerFactory, consumerInstanceSettings)
        {
            ConsumerInstanceSettings = consumerInstanceSettings;
            _consumeRetryPolicy = BuildConsumeRetryPolicy();
        }

        protected BatchConsumerSettings ConsumerInstanceSettings { get; }

        protected sealed override Task OnStartingAsync(CancellationToken token)
        {
            Logger.LogDebug($"Performing {nameof(OnStartingAsync)} for {nameof(ConsumerService<TKey, TValue>)}.");
            _consumerTask = Task.Run(() => ConsumeLoopAsync(_cancellationTokenSource.Token), token);
            return Task.CompletedTask;
        }

        protected sealed override async Task OnStoppingAsync(CancellationToken token)
        {
            Logger.LogDebug($"Performing {nameof(OnStoppingAsync)} for {nameof(ConsumerService<TKey, TValue>)}.");
            _cancellationTokenSource.Cancel();
            await _consumerTask;
        }

        protected abstract Task HandleMessageBatchAsync(List<ConsumeResult<TKey, TValue>> results, CancellationToken token);

        private async Task ConsumeLoopAsync(CancellationToken serviceToken)
        {
            Logger.LogInformation("Begin consumer polling.");

            while (!serviceToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResults = WaitForNextBatch(serviceToken);

                    if (consumeResults.Any())
                    {
                        await HandleMessageBatchAsync(consumeResults, serviceToken);
                    }
                }
                catch (OperationCanceledException)
                {
                    Logger.LogDebug("Cancelled consumer polling");
                }
                catch (Exception e)
                {
                    Logger.LogError(e, "Consumer exception in hosted service");
                }
            }

            Logger.LogDebug("Consumer polling complete.");
        }

        private List<ConsumeResult<TKey, TValue>> WaitForNextBatch(CancellationToken serviceToken)
        {
            var timer = Stopwatch.StartNew();

            if (IsSingleMessageMode())
            {
                Logger.LogTrace("Executing consumer polling in single message mode.");
                var result = new[] { SafeConsume(Consumer, serviceToken) }.ToList();

                timer.Stop();
                Logger.LogDebug($"Returning single message.  {timer.ElapsedMilliseconds}ms");

                return result;
            }

            var results = new List<ConsumeResult<TKey, TValue>>();
            var waitTokenSource = new CancellationTokenSource();
            var cancelWhenServiceStops = serviceToken.Register(() =>
            {
                // ReSharper disable once AccessToDisposedClosure
                // this delegate is un-registered before disposal in the outer scope
                waitTokenSource.Cancel();
            });
            Logger.LogTrace("Begin polling for next batch of messages.");

            try
            {
                waitTokenSource.CancelAfter(ConsumerInstanceSettings.MaxBatchWaitMilliseconds);
                while (results.Count < ConsumerInstanceSettings.MaxBatchSize && !waitTokenSource.IsCancellationRequested)
                {
                    results.Add(SafeConsume(Consumer, waitTokenSource.Token));
                }
            }
            catch (OperationCanceledException)
            {
                Logger.LogDebug($"Wait for batch canceled.  Service Stopped: {serviceToken.IsCancellationRequested}");
            }
            finally
            {
                timer.Stop();
                cancelWhenServiceStops.Dispose(); // Unregisters and disposes. In later net versions the Unregister call should be used.
                waitTokenSource.Dispose();
            }

            if (Logger.IsEnabled(LogLevel.Trace))
            {
                Logger.LogTrace($"Returning {results.Count} results.  Batch wait canceled: {waitTokenSource.IsCancellationRequested}.  Service canceled: {serviceToken.IsCancellationRequested}.  Elapsed {timer.ElapsedMilliseconds} ms");
            }

            return results;
        }

        private bool IsSingleMessageMode()
        {
            return ConsumerInstanceSettings.MaxBatchWaitMilliseconds == 0 && ConsumerInstanceSettings.MaxBatchSize == 1;
        }

        /// <summary>
        /// Allows only <see cref="OperationCanceledException"/> to be thrown.
        /// </summary>
        /// <param name="consumer"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        private ConsumeResult<TKey, TValue> SafeConsume(IConsumer<TKey, TValue> consumer, CancellationToken token)
        {
            var executionContext = new Context(_serviceName);
            return _consumeRetryPolicy.Execute((context, t) => consumer.Consume(t), executionContext, token);
        }

        private ISyncPolicy<ConsumeResult<TKey, TValue>> BuildConsumeRetryPolicy()
        {
            return Policy<ConsumeResult<TKey, TValue>>
                .Handle<Exception>(e => !(e is OperationCanceledException))
                .WaitAndRetryForever(
                    (i, context) => TimeSpan.FromSeconds(Math.Min(Math.Pow(2, i), ConsumerInstanceSettings.ConsumerErrorMaxBackOffSeconds)), // this is mostly to expose broker issues.  Consumer exceptions with correct settings should be rare
                    (e, i, t, c) =>
                    {
                        var messageDetails = string.Empty;

                        if (e.Exception is ConsumeException ce)
                        {
                            OnErrorHandler(ce, ce.Error);  // This will call the overriden method in the consuming app in case of exceptions.
                            var topic = ce.ConsumerRecord.Topic;
                            var partition = ce.ConsumerRecord.Partition;
                            var offset = ce.ConsumerRecord.Offset;
                            var errorMessage = $"{ce.Message} - {ce.StackTrace}";

                            messageDetails = $"Topic: {topic} Partition: {partition} Offset: {offset} Error: {errorMessage}. ";
                        }

                        Logger.LogError(e.Exception, $"Consumer BackOff {i} for Operation: {c.OperationKey} CorrelationId: {c.CorrelationId}. {messageDetails}Waiting {t} to retry operation.");
                    });
        }
    }
}
