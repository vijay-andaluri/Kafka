using System;
using System.Collections.Concurrent;
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
    public abstract class CacheConsumerService<TMessageKey, TMessage, TCacheKey> : BaseConsumerService<TMessageKey, TMessage>
    {
        private readonly string _serviceName = $"{typeof(BaseConsumerService<TMessageKey, TMessage>).Name.Replace("`2", string.Empty)}<{typeof(TMessageKey).Name}, {typeof(TMessage).Name}>";

        private readonly Func<ConsumeResult<TMessageKey, TMessage>, bool> _ignoreMessagePredicate;

        private readonly CancellationTokenSource _cancellationTokenSource;

        private readonly ISyncPolicy<ConsumeResult<TMessageKey, TMessage>> _consumeRetryPolicy;

        private readonly ConcurrentDictionary<int, long> _offsets = new();

        private ConcurrentDictionary<TCacheKey, ConsumeResult<TMessageKey, TMessage>> _cache =
            new();

        private Task _consumerTask = Task.CompletedTask;

        private Task _loadTopicTask = Task.CompletedTask;

        protected CacheConsumerService(ILoggerFactory loggerFactory, IConsumerFactory consumerFactory, ConsumerInstanceSettings consumerInstanceSettings, Func<ConsumeResult<TMessageKey, TMessage>, bool> ignoreMessagePredicate = default)
            : base(loggerFactory, consumerFactory, consumerInstanceSettings)
        {
            ConsumerInstanceSettings = consumerInstanceSettings;
            _consumeRetryPolicy = BuildConsumeRetryPolicy();
            _ignoreMessagePredicate = ignoreMessagePredicate ?? (r => false);
            var validEofSetting = consumerInstanceSettings.EnablePartitionEof
                                  ?? throw new ArgumentException("Partition EOF messages must be enabled", nameof(consumerInstanceSettings.EnablePartitionEof));
            _cancellationTokenSource = new CancellationTokenSource();
        }

        protected ConsumerInstanceSettings ConsumerInstanceSettings { get; }

        /// <summary>
        /// Determines key to be used when loading initial cache data on startup.
        /// </summary>
        protected abstract Func<TMessageKey, TMessage, TCacheKey> LoadMessageKeySelector { get; }

        /// <summary>
        /// Override to ensure that on rebalance or other reassignment the locally stored offsets are used since commits are not made for this consumer.
        /// </summary>
        protected override Func<IConsumer<TMessageKey, TMessage>, List<TopicPartition>, IEnumerable<TopicPartitionOffset>> PartitionAssignmentHandler =>
        (_, partitions) =>
        {
            var assignment = partitions.Select(p => new TopicPartitionOffset(
                p.Topic,
                p.Partition,
                _offsets.TryGetValue(p.Partition.Value, out var offset) ? offset : Offset.Beginning)).ToList();

            var latestOffsets = assignment.Select(t => $"({t.Partition}, {t.Offset})");
            Logger.LogInformation($"Reassigning consumer using latest processed offsets: [{string.Join(",", latestOffsets)}]");
            return assignment;
        };

        /// <summary>
        /// Called when the end of all assigned partitions is reached on initial start.
        /// </summary>
        /// <param name="valuesLoaded">Values loaded from topic grouped by key with the latest message as the corresponding value</param>
        /// <param name="token"></param>
        /// <returns></returns>
        protected abstract Task OnMessagesLoadedAsync(Dictionary<TCacheKey, ConsumeResult<TMessageKey, TMessage>> valuesLoaded, CancellationToken token);

        /// <summary>
        /// Called after initial startup loading has completed and new messages are received.
        /// </summary>
        /// <param name="result"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        protected abstract Task OnNewMessageAsync(ConsumeResult<TMessageKey, TMessage> result, CancellationToken token);

        protected sealed override async Task OnStartingAsync(CancellationToken token)
        {
            _loadTopicTask = Task.Run(() => LoadTopicMessagesAsync(_cancellationTokenSource.Token), token).ContinueWith(
                _ =>
                {
                    _consumerTask = Task.Run(() => ConsumeLoopAsync(_cancellationTokenSource.Token), token);
                },
                token);

            await _loadTopicTask;
        }

        protected sealed override async Task OnStoppingAsync(CancellationToken token)
        {
            Logger.LogTrace("Stopping cache consumer.");
            _cancellationTokenSource.Cancel();
            await _consumerTask;
        }

        private async Task LoadTopicMessagesAsync(CancellationToken token)
        {
            var assignedPartitions = new List<int>();
            var cacheSizeOnLoad = 0;
            var timer = Stopwatch.StartNew();
            var isFirstMessage = true; // instead of waiting for assignments just wait for first message and then load assigned partitions
            var topicNotKeyed = typeof(TMessageKey) == typeof(Null) || typeof(TMessageKey) == typeof(Ignore);

            if (topicNotKeyed)
            {
                Logger.LogWarning($"Topic: '{ConsumerInstanceSettings.TopicName}' does not have a key type specified.  Message timestamp will be used to preserve order.");
            }

            do
            {
                var result = SafeConsume(Consumer, token);

                if (isFirstMessage)
                {
                    isFirstMessage = false;
                    assignedPartitions.AddRange(Consumer.Assignment.Select(p => p.Partition.Value));
                    Logger.LogTrace($"Loaded partitions [{string.Join(",", assignedPartitions)}]");
                }

                if (result.IsPartitionEOF)
                {
                    Logger.LogTrace($"Reached the end of Partition: {result.Partition.Value}");
                    assignedPartitions.Remove(result.Partition.Value);
                    continue;
                }

                if (_ignoreMessagePredicate(result))
                {
                    Logger.LogTrace($"Ignoring message Partition: {result.Partition.Value} Offset: {result.Offset.Value}");
                    continue;
                }

                cacheSizeOnLoad++;
                var key = LoadMessageKeySelector(result.Message.Key, result.Message.Value);
                _cache.AddOrUpdate(
                    key,
                    result,
                    (k, m) =>
                        topicNotKeyed && result.Message.Timestamp.UnixTimestampMs <= m.Message.Timestamp.UnixTimestampMs
                            ? m
                            : result);
                _offsets[result.Partition.Value] = result.Offset.Value + 1;
            }
            while (!token.IsCancellationRequested && assignedPartitions.Any());

            Logger.LogTrace($"Loaded {cacheSizeOnLoad} messages from all assigned partitions. {timer.ElapsedMilliseconds}ms elapsed.");

            await OnMessagesLoadedAsync(_cache.ToDictionary(k => k.Key, v => v.Value), _cancellationTokenSource.Token);
            _cache = null;
        }

        private async Task ConsumeLoopAsync(CancellationToken serviceToken)
        {
            Logger.LogInformation("Begin polling for new cache entries.");

            while (!serviceToken.IsCancellationRequested)
            {
                try
                {
                    var nextResult = SafeConsume(Consumer, serviceToken);

                    if (nextResult.IsPartitionEOF)
                    {
                        Logger.LogTrace($"Reached the end of partition {nextResult.Partition.Value}");
                        continue;
                    }

                    if (_ignoreMessagePredicate(nextResult))
                    {
                        Logger.LogTrace($"Ignoring message Partition: {nextResult.Partition.Value} Offset: {nextResult.Offset.Value}");
                        continue;
                    }

                    await OnNewMessageAsync(nextResult, serviceToken);
                    _offsets[nextResult.Partition.Value] = nextResult.Offset.Value + 1;
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

        /// <summary>
        /// Allows only <see cref="OperationCanceledException"/> to be thrown.
        /// </summary>
        /// <param name="consumer"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        private ConsumeResult<TMessageKey, TMessage> SafeConsume(IConsumer<TMessageKey, TMessage> consumer, CancellationToken token)
        {
            var executionContext = new Context(_serviceName);
            return _consumeRetryPolicy.Execute((context, t) => consumer.Consume(t), executionContext, token);
        }

        private ISyncPolicy<ConsumeResult<TMessageKey, TMessage>> BuildConsumeRetryPolicy()
        {
            return Policy<ConsumeResult<TMessageKey, TMessage>>
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
