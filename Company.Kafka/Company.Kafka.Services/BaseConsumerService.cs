using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Company.Kafka.Services.Configuration;
using Company.Kafka.Services.Factories.Interfaces;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Company.Kafka.Services
{
    public abstract class BaseConsumerService<TKey, TValue> : IHostedService, IDisposable
    {
        private readonly CancellationTokenSource _cancellationTokenSource;

        private readonly ILogger _syslogLogger;

        private readonly List<Task> _consumerTasks = new();

        private readonly ConsumerInstanceSettings _consumerInstanceSettings;

        private bool _disposed;

        protected BaseConsumerService(
            ILoggerFactory loggerFactory,
            IConsumerFactory consumerFactory,
            ConsumerInstanceSettings consumerInstanceSettings)
        {
            _cancellationTokenSource = new CancellationTokenSource();

            Logger = string.IsNullOrWhiteSpace(consumerInstanceSettings.LoggerName)
                ? loggerFactory.CreateLogger(GetType())
                : loggerFactory.CreateLogger(consumerInstanceSettings.LoggerName);
            _syslogLogger = string.IsNullOrWhiteSpace(consumerInstanceSettings.SysLoggerName)
                ? loggerFactory.CreateLogger($"{GetType().FullName}.KafkaSysLog")
                : loggerFactory.CreateLogger(consumerInstanceSettings.SysLoggerName);

            _consumerInstanceSettings = consumerInstanceSettings;

            Consumer = consumerFactory.GetConsumer<TKey, TValue>(consumerInstanceSettings, b =>
            {
                b.SetLogHandler(OnLogHandler)
                    .SetCommittedOffsetsHandler(OnCommittedOffsetsHandler)
                    .SetErrorHandler(OnErrorHandler);

                if (PartitionAssignmentHandler != default)
                {
                    b.SetPartitionsAssignedHandler(PartitionAssignmentHandler);
                }
                else
                {
                    b.SetPartitionsAssignedHandler(OnPartitionsAssigned);
                }
            });
        }

        protected IConsumer<TKey, TValue> Consumer { get; }

        protected ILogger Logger { get; }

        protected virtual Func<IConsumer<TKey, TValue>, List<TopicPartition>, IEnumerable<TopicPartitionOffset>> PartitionAssignmentHandler { get; }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            Logger.LogInformation($"Consumer started for topic: {_consumerInstanceSettings.TopicName}");
            Consumer.Subscribe(_consumerInstanceSettings.TopicName);
            _consumerTasks.Add(Task.Run(LoggingLoopAsync, cancellationToken));

            await OnStartingAsync(cancellationToken);
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            await OnStoppingAsync(cancellationToken);

            Logger.LogInformation("Stopping consumer");

            _cancellationTokenSource?.Cancel();

            await Task.WhenAll(_consumerTasks);

            Consumer.Unsubscribe();
        }

        public virtual void Dispose()
        {
            Logger.LogTrace("Begin disposing");
            Dispose(true);
            GC.SuppressFinalize(this);
            Logger.LogTrace("Complete disposing");
        }

        /// <summary>
        /// Task performed at the end of base service startup directly after consumer subscription to topic.
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        protected virtual Task OnStartingAsync(CancellationToken token)
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Task performed at the beginning of base service stopping.
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        protected virtual Task OnStoppingAsync(CancellationToken token)
        {
            return Task.CompletedTask;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                _cancellationTokenSource?.Dispose();
                Consumer?.Close();
                Consumer?.Dispose();
            }

            _disposed = true;
        }

        protected virtual void OnLogHandler(object sender, LogMessage logInfo) => _syslogLogger.WriteConsumerSyslogLogMessage(logInfo);

        protected virtual void OnCommittedOffsetsHandler(object sender, CommittedOffsets commit) => Logger.LogCommittedOffsets(commit);

        protected virtual void OnErrorHandler(object sender, Error error) => Logger.LogError($"Consumer error: {error.Code} - {error.Reason}");

        protected virtual void OnPartitionsAssigned(IConsumer<TKey, TValue> consumer, List<TopicPartition> assignments) => Logger.LogPartitionsAssigned(assignments);

        protected virtual void PartitionsRevokedHandler(IConsumer<TKey, TValue> consumer, List<TopicPartitionOffset> assignments)
        {
            // the assignments here are the current assignments PRIOR to revoking.
        }

        protected virtual void LogPosition() => Logger.LogConsumerPosition(Consumer);

        private async Task LoggingLoopAsync()
        {
            while (!_cancellationTokenSource.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(_consumerInstanceSettings.LoggingDelaySeconds), _cancellationTokenSource.Token);
                    LogPosition();
                }
                catch (TaskCanceledException)
                {
                    Logger.LogDebug("Logging task cancelled");
                }
                catch (Exception e)
                {
                    Logger.LogError(e, "Unhandled exception during periodic logging");
                }
            }

            Logger.LogDebug("Cancellation requested ending logging task");
        }
    }
}
