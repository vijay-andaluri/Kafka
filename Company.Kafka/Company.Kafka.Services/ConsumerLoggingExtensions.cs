using System;
using System.Collections.Generic;
using System.Linq;

using Confluent.Kafka;

using Microsoft.Extensions.Logging;

namespace Company.Kafka.Services
{
    public static class ConsumerLoggingExtensions
    {
        public static void WriteConsumerSyslogLogMessage(this ILogger logger, LogMessage logInfo)
        {
            var convertedLevel = ConvertSyslogLevel((int)logInfo.Level);

            if (logger.IsEnabled(convertedLevel))
            {
                var now = DateTime.UtcNow.ToString("u");

                logger.Log(convertedLevel, $"{now}|{logInfo.Name}|{convertedLevel}|{logInfo.Facility}|{logInfo.Message}");
            }
        }

        public static void LogPartitionsAssigned(this ILogger logger, List<TopicPartition> assignments)
        {
            var topicsAssignments = assignments.GroupBy(t => t.Topic);

            foreach (var topicPartition in topicsAssignments)
            {
                logger.LogInformation($"Partitions assigned in Topic '{topicPartition.Key}' [{string.Join(",", assignments.Select(t => t.Partition.Value))}]");
            }
        }

        public static void LogCommittedOffsets(this ILogger logger, CommittedOffsets commit)
        {
            var commitsGroupedByError = commit.Offsets
                .GroupBy(x => x.Error.IsError)
                .ToDictionary(g => g.Key);

            if (commitsGroupedByError.ContainsKey(true))
            {
                logger.LogError($"Failed to commit on: {GenerateCommitErrorMessage(commitsGroupedByError[true])}");
            }

            if (commitsGroupedByError.ContainsKey(false))
            {
                logger.LogInformation($"Successful commit on: {GeneratePositionLogMessage(commitsGroupedByError[false])}");
            }
        }

        public static void LogConsumerPosition<TKey, TValue>(this ILogger logger, IConsumer<TKey, TValue> consumer)
        {
            var partitionOffsets = consumer.Assignment?.Select(partition =>
            {
                var position = consumer.Position(partition);
                return new TopicPartitionOffsetError(partition, position, new Error(ErrorCode.NoError));
            }) ?? Enumerable.Empty<TopicPartitionOffsetError>();

            var message = partitionOffsets.Any()
                ? $"Current consumer assignment positions in topic: {GeneratePositionLogMessage(partitionOffsets)}"
                : "Consumer has no partition assignments";

            logger.LogInformation(message);
        }

        private static string GeneratePositionLogMessage(IEnumerable<TopicPartitionOffsetError> offsets) =>
            string.Join(",", offsets.Select(o => $"{o.Partition} - {o.Offset}"));

        private static string GenerateCommitErrorMessage(IEnumerable<TopicPartitionOffsetError> commitsWithErrors) =>
            string.Join(",", commitsWithErrors.Select(o => $"{o.Partition} - {o.Offset} - Error: {o.Error.Reason}"));

        private static LogLevel ConvertSyslogLevel(int level)
        {
            switch (level)
            {
                case 7:
                    return LogLevel.Debug;
                case 6:
                case 5:
                    return LogLevel.Information;
                case 4:
                    return LogLevel.Warning;
                case 3:
                    return LogLevel.Error;
                case 2:
                case 1:
                case 0:
                    return LogLevel.Critical;
                default:
                    return LogLevel.Error;
            }
        }
    }
}
