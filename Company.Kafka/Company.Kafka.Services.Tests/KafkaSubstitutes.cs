using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc.TagHelpers.Cache;
using NSubstitute;

namespace Company.Kafka.Services.Tests
{
    public static class KafkaSubstitutes
    {
        public static ConsumerConfig FakeConsumerConfig =>
            new ConsumerConfig
            {
                ClientId = "testservice",
                GroupId = "testservice",
                BootstrapServers = "fake.cl.local"
            };

        public static ProducerConfig FakeProducerConfig =>
            new ProducerConfig
            {
                ClientId = "testservice",
                BootstrapServers = "fake.cl.local"
            };

        public static void ReturnAndSignalStop<TKey, TValue>(this IConsumer<TKey, TValue> consumer, List<Message<TKey, TValue>> messagesToReturn, CancellationTokenSource tokenSource, bool sendPartitionEoF = false)
        {
            var returnedCount = 0;
            consumer.Consume(Arg.Any<CancellationToken>())
                .Returns(x =>
                {
                    if (returnedCount < messagesToReturn.Count)
                    {
                        return CreateResultFromMessage(messagesToReturn[returnedCount++], returnedCount);
                    }

                    if (returnedCount == messagesToReturn.Count && sendPartitionEoF)
                    {
                        returnedCount++;
                        return CreateResultFromMessage(default(Message<TKey, TValue>), returnedCount + 1, 0, true);
                    }

                    tokenSource.Cancel();
                    var token = (CancellationToken)x[0];
                    token.WaitHandle.WaitOne();
                    token.ThrowIfCancellationRequested();

                    return CreateResultFromMessage(messagesToReturn[returnedCount++], returnedCount);
                });
        }

        public static void ReturnAndSignalStop<TKey, TValue>(this IConsumer<TKey, TValue> consumer, List<ConsumeResult<TKey, TValue>> results, CancellationTokenSource tokenSource, bool sendPartitionEoF = false)
        {
            var returnedCount = 0;
            consumer.Consume(Arg.Any<CancellationToken>())
                .Returns(x =>
                {
                    if (returnedCount < results.Count)
                    {
                        return results[returnedCount++];
                    }

                    if (returnedCount == results.Count && sendPartitionEoF)
                    {
                        returnedCount++;
                        return CreateResultFromMessage(default(Message<TKey, TValue>), returnedCount + 1, 0, true);
                    }

                    tokenSource.Cancel();
                    var token = (CancellationToken)x[0];
                    token.WaitHandle.WaitOne();
                    token.ThrowIfCancellationRequested();

                    return results[returnedCount++];
                });
        }

        public static void ReturnAndWait<TKey, TValue>(this IConsumer<TKey, TValue> consumer, List<Message<TKey, TValue>> messagesToReturn, bool sendPartitionEoF = false)
        {
            var returnedCount = 0;
            consumer.Consume(Arg.Any<CancellationToken>())
                .Returns(x =>
                {
                    if (returnedCount < messagesToReturn.Count)
                    {
                        return CreateResultFromMessage(messagesToReturn[returnedCount++], returnedCount);
                    }

                    if (returnedCount == messagesToReturn.Count && sendPartitionEoF)
                    {
                        returnedCount++;
                        return CreateResultFromMessage(default(Message<TKey, TValue>), returnedCount + 1, 0, true);
                    }

                    var token = (CancellationToken)x[0];
                    token.WaitHandle.WaitOne();
                    token.ThrowIfCancellationRequested();

                    return CreateResultFromMessage(messagesToReturn[returnedCount++], returnedCount);
                });
        }

        public static void WaitForCancellation<TKey, TValue>(this IConsumer<TKey, TValue> consumer, bool sendEoF = false)
        {
            consumer.Consume(Arg.Any<CancellationToken>())
                .Returns(x =>
                {
                    if (sendEoF)
                    {
                        return CreateResultFromMessage<TKey, TValue>(default, 0, 0, true);
                    }

                    var token = (CancellationToken)x[0];
                    token.WaitHandle.WaitOne();
                    token.ThrowIfCancellationRequested();

                    return default;
                });
        }

        public static IConsumer<TKey, TValue> SetAssignments<TKey, TValue>(this IConsumer<TKey, TValue> consumer, params int[] partitions)
        {
            consumer.Assignment.Returns(partitions.Select(p => new TopicPartition("test", p)).ToList());
            return consumer;
        }

        public static ConsumeResult<TKey, TRecord> CreateResultFromMessage<TKey, TRecord>(Message<TKey, TRecord> message, int offset, int partition = 0, bool isEof = false) =>
        new ConsumeResult<TKey, TRecord>
        {
            Offset = offset,
            Topic = "test",
            Partition = partition,
            IsPartitionEOF = isEof,
            Message = message,
        };

        public static ConsumeResult<TKey, TRecord> CreateResult<TKey, TRecord>(TKey key, TRecord value, int offset, int partition = 0, bool isEof = false) =>
            new ConsumeResult<TKey, TRecord>
            {
                Offset = offset,
                Topic = "test",
                Partition = partition,
                IsPartitionEOF = isEof,
                Message = CreateMessage(key, value)
            };

        public static Message<TKey, TValue> CreateMessage<TKey, TValue>(TKey key, TValue value)
        {
            return new Message<TKey, TValue>
            {
                Key = key,
                Value = value,
                Timestamp = new Timestamp(DateTimeOffset.Now)
            };
        }
    }
}
