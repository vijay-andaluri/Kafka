using System.Collections.Generic;
using System.Linq;

using Confluent.Kafka;

namespace Company.Kafka.Services
{
    public static class ConsumerExtensions
    {
        // Stores latest offsets for a given batch of messages to be committed.
        public static List<TopicPartitionOffset> StoreBatchOffsets<TKey, TValue>(this IConsumer<TKey, TValue> consumer, List<ConsumeResult<TKey, TValue>> messageBatch)
        {
            if (messageBatch == default)
            {
                return default;
            }

            if (!messageBatch.Any())
            {
                return new List<TopicPartitionOffset>(0);
            }

            var topic = messageBatch.First().Topic;
            var commitOffsets = messageBatch.GroupBy(r => r.Partition).Select(g =>
                new TopicPartitionOffset(topic, g.Key, g.Max(r => r.Offset.Value + 1))).ToList();

            commitOffsets.ForEach(consumer.StoreOffset);

            return commitOffsets;
        }
    }
}
