using System;
using System.Collections.Generic;

using Confluent.Kafka;

namespace Company.Kafka.Services.Factories
{
    public class ConsumerBuilderOptions<TKey, TValue>
    {
        private readonly ConsumerBuilder<TKey, TValue> _consumerBuilder;

        public ConsumerBuilderOptions(ConsumerBuilder<TKey, TValue> consumerBuilder)
            => _consumerBuilder = consumerBuilder;

        public virtual ConsumerBuilderOptions<TKey, TValue> SetLogHandler(Action<IConsumer<TKey, TValue>, LogMessage> logHandler)
        {
            _consumerBuilder.SetLogHandler(logHandler);
            return this;
        }

        public virtual ConsumerBuilderOptions<TKey, TValue> SetErrorHandler(Action<IConsumer<TKey, TValue>, Error> errorHandler)
        {
            _consumerBuilder.SetErrorHandler(errorHandler);
            return this;
        }

        public virtual ConsumerBuilderOptions<TKey, TValue> SetCommittedOffsetsHandler(Action<IConsumer<TKey, TValue>, CommittedOffsets> committedOffsetsHandler)
        {
            _consumerBuilder.SetOffsetsCommittedHandler(committedOffsetsHandler);
            return this;
        }

        public virtual ConsumerBuilderOptions<TKey, TValue> SetPartitionsAssignedHandler(Action<IConsumer<TKey, TValue>, List<TopicPartition>> partitionsAssignedHandler)
        {
            _consumerBuilder.SetPartitionsAssignedHandler(partitionsAssignedHandler);
            return this;
        }

        public virtual ConsumerBuilderOptions<TKey, TValue> SetPartitionsAssignedHandler(Func<IConsumer<TKey, TValue>, List<TopicPartition>, IEnumerable<TopicPartitionOffset>> partitionsAssignedHandler)
        {
            _consumerBuilder.SetPartitionsAssignedHandler(partitionsAssignedHandler);
            return this;
        }
    }
}
