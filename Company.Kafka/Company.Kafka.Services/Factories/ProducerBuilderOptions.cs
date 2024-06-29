using System;

using Confluent.Kafka;

namespace Company.Kafka.Services.Factories
{
    public class ProducerBuilderOptions<TKey, TValue>
    {
        private readonly ProducerBuilder<TKey, TValue> _producerBuilder;

        public ProducerBuilderOptions(ProducerBuilder<TKey, TValue> consumerBuilder)
            => _producerBuilder = consumerBuilder;

        public ProducerBuilderOptions<TKey, TValue> SetLogHandler(Action<IProducer<TKey, TValue>, LogMessage> logHandler)
        {
            _producerBuilder.SetLogHandler(logHandler);
            return this;
        }

        public ProducerBuilderOptions<TKey, TValue> SetErrorHandler(Action<IProducer<TKey, TValue>, Error> errorHandler)
        {
            _producerBuilder.SetErrorHandler(errorHandler);
            return this;
        }

        public ProducerBuilderOptions<TKey, TValue> SetStatisticsHandler(Action<IProducer<TKey, TValue>, string> statisticsHandler)
        {
            _producerBuilder.SetStatisticsHandler(statisticsHandler);
            return this;
        }
    }
}
