using Company.Kafka.Services.Configuration;

namespace Company.Kafka.TestHost.Configuration
{
    public class Consumers
    {
        public BatchConsumerSettings TestConsumer { get; set; }

        public ConsumerInstanceSettings BaseFanOutConsumer { get; set; }

        public ConsumerInstanceSettings CacheConsumerSettings { get; set; }
    }
}
