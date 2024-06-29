using Confluent.Kafka;

namespace Company.Kafka.Services.Configuration
{
    public class ConsumerInstanceSettings : ConsumerConfig
    {
        /// <summary>
        /// The topic to which the consumer will subscribe
        /// </summary>
        public string TopicName { get; set; }

        /// <summary>
        /// Number of seconds to delay between logging consumer current position in the subscribed <see cref="TopicName"/>
        /// </summary>
        public int LoggingDelaySeconds { get; set; } = 1800;

        /// <summary>
        /// Max back off time delay when an unhandled exception is thrown during attempting to consume messages. The max is reached in an exponential fashion base 2.
        /// </summary>
        public int ConsumerErrorMaxBackOffSeconds { get; set; } = 600;

        /// <summary>
        /// Sets the logger name for the service implementation.  Default is the qualified implementation class name.
        /// </summary>
        public string LoggerName { get; set; }

        /// <summary>
        /// Sets the logger name for syslog messages from the librdkafka binaries.  Default is 'KafkaConsumerLogger'.
        /// </summary>
        public string SysLoggerName { get; set; }
    }
}
