namespace Company.Kafka.Services.Configuration
{
    public class BatchConsumerSettings : ConsumerInstanceSettings
    {
        /// <summary>
        /// Maximum number of message allowed in a batch.  Must be greater than 1.
        /// </summary>
        public int MaxBatchSize { get; set; }

        /// <summary>
        /// Maximum total time in milliseconds to wait for a batch.  This is NOT a sliding wait time.  Must be greater than 0.
        /// </summary>
        public int MaxBatchWaitMilliseconds { get; set; }
    }
}
