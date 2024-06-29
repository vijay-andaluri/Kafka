using System;

using Company.Kafka.TestHost.Enums;

namespace Company.Kafka.TestHost.Messages
{
    public class TestMessage
    {
        public DateTime CreatedAt { get; set; }

        public DateTimeOffset CreatedAtOffset { get; set; }

        public ThingType ThingType { get; set; }

        public decimal Amount { get; set; }

        public string Name { get; set; }

        public byte Thing { get; set; }

        public bool IsActive { get; set; }

        public double Radius { get; set; }

        public short SmallNumber { get; set; }
    }
}
