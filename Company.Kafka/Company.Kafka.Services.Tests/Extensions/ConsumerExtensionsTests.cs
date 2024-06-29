using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Confluent.Kafka;
using FluentAssertions;
using NSubstitute;
using NUnit.Framework;

namespace Company.Kafka.Services.Tests.Extensions
{
    public class ConsumerExtensionsTests
    {
        private IConsumer<string, string> _testConsumer;

        [SetUp]
        public void SetUp()
        {
            _testConsumer = Substitute.For<IConsumer<string, string>>();
        }

        [Test]
        public void StoreOffsets_ReturnsDefault_WhenDefaultList()
        {
            // Act
            var result = _testConsumer.StoreBatchOffsets(default);

            // Assert
            result.Should().BeNull();
        }

        [Test]
        public void StoreOffsets_ReturnsEmptyList_WhenEmptyResultList()
        {
            // Act
            var result = _testConsumer.StoreBatchOffsets(new List<ConsumeResult<string, string>>(0));

            // Assert
            result.Should().BeEmpty();
        }

        [Test]
        public void StoreOffsets_StoresCorrectOffsets()
        {
            // Arrange
            var consumeResults = new List<ConsumeResult<string, string>>
            {
                new()
                {
                    Topic = "test",
                    Partition = new Partition(1),
                    Offset = new Offset(3),
                    Message = new Message<string, string>()
                },
                new()
                {
                    Topic = "test",
                    Partition = new Partition(1),
                    Offset = new Offset(1),
                    Message = new Message<string, string>()
                },
                new()
                {
                    Topic = "test",
                    Partition = new Partition(1),
                    Offset = new Offset(2),
                    Message = new Message<string, string>()
                },
                new()
                {
                    Topic = "test",
                    Partition = new Partition(3),
                    Offset = new Offset(55),
                    Message = new Message<string, string>()
                },
                new()
                {
                    Topic = "test",
                    Partition = new Partition(2),
                    Offset = new Offset(0),
                    Message = new Message<string, string>()
                },
                new()
                {
                    Topic = "test",
                    Partition = new Partition(2),
                    Offset = new Offset(100),
                    Message = new Message<string, string>()
                },
                new()
                {
                    Topic = "test",
                    Partition = new Partition(2),
                    Offset = new Offset(1),
                    Message = new Message<string, string>()
                },
            };

            // Act
            var result = _testConsumer.StoreBatchOffsets(consumeResults);

            // Assert
            result.Should().BeEquivalentTo(new[]
            {
                new TopicPartitionOffset("test", new Partition(1), new Offset(4)),
                new TopicPartitionOffset("test", new Partition(2), new Offset(101)),
                new TopicPartitionOffset("test", new Partition(3), new Offset(56))
            } );
        }
    }
}
