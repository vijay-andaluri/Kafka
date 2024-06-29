using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Company.AspNetCoreTools.TestHelpers.Logging;
using Company.Kafka.Services.Configuration;
using Company.Kafka.Services.Factories;
using Company.Kafka.Services.Factories.Interfaces;
using Company.Kafka.Services.Tests.TestClasses;

using Confluent.Kafka;

using FluentAssertions;

using Microsoft.Extensions.Logging;

using NSubstitute;

using NUnit.Framework;
using NUnit.Framework.Internal;

namespace Company.Kafka.Services.Tests
{
    public class CacheServiceTests
    {
        private TestCacheConsumer<string> _testConsumer;

        private CancellationTokenSource _testServiceTokenSource;

        private IConsumerFactory _consumerFactory;

        private IConsumer<string, string> _consumer;

        private IConsumer<Ignore, string> _notKeyedConsumer;

        private ConsumerInstanceSettings _consumerInstanceSettings;

        private ILoggerFactory _loggerFactory;

        private JsonLoggingFixture _loggingFixture;

        private List<TopicPartition> _assignment;

        private Action<ConsumerBuilderOptions<string, string>> _consumerSetupAction;

        private ConsumerBuilderOptions<string, string> _consumerSetup;

        [SetUp]
        public void Setup()
        {
            _consumerInstanceSettings = new ConsumerInstanceSettings
            {
                TopicName = "test_topic",
                LoggingDelaySeconds = 1,
                EnablePartitionEof = true
            };
            _assignment = new List<TopicPartition> { new("test_topic", new Partition(0)) };
            var config = new ConsumerConfig { BootstrapServers = "test" };
            _consumerFactory = Substitute.For<IConsumerFactory>();
            _loggingFixture = new JsonLoggingFixture();
            _loggerFactory = Substitute.For<ILoggerFactory>();
            _loggerFactory.CreateLogger(Arg.Any<string>()).Returns(_loggingFixture.GetLogger<TestConsumer>());
            _consumer = Substitute.For<IConsumer<string, string>>();
            _consumer.Assignment.Returns(_assignment);
            _consumerFactory.GetConsumer(
                    Arg.Any<ConsumerConfig>(),
                    Arg.Do<Action<ConsumerBuilderOptions<string, string>>>(a => _consumerSetupAction = a))
                .Returns(_consumer);

            _testServiceTokenSource = new CancellationTokenSource();
            _testConsumer = new TestCacheConsumer<string>(_loggerFactory, _consumerFactory, _consumerInstanceSettings);
        }

        [Test]
        public async Task OnLoading_GroupsMessagesCorrectly()
        {
            // Arrange
            var firstKey = "1";
            var secondKey = "2";
            Dictionary<string, ConsumeResult<string, string>> processedResults = default;

            var consumeResults = new List<ConsumeResult<string, string>>
            {
                KafkaSubstitutes.CreateResult(firstKey, "first_message_1", 0),
                KafkaSubstitutes.CreateResult(secondKey, "first_message_2", 1),
                KafkaSubstitutes.CreateResult(firstKey, "second_message_1", 2),
                KafkaSubstitutes.CreateResult(secondKey, "second_message_2", 3)
            };

            _testConsumer.OnLoadingAction = m =>
            {
                processedResults = m;
                _testServiceTokenSource.Cancel();
            };
            _consumer.ReturnAndSignalStop(consumeResults, _testServiceTokenSource, true);

            // Act
            await _testConsumer.StartAsync(default);
            _testServiceTokenSource.Token.WaitHandle.WaitOne();
            await _testConsumer.StopAsync(default);

            // Assert
            processedResults.Should().NotBeNull();
            processedResults.Should().ContainKey(firstKey).WhoseValue.Message.Value.Should().Be("second_message_1");
            processedResults.Should().ContainKey(secondKey).WhoseValue.Message.Value.Should().Be("second_message_2");
        }

        [Test]
        public async Task OnLoading_KeepsLatestMessageByTimestamp_WhenNotKeyed()
        {
            // Arrange
            _notKeyedConsumer = Substitute.For<IConsumer<Ignore, string>>();
            _notKeyedConsumer.Assignment.Returns(_assignment);
            Dictionary<string, ConsumeResult<Ignore, string>> processedResults = default;

            _consumerFactory.GetConsumer(
                    Arg.Any<ConsumerConfig>(),
                    Arg.Any<Action<ConsumerBuilderOptions<Ignore, string>>>())
                .ReturnsForAnyArgs(c => _notKeyedConsumer);

            var notKeyedService = new TestCacheConsumer<Ignore>(_loggerFactory, _consumerFactory, _consumerInstanceSettings);

            var consumeResults = new List<ConsumeResult<Ignore, string>>
            {
                KafkaSubstitutes.CreateResult<Ignore, string>(default, "latest_by_timestamp", 0, 0, false),
                KafkaSubstitutes.CreateResult<Ignore, string>(default, "last_received", 1, 0, false),
            };

            // make them out of order
            var time = DateTimeOffset.Now;
            consumeResults[1].Message.Timestamp = new Timestamp(time.AddMilliseconds(-1));
            consumeResults[0].Message.Timestamp = new Timestamp(time);

            notKeyedService.OnLoadingAction = m =>
            {
                processedResults = m;
                _testServiceTokenSource.Cancel();
            };
            _notKeyedConsumer.ReturnAndSignalStop(consumeResults, _testServiceTokenSource, true);

            // Act
            await notKeyedService.StartAsync(default);
            _testServiceTokenSource.Token.WaitHandle.WaitOne();
            await notKeyedService.StopAsync(default);

            // Assert
            processedResults.Should().HaveCount(1);
            processedResults.Should().ContainKey("ignored").WhoseValue.Message.Value.Should().Be("latest_by_timestamp");
        }

        [Test]
        public async Task OnLoading_SkipsMessage_BasedOnFilter()
        {
            // Arrange
            var firstKey = "1";
            var secondKey = "2";
            Dictionary<string, ConsumeResult<string, string>> processedResults = default;
            _testConsumer = new TestCacheConsumer<string>(_loggerFactory, _consumerFactory, _consumerInstanceSettings, r => r.Offset.Value == 3);
            var consumeResults = new List<ConsumeResult<string, string>>
            {
                KafkaSubstitutes.CreateResult(firstKey, "first_message_1", 0),
                KafkaSubstitutes.CreateResult(secondKey, "first_message_2", 1),
                KafkaSubstitutes.CreateResult(firstKey, "second_message_1", 2),
                KafkaSubstitutes.CreateResult(secondKey, "second_message_2", 3)
            };

            _testConsumer.OnLoadingAction = m =>
            {
                processedResults = m;
                _testServiceTokenSource.Cancel();
            };
            _consumer.ReturnAndSignalStop(consumeResults, _testServiceTokenSource, true);

            // Act
            await _testConsumer.StartAsync(default);
            _testServiceTokenSource.Token.WaitHandle.WaitOne();
            await _testConsumer.StopAsync(default);

            // Assert
            processedResults.Should().NotBeNull();
            processedResults.Should().ContainKey(firstKey).WhoseValue.Message.Value.Should().Be("second_message_1");
            processedResults.Should().ContainKey(secondKey).WhoseValue.Message.Value.Should().Be("first_message_2");
        }

        [Test]
        public async Task OnLoading_IgnoresNewMessages_BasedOnFilter()
        {
            // Arrange
            var firstKey = "1";
            var secondKey = "2";
            var loadingRan = false;
            Dictionary<string, ConsumeResult<string, string>> loadedMessages = default;
            _testConsumer = new TestCacheConsumer<string>(_loggerFactory, _consumerFactory, _consumerInstanceSettings, r => r.Offset.Value == 6);
            var newMessages = new List<ConsumeResult<string, string>>();
            var consumeResults = new List<ConsumeResult<string, string>>
            {
                KafkaSubstitutes.CreateResult(firstKey, "first_message_1", 0),
                KafkaSubstitutes.CreateResult(secondKey, "first_message_2", 1),
                KafkaSubstitutes.CreateResult(firstKey, "second_message_1", 2),
                KafkaSubstitutes.CreateResult(secondKey, "second_message_2", 3),
                KafkaSubstitutes.CreateResult(string.Empty, string.Empty, 4, 0, true), // mark the end of existing messages
                KafkaSubstitutes.CreateResult(firstKey, "new_message_1", 5),
                KafkaSubstitutes.CreateResult(secondKey, "new_message_2", 6),
                KafkaSubstitutes.CreateResult(string.Empty, string.Empty, 4, 0, true)
            };

            _testConsumer.OnLoadingAction = d =>
            {
                loadedMessages = d;
                loadingRan = true;
            };

            _testConsumer.NewMessageAction = m =>
            {
                if (!loadingRan)
                {
                    throw new NUnitException("Loading task has not completed before new message processed.");
                }

                newMessages.Add(m);
            };

            _consumer.ReturnAndSignalStop(consumeResults, _testServiceTokenSource, true);

            // Act
            await _testConsumer.StartAsync(default);
            _testServiceTokenSource.Token.WaitHandle.WaitOne();
            await _testConsumer.StopAsync(default);

            // Assert
            newMessages.Should().ContainSingle(r => r.Message.Key == firstKey).Which
                .Message.Value.Should().Be("new_message_1");
            newMessages.Should().NotContain(r => r.Message.Key == secondKey);
        }

        [Test]
        public async Task OnLoading_LoadingCompletesBeforeNewPolling()
        {
            // Arrange
            var firstKey = "1";
            var secondKey = "2";
            var loadingRan = false;
            Dictionary<string, ConsumeResult<string, string>> loadedMessages = default;
            var newMessages = new List<ConsumeResult<string, string>>();
            var consumeResults = new List<ConsumeResult<string, string>>
            {
                KafkaSubstitutes.CreateResult(firstKey, "first_message_1", 0),
                KafkaSubstitutes.CreateResult(secondKey, "first_message_2", 1),
                KafkaSubstitutes.CreateResult(firstKey, "second_message_1", 2),
                KafkaSubstitutes.CreateResult(secondKey, "second_message_2", 3),
                KafkaSubstitutes.CreateResult(string.Empty, string.Empty, 4, 0, true), // mark the end of existing messages
                KafkaSubstitutes.CreateResult(firstKey, "new_message_1", 5),
                KafkaSubstitutes.CreateResult(secondKey, "new_message_2", 6),
                KafkaSubstitutes.CreateResult(string.Empty, string.Empty, 4, 0, true)
            };

            _testConsumer.OnLoadingAction = d =>
            {
                loadedMessages = d;
                loadingRan = true;
            };

            _testConsumer.NewMessageAction = m =>
            {
                if (!loadingRan)
                {
                    throw new NUnitException("Loading task has not completed before new message processed.");
                }

                newMessages.Add(m);
            };

            _consumer.ReturnAndSignalStop(consumeResults, _testServiceTokenSource, true);

            // Act
            await _testConsumer.StartAsync(default);
            _testServiceTokenSource.Token.WaitHandle.WaitOne();
            await _testConsumer.StopAsync(default);

            // Assert
            newMessages.Should().ContainSingle(r => r.Message.Key == firstKey).Which
                .Message.Value.Should().Be("new_message_1");
            newMessages.Should().ContainSingle(r => r.Message.Key == secondKey).Which
                .Message.Value.Should().Be("new_message_2");
        }

        [Test]
        public async Task OnAssignment_SendsCorrectOffsets_OnReassignment()
        {
            // Arrange
            // this could potentially be cleaned up in the future
            // The idea is to make sure the consumer is translating the latest offsets it stores to keep its place in the topic on reassign
            Func<IConsumer<string, string>, List<TopicPartition>, IEnumerable<TopicPartitionOffset>> onAssign = default;
            _consumerSetup = Substitute.ForPartsOf<ConsumerBuilderOptions<string, string>>(new ConsumerBuilder<string, string>(KafkaSubstitutes.FakeConsumerConfig));
            _consumerSetup.SetPartitionsAssignedHandler(
                Arg.Do<Func<IConsumer<string, string>, List<TopicPartition>, IEnumerable<TopicPartitionOffset>>>(a =>
                    onAssign = a));
            _consumerSetupAction.Invoke(_consumerSetup);

            var firstKey = "1";
            var secondKey = "2";
            Dictionary<string, ConsumeResult<string, string>> processedResults = default;

            var consumeResults = new List<ConsumeResult<string, string>>
            {
                KafkaSubstitutes.CreateResult(firstKey, "first_message_1", 0),
                KafkaSubstitutes.CreateResult(secondKey, "first_message_2", 1),
                KafkaSubstitutes.CreateResult(firstKey, "second_message_1", 2),
                KafkaSubstitutes.CreateResult(secondKey, "second_message_2", 3)
            };

            _testConsumer.OnLoadingAction = m =>
            {
                processedResults = m;
                _testServiceTokenSource.Cancel();
            };
            _consumer.ReturnAndSignalStop(consumeResults, _testServiceTokenSource, true);

            // Act
            await _testConsumer.StartAsync(default);
            _testServiceTokenSource.Token.WaitHandle.WaitOne();
            var reassignment = onAssign?.Invoke(_consumer, _assignment);
            await _testConsumer.StopAsync(default);

            // Assert
            reassignment.Should().NotBeNull().And.Subject.Should().ContainSingle()
                .Which.Should().Be(new TopicPartitionOffset("test_topic", 0, 4));
        }

        [Test]
        public async Task OnAssignment_SendsCorrectOffsets_OnModifiedAssignment()
        {
            // Arrange
            Func<IConsumer<string, string>, List<TopicPartition>, IEnumerable<TopicPartitionOffset>> onAssign = default;
            _consumerSetup = Substitute.ForPartsOf<ConsumerBuilderOptions<string, string>>(new ConsumerBuilder<string, string>(KafkaSubstitutes.FakeConsumerConfig));
            _consumerSetup.SetPartitionsAssignedHandler(
                Arg.Do<Func<IConsumer<string, string>, List<TopicPartition>, IEnumerable<TopicPartitionOffset>>>(a =>
                    onAssign = a));
            _consumerSetupAction.Invoke(_consumerSetup);

            var modifiedAssignment = _assignment.Concat(new[] { new TopicPartition("test_topic", 1) });

            var firstKey = "1";
            var secondKey = "2";
            Dictionary<string, ConsumeResult<string, string>> processedResults = default;

            var consumeResults = new List<ConsumeResult<string, string>>
            {
                KafkaSubstitutes.CreateResult(firstKey, "first_message_1", 0),
                KafkaSubstitutes.CreateResult(secondKey, "first_message_2", 1),
                KafkaSubstitutes.CreateResult(firstKey, "second_message_1", 2),
                KafkaSubstitutes.CreateResult(secondKey, "second_message_2", 3)
            };

            _testConsumer.OnLoadingAction = m =>
            {
                processedResults = m;
                _testServiceTokenSource.Cancel();
            };
            _consumer.ReturnAndSignalStop(consumeResults, _testServiceTokenSource, true);

            // Act
            await _testConsumer.StartAsync(default);
            _testServiceTokenSource.Token.WaitHandle.WaitOne();
            var reassignment = onAssign?.Invoke(_consumer, modifiedAssignment.ToList());
            await _testConsumer.StopAsync(default);

            // Assert
            var newPositions = reassignment.Should().NotBeNull().And.Subject.Should().HaveCount(2)
                .And.Subject.ToDictionary(k => k.Partition);
            newPositions.Should().ContainKey(0).WhoseValue.Should().Be(new TopicPartitionOffset("test_topic", 0, 4));
            newPositions.Should().ContainKey(1).WhoseValue.Should().Be(new TopicPartitionOffset("test_topic", 1, Offset.Beginning));
        }

        [Test]
        public async Task OnAssignment_SendsCorrectOffsets_OnRemovedAssignment()
        {
            // Arrange
            Func<IConsumer<string, string>, List<TopicPartition>, IEnumerable<TopicPartitionOffset>> onAssign = default;
            _consumerSetup = Substitute.ForPartsOf<ConsumerBuilderOptions<string, string>>(new ConsumerBuilder<string, string>(KafkaSubstitutes.FakeConsumerConfig));
            _consumerSetup.SetPartitionsAssignedHandler(
                Arg.Do<Func<IConsumer<string, string>, List<TopicPartition>, IEnumerable<TopicPartitionOffset>>>(a =>
                    onAssign = a));
            _consumerSetupAction.Invoke(_consumerSetup);

            var firstKey = "1";
            var secondKey = "2";
            Dictionary<string, ConsumeResult<string, string>> processedResults = default;

            var consumeResults = new List<ConsumeResult<string, string>>
            {
                KafkaSubstitutes.CreateResult(firstKey, "second_message_1", 2, 1),
            };

            _testConsumer.OnLoadingAction = m =>
            {
                processedResults = m;
                _testServiceTokenSource.Cancel();
            };
            _consumer.ReturnAndSignalStop(consumeResults, _testServiceTokenSource, true);

            // Act
            await _testConsumer.StartAsync(default);
            _testServiceTokenSource.Token.WaitHandle.WaitOne();
            var reassignment = onAssign?.Invoke(_consumer, _assignment);
            await _testConsumer.StopAsync(default);

            // Assert
            reassignment.Should().NotBeNull().And.Subject.Should().ContainSingle()
                .Which.Should().Be(new TopicPartitionOffset("test_topic", 0, Offset.Beginning));
        }
    }
}
