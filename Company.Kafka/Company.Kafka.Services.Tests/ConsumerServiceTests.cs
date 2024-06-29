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

namespace Company.Kafka.Services.Tests
{
    public class ConsumerServiceTests
    {
        private TestConsumer _testConsumer;

        private CancellationTokenSource _testServiceTokenSource;

        private IConsumerFactory _consumerFactory;

        private IConsumer<string, string> _consumer;

        private ConsumerInstanceSettings _consumerInstanceSettings;

        private ILoggerFactory _loggerFactory;

        private JsonLoggingFixture _loggingFixture;

        [SetUp]
        public void Setup()
        {
            _consumerInstanceSettings = new ConsumerInstanceSettings
            {
                TopicName = "test_topic",
                LoggingDelaySeconds = 1
            };

            var config = new ConsumerConfig { BootstrapServers = "test" };
            _consumerFactory = Substitute.For<IConsumerFactory>();
            _loggingFixture = new JsonLoggingFixture();
            _loggerFactory = Substitute.For<ILoggerFactory>();
            _loggerFactory.CreateLogger(Arg.Any<string>()).Returns(_loggingFixture.GetLogger<TestConsumer>());
            _consumer = Substitute.For<IConsumer<string, string>>();
            _consumerFactory.GetConsumer(Arg.Any<ConsumerConfig>(), Arg.Any<Action<ConsumerBuilderOptions<string, string>>>())
                .Returns(_consumer);

            _testServiceTokenSource = new CancellationTokenSource();
            _testConsumer = new TestConsumer(_loggerFactory, _consumerFactory, _consumerInstanceSettings);
        }

        [Test]
        public async Task HandleMessage_PassesCorrectMessage()
        {
            // Arrange
            ConsumeResult<string, string> result = default;

            var uniqueResult = new ConsumeResult<string, string>
            {
                Message = new Message<string, string>
                {
                    Key = Guid.NewGuid().ToString(),
                    Value = Guid.NewGuid().ToString()
                }
            };

            _testConsumer.MessageAction = (m, c) =>
            {
                result = m;
                _testServiceTokenSource.Cancel();
            };
            _consumer.Consume(Arg.Any<CancellationToken>())
                .Returns(uniqueResult);

            // Act
            await _testConsumer.StartAsync(default);
            _testServiceTokenSource.Token.WaitHandle.WaitOne();
            await _testConsumer.StopAsync(default);

            // Assert
            result.Message.Key.Should().Be(uniqueResult.Message.Key);
            result.Message.Value.Should().Be(uniqueResult.Message.Value);
        }

        [Test]
        public async Task ConsumerService_PassesValidCancellationToken()
        {
            // Arrange
            CancellationToken tokenReceived = default;

            _testConsumer.MessageAction = (m, c) =>
            {
                tokenReceived = c;
                _testServiceTokenSource.Cancel();
            };
            _consumer.Consume(Arg.Any<CancellationToken>())
                .Returns(new ConsumeResult<string, string>());

            // Act
            await _testConsumer.StartAsync(default);
            _testServiceTokenSource.Token.WaitHandle.WaitOne();
            await _testConsumer.StopAsync(default);

            // Assert
            tokenReceived.Should().NotBe(CancellationToken.None);
        }

        [Test]
        public async Task ConsumerService_SignalsCancellation_OnStop()
        {
            // Arrange
            var cancellationRequested = false;
            _testConsumer.MessageAction = (m, c) =>
            {
                // cancel test wait and then wait for service signal
                _testServiceTokenSource.Cancel();
                c.WaitHandle.WaitOne();
                cancellationRequested = c.IsCancellationRequested;
            };
            _consumer.Consume(Arg.Any<CancellationToken>())
                .Returns(new ConsumeResult<string, string>());

            // Act
            await _testConsumer.StartAsync(default);
            _testServiceTokenSource.Token.WaitHandle.WaitOne();
            await _testConsumer.StopAsync(default);

            // Assert
            cancellationRequested.Should().BeTrue();
        }

        [Test]
        public async Task ConsumerService_ShutsDownConsumerHandle_OnDispose()
        {
            // Arrange
            _testConsumer.MessageAction = (m, c) =>
            {
                // ensure a message is received, cancel, and wait for shutdown
                _testServiceTokenSource.Cancel();
                c.WaitHandle.WaitOne();
            };
            _consumer.Consume(Arg.Any<CancellationToken>())
                .Returns(new ConsumeResult<string, string>());

            // Act
            await _testConsumer.StartAsync(default);
            _testServiceTokenSource.Token.WaitHandle.WaitOne();
            await _testConsumer.StopAsync(default);
            _testConsumer.Dispose();

            // Assert
            Received.InOrder(() =>
            {
                _consumer.Close();
                _consumer.Dispose();
            });
        }

        [Test]
        public async Task ConsumerService_UnsubscribesConsumer_OnStop()
        {
            // Arrange
            var disposeWaitHandle = new EventWaitHandle(false, EventResetMode.ManualReset);
            _testConsumer.MessageAction = (m, c) =>
            {
                // ensure a message is received, cancel, and wait for shutdown
                _testServiceTokenSource.Cancel();
                c.WaitHandle.WaitOne();
            };
            _consumer.Consume(Arg.Any<CancellationToken>())
                .Returns(new ConsumeResult<string, string>());

            // Act
            await _testConsumer.StartAsync(default);
            _testServiceTokenSource.Token.WaitHandle.WaitOne();
            await _testConsumer.StopAsync(default);

            // Assert
            _consumer.Received(1).Unsubscribe();
            _consumer.DidNotReceive().Dispose();
        }

        [Test]
        public async Task ConsumerService_CapturesException()
        {
            // we should throw an exception while handling
            // base should still run and stop without issue
            // Arrange
            var threwException = false;
            _testConsumer.MessageAction = (m, c) =>
            {
                if (!threwException)
                {
                    threwException = true;
                    throw new Exception("BOOM!");
                }

                _testServiceTokenSource.Cancel();
            };
            _consumer.Consume(Arg.Any<CancellationToken>())
                .Returns(new ConsumeResult<string, string>());

            // Act
            await _testConsumer.StartAsync(default);
            _testServiceTokenSource.Token.WaitHandle.WaitOne();
            Func<Task> stopService = () => _testConsumer.StopAsync(default);

            // Assert
            await stopService.Should().NotThrowAsync();
            threwException.Should().BeTrue();
        }

        [Test]
        public async Task BatchConsumerService_CapturesAndRetries_WhenConsumeException()
        {
            // we should throw an exception while handling
            // base should still run and stop without issue
            // Arrange
            var exception = new ConsumeException(
                new ConsumeResult<byte[], byte[]>
                {
                    Offset = new Offset(1234),
                    Topic = "test",
                    Partition = new Partition(1)
                },
                new Error(ErrorCode.Local_KeyDeserialization));

            _consumer.Consume(Arg.Any<CancellationToken>())
                .Returns(x =>
                {
                    _testServiceTokenSource.Cancel();
                    throw exception;
                });

            // Act
            await _testConsumer.StartAsync(default);
            _testServiceTokenSource.Token.WaitHandle.WaitOne();
            Func<Task> stopService = () => _testConsumer.StopAsync(default);

            // Assert
            await stopService.Should().NotThrowAsync();
            _loggingFixture.Logs.Should()
                .ContainSingle(m => m.Message.StartsWith("Consumer BackOff 1 for Operation: BaseConsumerService<String, String> CorrelationId:"));
            _loggingFixture.Logs
                .Should().ContainSingle(l =>
                    l.Message.Contains("Topic: test Partition: [1] Offset: 1234 Error: Local: Key deserialization error")
                    && l.Level == "ERROR");
        }

        private static Dictionary<int, Offset> GenerateTopicPositions(int partitionCount)
        {
            var randomGen = new Random();
            var positions = new Dictionary<int, Offset>();

            for (var i = 0; i < partitionCount; i++)
            {
                positions[i] = new Offset((long)randomGen.Next(0, 9999999));
            }

            return positions;
        }
    }
}
