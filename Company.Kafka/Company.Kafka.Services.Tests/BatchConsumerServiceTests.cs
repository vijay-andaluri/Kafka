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

using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Company.Kafka.Services.Tests
{
    public class BatchConsumerServiceTests
    {
        private TestBatchConsumer _testConsumer;

        private CancellationTokenSource _testServiceTokenSource;

        private IConsumerFactory _consumerFactory;

        private IConsumer<string, string> _consumer;

        private BatchConsumerSettings _consumerInstanceSettings;

        private ILoggerFactory _loggerFactory;

        private JsonLoggingFixture _loggingFixture;

        [SetUp]
        public void Setup()
        {
            _consumerInstanceSettings = new BatchConsumerSettings
            {
                MaxBatchSize = 5,
                MaxBatchWaitMilliseconds = 100,
                TopicName = "test_topic",
                LoggingDelaySeconds = 1
            };

            var config = new ConsumerConfig { BootstrapServers = "test" };
            _consumerFactory = Substitute.For<IConsumerFactory>();
            _loggingFixture = new JsonLoggingFixture(LogLevel.Trace);
            _loggerFactory = Substitute.For<ILoggerFactory>();
            _loggerFactory.CreateLogger(Arg.Any<string>()).Returns(_loggingFixture.GetLogger<TestConsumer>());
            _consumer = Substitute.For<IConsumer<string, string>>();
            _consumerFactory.GetConsumer(Arg.Any<ConsumerConfig>(), Arg.Any<Action<ConsumerBuilderOptions<string, string>>>())
                .Returns(_consumer);

            _testServiceTokenSource = new CancellationTokenSource();
            _testConsumer = new TestBatchConsumer(_loggerFactory, _consumerFactory, _consumerInstanceSettings);
        }

        [Test]
        public async Task HandleMessage_PassesCorrectMessages()
        {
            // Arrange
            List<ConsumeResult<string, string>> results = default;

            var uniqueResults = Enumerable.Range(0, _consumerInstanceSettings.MaxBatchSize).Select(i => new Message<string, string>
                {
                    Key = Guid.NewGuid().ToString(),
                    Value = Guid.NewGuid().ToString()
                })
                .ToList();

            _testConsumer.MessageAction = (m, c) =>
            {
                results = m;
                _testServiceTokenSource.Cancel();
            };
            _consumer.ReturnAndSignalStop(uniqueResults, _testServiceTokenSource);

            // Act
            await _testConsumer.StartAsync(default);
            _testServiceTokenSource.Token.WaitHandle.WaitOne();
            await _testConsumer.StopAsync(default);

            // Assert
            results.Select(r => r.Message).Should().BeEquivalentTo(uniqueResults);
        }

        [Test]
        public async Task BatchConsumerService_ReturnsBatch_WhenMaxCountReached()
        {
            // Arrange
            List<ConsumeResult<string, string>> firstResultSet = default;
            List<ConsumeResult<string, string>> secondResultSet = default;
            var batchCount = 0;

            var uniqueResults = Enumerable.Range(0, _consumerInstanceSettings.MaxBatchSize * 2).Select(i => new Message<string, string>
                {
                    Key = Guid.NewGuid().ToString(),
                    Value = Guid.NewGuid().ToString()
                })
                .ToList();

            _testConsumer.MessageAction = (m, c) =>
            {
                if (batchCount == 0)
                {
                    firstResultSet = m;
                    batchCount++;
                    return;
                }

                if (batchCount == 1)
                {
                    secondResultSet = m;
                }

                _testServiceTokenSource.Cancel();
            };
            _consumer.ReturnAndSignalStop(uniqueResults, _testServiceTokenSource);

            // Act
            await _testConsumer.StartAsync(default);
            _testServiceTokenSource.Token.WaitHandle.WaitOne();
            await _testConsumer.StopAsync(default);

            // Assert
            firstResultSet.Select(r => r.Message).Should().BeEquivalentTo(uniqueResults.Take(_consumerInstanceSettings.MaxBatchSize));
            secondResultSet.Select(r => r.Message).Should().BeEquivalentTo(uniqueResults.Skip(_consumerInstanceSettings.MaxBatchSize).Take(_consumerInstanceSettings.MaxBatchSize));
            _loggingFixture.Logs.Where(l =>
                    l.Level == "TRACE" &&
                    l.Message.StartsWith("Returning 5 results.  Batch wait canceled: False.  Service canceled: False."))
                .Should().HaveCount(2);
        }

        [Test]
        public async Task BatchConsumerService_ReturnsBatch_WhenWaitExpires()
        {
            // Arrange
            List<ConsumeResult<string, string>> results = default;
            _consumerInstanceSettings.MaxBatchWaitMilliseconds = 10;

            var uniqueResults = Enumerable.Range(0, _consumerInstanceSettings.MaxBatchSize).Select(i => new Message<string, string>
                {
                    Key = Guid.NewGuid().ToString(),
                    Value = Guid.NewGuid().ToString()
                })
                .ToList();

            _testConsumer.MessageAction = (m, c) =>
            {
                results = m;
                _testServiceTokenSource.Cancel();
            };

            _consumer.ReturnAndWait(uniqueResults.Take(1).ToList());

            // Act
            await _testConsumer.StartAsync(default);
            _testServiceTokenSource.Token.WaitHandle.WaitOne();
            await _testConsumer.StopAsync(default);

            // Assert
            results.Select(r => r.Message).Should().BeEquivalentTo(uniqueResults.Take(1));
        }

        [Test]
        public async Task BatchConsumerService_NoBatch_WhenWaitExpiresAndNoResults()
        {
            // Arrange
            List<ConsumeResult<string, string>> results = default;
            _consumerInstanceSettings.MaxBatchWaitMilliseconds = 10;

            var uniqueResults = Enumerable.Range(0, _consumerInstanceSettings.MaxBatchSize).Select(i => new Message<string, string>
                {
                    Key = Guid.NewGuid().ToString(),
                    Value = Guid.NewGuid().ToString()
                })
                .ToList();

            _testConsumer.MessageAction = (m, c) =>
            {
                results ??= m; // ensure no results are returned
            };

            _consumer.ReturnAndSignalStop(new List<Message<string, string>>(0), _testServiceTokenSource);

            // Act
            await _testConsumer.StartAsync(default);
            _testServiceTokenSource.Token.WaitHandle.WaitOne();
            await _testConsumer.StopAsync(default);

            // Assert
            results.Should().BeNullOrEmpty();
            _consumer.ReceivedCalls()
                .Where(c => c.GetMethodInfo().Name == nameof(IConsumer<string, string>.Consume))
                .Should().HaveCountGreaterOrEqualTo(1);
        }

        [Test]
        public async Task BatchConsumerService_SignalsCancellation_OnStop()
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
        public async Task BatchConsumerService_ShutsDownConsumerHandle_OnDispose()
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
        public async Task BatchConsumerService_UnsubscribesConsumer_OnStop()
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
        public async Task BatchConsumerService_CapturesException()
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
    }
}
