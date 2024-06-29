using System;

using Company.Kafka.Services.DependencyInjection;
using Company.Kafka.Services.Factories;
using Company.Kafka.Services.Factories.Interfaces;

using Confluent.Kafka;
using Confluent.SchemaRegistry;

using FluentAssertions;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;

using NUnit.Framework;

namespace Company.Kafka.Services.Tests.DependencyInjection
{
    public class ServiceExtensionsTests
    {
        private IServiceCollection _testServices;

        [SetUp]
        public void Setup()
        {
            _testServices = new ServiceCollection();
        }

        [Test]
        public void AddConsumerFactory_ExpectedDependencyAvailable_WhenRegisteringOwnClient()
        {
            // Arrange
            _testServices.AddSchemaRegistryConsumerFactory(DummySchemaRegistryConfig());
            var serviceProvider = _testServices.BuildServiceProvider();

            // Act
            // Assert
            serviceProvider.GetService<IConsumerFactory>()
                .Should().NotBeNull()
                .And.Subject.Should().BeOfType<SchemaRegistryConsumerFactory>();

            serviceProvider.GetService<ISchemaRegistryClient>()
                .Should().BeNull(because: "The factory should house and dispose its own instance of the client.");
        }

        [Test]
        public void AddConsumerFactory_ThrowsException_WhenExistingRegistrationExpected()
        {
            // Arrange
            _testServices.AddSchemaRegistryConsumerFactory();
            var serviceProvider = _testServices.BuildServiceProvider();

            // Act
            // Assert
            Action getConsumerFactory = () => serviceProvider.GetService<IConsumerFactory>();

            getConsumerFactory.Should().ThrowExactly<ArgumentException>()
                .And.Message.Should().Be("ISchemaRegistryClient required.  Ensure a call to AddSchemaRegistryClient or an equivalent registration has been made.");
        }

        [Test]
        public void AddProducerFactory_ExpectedDependencyAvailable_WhenRegisteringOwnClient()
        {
            // Arrange
            _testServices.AddTransient(typeof(ILogger<>), typeof(NullLogger<>));
            _testServices.AddSchemaRegistryProducerFactory(DummySchemaRegistryConfig());
            var serviceProvider = _testServices.BuildServiceProvider();

            // Act
            // Assert
            serviceProvider.GetService<IProducerFactory>()
                .Should().NotBeNull()
                .And.Subject.Should().BeOfType<SchemaRegistryProducerFactory>();

            serviceProvider.GetService<ISchemaRegistryClient>()
                .Should().BeNull(because: "The factory should house and dispose its own instance of the client.");
        }

        [Test]
        public void AddProducerFactory_ThrowsException_WhenExistingRegistrationExpected()
        {
            // Arrange
            _testServices.AddSchemaRegistryProducerFactory();
            var serviceProvider = _testServices.BuildServiceProvider();

            // Act
            // Assert
            Action getProducerFactory = () => serviceProvider.GetService<IProducerFactory>();

            getProducerFactory.Should().ThrowExactly<ArgumentException>()
                .And.Message.Should().Be("ISchemaRegistryClient required.  Ensure a call to AddSchemaRegistryClient or an equivalent registration has been made.");
        }

        [Test]
        public void UseSchemaRegistry_ExpectedDependencyAvailable()
        {
            // Arrange
            _testServices.AddSchemaRegistryClient(DummySchemaRegistryConfig());
            var serviceProvider = _testServices.BuildServiceProvider();

            // Act
            // Assert
            var firstInstance = serviceProvider.GetService<ISchemaRegistryClient>()
                .Should().NotBeNull()
                .And.BeOfType<CachedSchemaRegistryClient>().Subject;

            serviceProvider.GetService<ISchemaRegistryClient>()
                .Should().NotBeNull()
                .And.BeEquivalentTo(firstInstance, because: "The client should be a singleton.");
        }

        [Test]
        public void AddProducer_DoesNotThrow_WhenExistingSerializer()
        {
            // Arrange
            var existingSerializer = Substitute.For<ISerializer<string>>();
            _testServices.AddTransient(c => existingSerializer);
            _testServices.AddKafkaProducer<string, string>(DummyProducerConfig());
            var serviceProvider = _testServices.BuildServiceProvider();

            // Act
            Action getProducer = () => serviceProvider.GetRequiredService<IProducer<string, string>>();

            // Assert
            getProducer.Should().NotThrow();
        }

        [Test]
        public void AddProducer_ExpectedDependenciesAvailable()
        {
            // Arrange
            _testServices.AddTransient(c => Substitute.For<ISchemaRegistryClient>());
            _testServices.AddKafkaProducer<string, string>(DummyProducerConfig());
            var serviceProvider = _testServices.BuildServiceProvider();

            // Act
            var firstResolved = serviceProvider.GetService<IProducer<string, string>>();
            var secondResolved = serviceProvider.GetService<IProducer<string, string>>();

            // Assert
            firstResolved.Should().NotBeNull()
                .And.Subject.Should().BeSameAs(secondResolved);
        }

        private static SchemaRegistryConfig DummySchemaRegistryConfig() =>
            new SchemaRegistryConfig
            {
                Url = "https://nowhere"
            };

        private static ProducerConfig DummyProducerConfig() =>
            new ProducerConfig
            {
                BootstrapServers = "https://nowhere",
                ClientId = "test"
            };

        private static ConsumerConfig DummyConsumerConfig() =>
            new ConsumerConfig
            {
                BootstrapServers = "https://nowhere",
                ClientId = "test"
            };
    }
}
