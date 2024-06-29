using System;
using System.Threading.Tasks;

using Company.Kafka.Services.Factories;
using Company.Kafka.Services.Factories.Interfaces;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;

using NUnit.Framework;

namespace Company.Kafka.Services.Tests.Factories
{
    public class SchemaRegistryProducerFactoryTests
    {
        private SchemaRegistryProducerFactory _testFactory;

        private ISchemaRegistryClient _testRegistryClient;

        private ProducerConfig _testConfig;

        private IProducerBuilderProvider _testBuilderProvider;

        [SetUp]
        public void SetUp()
        {
            _testRegistryClient = Substitute.For<ISchemaRegistryClient>();
            _testBuilderProvider = Substitute.For<IProducerBuilderProvider>();
            _testFactory = new SchemaRegistryProducerFactory(_testRegistryClient, true, _testBuilderProvider, new NullLogger<SchemaRegistryProducerFactory>());
        }

        [Test]
        public void GetProducer_CachesProducers_WhenSameType()
        {
            _testConfig = new ProducerConfig
            {
                BootstrapServers = "fake.cl.local:1234"
            };

            _testBuilderProvider.GetBuilder<string, string>(Arg.Any<ProducerConfig>())
                .Returns(Substitute.ForPartsOf<ProducerBuilder<string, string>>(_testConfig));

            var producerOne = _testFactory.GetProducer<string, string>(_testConfig, null);

            var producerTwo = _testFactory.GetProducer<string, string>(_testConfig, null);

            _testBuilderProvider.Received(1).GetBuilder<string, string>(Arg.Any<ProducerConfig>());
            producerTwo.Should().BeSameAs(producerOne).And.NotBeNull();
        }

        [Test]
        public async Task GetProducer_ThrowsException_WhenFactoryDisposed()
        {
            _testConfig = new ProducerConfig
            {
                BootstrapServers = "fake.cl.local:1234"
            };

            _testBuilderProvider.GetBuilder<string, string>(Arg.Any<ProducerConfig>())
                .Returns(Substitute.ForPartsOf<ProducerBuilder<string, string>>(_testConfig));

            var getABunchOfProducers = Task.Run(() =>
            {
                for (var i = 0; i < 2; i++)
                {
                    _testFactory.GetProducer<string, string>(_testConfig, null);
                    _testFactory.Dispose();
                }
            });

            Func<Task> finalResult = async () => await getABunchOfProducers;

            await finalResult.Should().ThrowAsync<InvalidOperationException>();
        }

        [TestCase(true)]
        [TestCase(false)]
        public void Dispose_ReleasesResourcesCorrectly(bool ownsRegistryClient)
        {
            _testConfig = new ProducerConfig
            {
                BootstrapServers = "fake.cl.local:1234"
            };

            _testFactory = new SchemaRegistryProducerFactory(_testRegistryClient, ownsRegistryClient, _testBuilderProvider, new NullLogger<SchemaRegistryProducerFactory>());

            var producerSub = Substitute.For<IProducer<string, string>>();
            var anotherProducerSub = Substitute.For<IProducer<long, long>>();

            _testBuilderProvider.GetBuilder<string, string>(Arg.Any<ProducerConfig>())
                .Returns(c =>
                {
                    var builder = Substitute.ForPartsOf<ProducerBuilder<string, string>>(_testConfig);
                    builder.Build().Returns(producerSub);

                    return builder;
                });

            _testBuilderProvider.GetBuilder<long, long>(Arg.Any<ProducerConfig>())
                .Returns(c =>
                {
                    var builder = Substitute.ForPartsOf<ProducerBuilder<long, long>>(_testConfig);
                    builder.Build().Returns(anotherProducerSub);

                    return builder;
                });

            _testFactory.GetProducer<long, long>(_testConfig, null);
            _testFactory.GetProducer<string, string>(_testConfig, null);

            _testFactory.Dispose();

            producerSub.Received(1).Dispose();
            anotherProducerSub.Received(1).Dispose();

            if (ownsRegistryClient)
            {
                _testRegistryClient.Received(1).Dispose();
            }
        }
    }
}
