using System;
using System.Threading.Tasks;

using Company.Kafka.Services.DependencyInjection;
using Company.Kafka.TestHost.Configuration;
using Company.Kafka.TestHost.HostedServices;
using Company.Kafka.TestHost.Messages;

using Confluent.Kafka;
using Confluent.SchemaRegistry;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using NLog.Web;

namespace Company.Kafka.TestHost
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var testHost = new HostBuilder()
                .ConfigureAppConfiguration((context, configBuilder) =>
                {
                    context.HostingEnvironment.ApplicationName = nameof(TestHost);

                    configBuilder
                        .SetBasePath(AppDomain.CurrentDomain.BaseDirectory)
                        .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                        .AddEnvironmentVariables();
                })
                .ConfigureServices((context, serviceCollection) =>
                {
                    var config = context.Configuration;
                    serviceCollection
                        .AddSchemaRegistryClient(config.GetInstance<SchemaRegistryConfig>())
                        .AddSchemaRegistryConsumerFactory()
                        .AddSchemaRegistryProducerFactory()
                        .AddSingleton(config.GetInstance<Consumers>())
                        .AddHostedService<TestCacheMessageConsumer>()
                        .AddKafkaProducer<string, TestMessage>(config.GetInstance<ProducerConfig>())
                        .AddHostedService<MessageConsumerService>()
                        .AddHostedService<FanOutConsumerService>()
                        .AddHostedService<TestMessageService>();
                })
                .ConfigureLogging((context, logBuilder) =>
                {
                    logBuilder.AddConfiguration(context.Configuration.GetSection("Logging"));
                })
                .UseNLog();

            await testHost.Build().RunAsync();
        }
    }
}
