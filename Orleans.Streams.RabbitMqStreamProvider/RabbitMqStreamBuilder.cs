using System;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.Streams.BatchContainer;

namespace Orleans.Streaming
{
    public interface IRabbitMqStreamConfigurator : INamedServiceConfigurator { }

    public static class RabbitMqStreamConfiguratorExtensions
    {
        public static void UseSerializer<TSerializer>(this IRabbitMqStreamConfigurator configurator) where TSerializer : class, IBatchContainerSerializer
        {
            configurator.ConfigureDelegate(services =>
            {
                services.AddTransientNamedService<IBatchContainerSerializer, TSerializer>(configurator.Name);
            });
        }
    }

    public class SiloRabbitMqStreamConfigurator : SiloPersistentStreamConfigurator, IRabbitMqStreamConfigurator
    {
        public SiloRabbitMqStreamConfigurator(string name, Action<Action<IServiceCollection>> configureDelegate)
            : base(name, configureDelegate, RabbitMqAdapterFactory.Create)
        {
            ConfigureDelegate(services =>
                {
                    services.ConfigureNamedOptionForLogging<RabbitMqOptions>(name)
                        .AddTransient<IConfigurationValidator>(sp => new RabbitMqOptionsValidator(sp.GetOptionsByName<RabbitMqOptions>(name), name))
                        .ConfigureNamedOptionForLogging<CachingOptions>(name)
                        .AddTransient<IConfigurationValidator>(sp => new CachingOptionsValidator(sp.GetOptionsByName<CachingOptions>(name), name));
                });
        }

        public SiloRabbitMqStreamConfigurator ConfigureRabbitMq(string host, int port, string virtualHost, string user, string password, string queueName, bool useQueuePartitioning = RabbitMqOptions.DefaultUseQueuePartitioning, int numberOfQueues = RabbitMqOptions.DefaultNumberOfQueues)
        {
            this.Configure<RabbitMqOptions>(ob => ob.Configure(options =>
                {
                    options.HostName = host;
                    options.Port = port;
                    options.VirtualHost = virtualHost;
                    options.UserName = user;
                    options.Password = password;
                    options.QueueNamePrefix = queueName;
                    options.UseQueuePartitioning = useQueuePartitioning;
                    options.NumberOfQueues = numberOfQueues;
                }));
            return this;
        }

        public SiloRabbitMqStreamConfigurator ConfigureCache(int cacheSize)
        {
            this.Configure<CachingOptions>(ob => ob.Configure(options => options.CacheSize = cacheSize));
            return this;
        }

        public SiloRabbitMqStreamConfigurator ConfigureCache(int cacheSize, TimeSpan cacheFillingTimeout)
        {
            this.Configure<CachingOptions>(ob => ob.Configure(options =>
                {
                    options.CacheSize = cacheSize;
                    options.CacheFillingTimeout = cacheFillingTimeout;
                }));
            return this;
        }
    }

    public class ClusterClientRabbitMqStreamConfigurator : ClusterClientPersistentStreamConfigurator, IRabbitMqStreamConfigurator
    {
        public ClusterClientRabbitMqStreamConfigurator(string name, IClientBuilder builder)
            : base(name, builder, RabbitMqAdapterFactory.Create)
        {
            builder
                .ConfigureApplicationParts(parts => parts.AddFrameworkPart(typeof(RabbitMqAdapterFactory).Assembly))
                .ConfigureServices(services => services
                    .ConfigureNamedOptionForLogging<RabbitMqOptions>(name)
                    .AddTransient<IConfigurationValidator>(sp => new RabbitMqOptionsValidator(sp.GetOptionsByName<RabbitMqOptions>(name), name))
                    .ConfigureNamedOptionForLogging<HashRingStreamQueueMapperOptions>(name));

        }

        public ClusterClientRabbitMqStreamConfigurator ConfigureRabbitMq(
            string host, int port, string virtualHost, string user, string password, string queueName,
            bool useQueuePartitioning = RabbitMqOptions.DefaultUseQueuePartitioning,
            int numberOfQueues = RabbitMqOptions.DefaultNumberOfQueues)
        {
            this.Configure<RabbitMqOptions>(ob => ob.Configure(options =>
            {
                options.HostName = host;
                options.Port = port;
                options.VirtualHost = virtualHost;
                options.UserName = user;
                options.Password = password;
                options.QueueNamePrefix = queueName;
                options.UseQueuePartitioning = useQueuePartitioning;
                options.NumberOfQueues = numberOfQueues;
            }));
            return this;
        }
    }
}