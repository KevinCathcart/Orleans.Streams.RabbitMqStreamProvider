using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.ApplicationParts;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.Streams.BatchContainer;
using Orleans.Streams.RabbitMq;

namespace Orleans.Hosting
{
    public interface IRabbitMqStreamConfigurator : INamedServiceConfigurator { }

    public static class RabbitMqStreamConfiguratorExtensions
    {
        public static void UseSerializer<TSerializer>(this IRabbitMqStreamConfigurator configurator) where TSerializer : IBatchContainerSerializer
        {
            configurator.ConfigureDelegate(services =>
            {
                services.AddTransientNamedService<IBatchContainerSerializer, DefaultBatchContainerSerializer>(configurator.Name);
            });
        }

        public static void ConfigureStreamQueueMapper(this IRabbitMqStreamConfigurator configurator, Func<IServiceProvider, string, IStreamQueueMapper> factory)
        {
            configurator.ConfigureComponent(factory);
        }

        public static void ConfigureTopologyProvider(this IRabbitMqStreamConfigurator configurator, Func<IServiceProvider, string, ITopologyProvider> factory)
        {
            configurator.ConfigureComponent(factory);
        }

        public static void ConfigureRabbitMq(this IRabbitMqStreamConfigurator configurator, string host, int port, string virtualHost, string user, string password, string queueName, bool useQueuePartitioning = RabbitMqOptions.DefaultUseQueuePartitioning, int numberOfQueues = RabbitMqOptions.DefaultNumberOfQueues)
        {
            configurator.Configure<RabbitMqOptions>(ob => ob.Configure(options =>
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
        }
    }

    public interface ISiloRabbitMqStreamConfigurator : IRabbitMqStreamConfigurator, ISiloPersistentStreamConfigurator { }

    public static class SiloRabbitMqStreamConfiguratorExtensions
    {
        public static void ConfigureCache(this ISiloRabbitMqStreamConfigurator configurator, int cacheSize)
        {
            configurator.Configure<CachingOptions>(ob => ob.Configure(options => options.CacheSize = cacheSize));
        }

        public static void ConfigureCache(this ISiloRabbitMqStreamConfigurator configurator, int cacheSize, TimeSpan cacheFillingTimeout)
        {
            configurator.Configure<CachingOptions>(ob => ob.Configure(options =>
                {
                    options.CacheSize = cacheSize;
                    options.CacheFillingTimeout = cacheFillingTimeout;
                }));
        }
    }

    public class SiloRabbitMqStreamConfigurator : SiloPersistentStreamConfigurator, ISiloRabbitMqStreamConfigurator
    {
        public SiloRabbitMqStreamConfigurator(string name, Action<Action<IServiceCollection>> configureDelegate, Action<Action<IApplicationPartManager>> configureAppPartsDelegate)
            : base(name, configureDelegate, RabbitMqAdapterFactory.Create)
        {
            configureAppPartsDelegate(RabbitMqStreamConfiguratorCommon.AddParts);
            this.ConfigureComponent(RabbitMqOptionsValidator.Create);
            this.ConfigureComponent(SimpleQueueCacheOptionsValidator.Create);

            this.ConfigureDelegate(services =>
            {
                services.TryAddSingleton<IRabbitMqStreamQueueMapperFactory, RabbitMqStreamQueueMapperFactory>();
                services.TryAddSingleton<ITopologyProviderFactory, TopologyProviderFactory>();
            });
        }
    }

    public interface IClusterClientRabbitMqStreamConfigurator : IRabbitMqStreamConfigurator, IClusterClientPersistentStreamConfigurator { }

    public class ClusterClientRabbitMqStreamConfigurator : ClusterClientPersistentStreamConfigurator, IClusterClientRabbitMqStreamConfigurator
    {
        public ClusterClientRabbitMqStreamConfigurator(string name, IClientBuilder builder)
            : base(name, builder, RabbitMqAdapterFactory.Create)
        {
            builder
                .ConfigureApplicationParts(RabbitMqStreamConfiguratorCommon.AddParts);
            this.ConfigureComponent(RabbitMqOptionsValidator.Create);

            this.ConfigureDelegate(services =>
            {
                services.TryAddSingleton<IRabbitMqStreamQueueMapperFactory, RabbitMqStreamQueueMapperFactory>();
                services.TryAddSingleton<ITopologyProviderFactory, TopologyProviderFactory>();
            });

        }
    }

    public static class RabbitMqStreamConfiguratorCommon
    {
        public static void AddParts(IApplicationPartManager parts)
        {
            parts.AddFrameworkPart(typeof(RabbitMqAdapterFactory).Assembly);
        }
    }
}