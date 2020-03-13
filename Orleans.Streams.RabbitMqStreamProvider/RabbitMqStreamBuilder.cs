using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.ApplicationParts;
using Orleans.Configuration;
using Orleans.Streams;
using Orleans.Streams.RabbitMq;

namespace Orleans.Hosting
{
    public interface IRabbitMqStreamConfigurator : INamedServiceConfigurator { }

    public static class RabbitMqStreamConfiguratorExtensions
    {
        public static void ConfigureStreamQueueMapper(this IRabbitMqStreamConfigurator configurator, Func<IServiceProvider, string, IStreamQueueMapper> factory)
        {
            configurator.ConfigureComponent(factory);
        }
        public static void ConfigureStreamQueueMapper<TStreamQueueMapper>(this IRabbitMqStreamConfigurator configurator)
            where TStreamQueueMapper : IStreamQueueMapper
        {
            configurator.ConfigureComponent<IStreamQueueMapper>((sp, n) => ActivatorUtilities.CreateInstance<TStreamQueueMapper>(sp));
        }

        public static void ConfigureTopologyProvider(this IRabbitMqStreamConfigurator configurator, Func<IServiceProvider, string, ITopologyProvider> factory)
        {
            configurator.ConfigureComponent(factory);
        }
        public static void ConfigureTopologyProvider<TTopologyProvider>(this IRabbitMqStreamConfigurator configurator)
            where TTopologyProvider : ITopologyProvider
        {
            configurator.ConfigureComponent<ITopologyProvider>((sp, n) => ActivatorUtilities.CreateInstance<TTopologyProvider>(sp));
        }

        public static void ConfigureQueueDataAdapter(this IRabbitMqStreamConfigurator configurator, Func<IServiceProvider, string, IQueueDataAdapter<RabbitMqMessage, IEnumerable<IBatchContainer>>> factory)
        {
            configurator.ConfigureComponent(factory);
        }

        public static void ConfigureQueueDataAdapter<TQueueDataAdapter>(this IRabbitMqStreamConfigurator configurator)
            where TQueueDataAdapter : IQueueDataAdapter<RabbitMqMessage, IEnumerable<IBatchContainer>>
        {
            configurator.ConfigureComponent<IQueueDataAdapter<RabbitMqMessage, IEnumerable<IBatchContainer>>>((sp, n) => ActivatorUtilities.CreateInstance<TQueueDataAdapter>(sp));
        }

        public static void ConfigureRabbitMq(this IRabbitMqStreamConfigurator configurator, Action<OptionsBuilder<RabbitMqOptions>> configureOptions)
        {
            configurator.Configure(configureOptions);
        }
    }

    public interface ISiloRabbitMqStreamConfigurator : IRabbitMqStreamConfigurator, ISiloPersistentStreamConfigurator { }

    public static class SiloRabbitMqStreamConfiguratorExtensions
    {
        public static void ConfigureCacheSize(this ISiloRabbitMqStreamConfigurator configurator, int cacheSize = SimpleQueueCacheOptions.DEFAULT_CACHE_SIZE)
        {
            configurator.Configure<SimpleQueueCacheOptions>(ob => ob.Configure(options => options.CacheSize = cacheSize));
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
            this.ConfigureComponent(RabbitMqOnlineConnectorFactory.Create);

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
            this.ConfigureComponent(RabbitMqOnlineConnectorFactory.Create);

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