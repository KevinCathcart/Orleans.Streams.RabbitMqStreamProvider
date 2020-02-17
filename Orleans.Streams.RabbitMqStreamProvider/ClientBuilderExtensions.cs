using System;
using Microsoft.Extensions.Options;
using Orleans.Configuration;

namespace Orleans.Hosting
{
    public static class ClientBuilderExtensions
    {
        /// <summary>
        /// Configure client to use RMQ persistent streams.
        /// </summary>
        public static IClientBuilder AddRabbitMqStream(this IClientBuilder builder, string name, Action<ClusterClientRabbitMqStreamConfigurator> configure)
        {
            var configurator = new ClusterClientRabbitMqStreamConfigurator(name, builder);
            configure?.Invoke(configurator);
            return builder;
        }

        /// <summary>
        /// Configure client to use RMQ persistent streams.
        /// </summary>
        public static IClientBuilder AddRabbitMqStream(this IClientBuilder builder, string name, Action<OptionsBuilder<RabbitMqOptions>> configureOptions)
            => builder.AddRabbitMqStream(name, b => b.Configure(configureOptions));

    }
}