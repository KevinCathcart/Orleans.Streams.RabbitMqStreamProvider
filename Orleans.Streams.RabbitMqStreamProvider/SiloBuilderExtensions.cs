using System;
using Microsoft.Extensions.Options;
using Orleans.Configuration;

namespace Orleans.Hosting
{
    public static class SiloBuilderExtensions
    {
        /// <summary>
        /// Configure silo to use RMQ persistent streams.
        /// </summary>
        public static ISiloHostBuilder AddRabbitMqStream(this ISiloHostBuilder builder, string name, Action<SiloRabbitMqStreamConfigurator> configure)
        {
            var configurator = new SiloRabbitMqStreamConfigurator(name,
                configureServicesDelegate => builder.ConfigureServices(configureServicesDelegate),
                configureAppPartsDelegate => builder.ConfigureApplicationParts(configureAppPartsDelegate));
            configure?.Invoke(configurator);
            return builder;
        }

        /// <summary>
        /// Configure silo to use RMQ persistent streams.
        /// </summary>
        public static ISiloHostBuilder AddRabbitMqStream(this ISiloHostBuilder builder, string name, Action<OptionsBuilder<RabbitMqOptions>> configureOptions)
            => builder.AddRabbitMqStream(name, b => b.Configure(configureOptions));

    }
}