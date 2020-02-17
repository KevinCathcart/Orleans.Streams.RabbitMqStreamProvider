using System;
using Orleans.Streaming;

namespace Orleans.Hosting
{
    public static class SiloBuilderExtensions
    {
        /// <summary>
        /// Configure silo to use RMQ persistent streams.
        /// </summary>
        public static ISiloHostBuilder AddRabbitMqStream(this ISiloHostBuilder builder, string name, Action<SiloRabbitMqStreamConfigurator> configure)
        {
            configure?.Invoke(new SiloRabbitMqStreamConfigurator(name, configDelegate => builder.ConfigureServices(configDelegate)));
            return builder;
        }
    }
}