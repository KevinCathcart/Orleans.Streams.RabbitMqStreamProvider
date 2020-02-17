using System;
using Orleans.Streaming;

namespace Orleans.Hosting
{
    public static class ClientBuilderExtensions
    {
        /// <summary>
        /// Configure client to use RMQ persistent streams.
        /// This version enables to inject a custom BacthContainer serializer.
        /// </summary>
        public static IClientBuilder AddRabbitMqStream(this IClientBuilder builder, string name, Action<ClusterClientRabbitMqStreamConfigurator> configure)
        {
            configure?.Invoke(new ClusterClientRabbitMqStreamConfigurator(name, builder));
            return builder;
        }
    }
}