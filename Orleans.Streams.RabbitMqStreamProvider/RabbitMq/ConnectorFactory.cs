using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;

namespace Orleans.Streams.RabbitMq
{
    internal class RabbitMqOnlineConnectorFactory : IRabbitMqConnectorFactory
    {
        private readonly ITopologyProvider _topologyProvider;
        private readonly RabbitMqConnectionProvider _connectionProvider;

        public RabbitMqOnlineConnectorFactory(RabbitMqOptions options, ILoggerFactory loggerFactory, ITopologyProvider topologyProvider)
        {
            _connectionProvider = new RabbitMqConnectionProvider(options, loggerFactory.CreateLogger<RabbitMqConnectionProvider>());
            LoggerFactory = loggerFactory;
            _topologyProvider = topologyProvider;
        }

        public ILoggerFactory LoggerFactory { get; }

        public IRabbitMqConsumer CreateConsumer(QueueId queueId)
            => new RabbitMqConsumer(CreateConnector(LoggerFactory.CreateLogger($"{typeof(RabbitMqConsumer).FullName}.{queueId}")), _topologyProvider.GetNameForQueue(queueId), _topologyProvider);

        public IRabbitMqProducer CreateProducer()
            => new RabbitMqProducer(CreateConnector(LoggerFactory.CreateLogger<RabbitMqProducer>()), _topologyProvider);

        public IRabbitMqConnector CreateGenericConnector(string name)
            => CreateConnector(LoggerFactory.CreateLogger($"{typeof(RabbitMqConnector)}.{name}"));

        private IRabbitMqConnector CreateConnector(ILogger logger)
            => new RabbitMqConnector(_connectionProvider, logger);

        internal static IRabbitMqConnectorFactory Create(IServiceProvider services, string name)
        {
            var options = services.GetOptionsByName<RabbitMqOptions>(name);
            var topologyFactory = services.GetRequiredService<ITopologyProviderFactory>();
            return ActivatorUtilities.CreateInstance<RabbitMqOnlineConnectorFactory>(services, options, topologyFactory.Get(name));
        }
    }
}