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
            => new RabbitMqConsumer(new RabbitMqConnector(_connectionProvider, LoggerFactory.CreateLogger($"{typeof(RabbitMqConsumer).FullName}.{queueId}")), _topologyProvider.GetNameForQueue(queueId), _topologyProvider);

        public IRabbitMqProducer CreateProducer()
            => new RabbitMqProducer(new RabbitMqConnector(_connectionProvider, LoggerFactory.CreateLogger<RabbitMqProducer>()), _topologyProvider);
    }
}