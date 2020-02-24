using Microsoft.Extensions.Logging;
using Orleans.Configuration;

namespace Orleans.Streams.RabbitMq
{
    internal class RabbitMqOnlineConnectorFactory : IRabbitMqConnectorFactory
    {
        private readonly RabbitMqOptions _options;
        private readonly RabbitMqConnectionProvider _connectionProvider;

        public RabbitMqOnlineConnectorFactory(RabbitMqOptions options, ILoggerFactory loggerFactory)
        {
            _options = options;
            _connectionProvider = new RabbitMqConnectionProvider(options, loggerFactory.CreateLogger<RabbitMqConnectionProvider>());
            LoggerFactory = loggerFactory;
        }

        public ILoggerFactory LoggerFactory { get; }

        public IRabbitMqConsumer CreateConsumer(QueueId queueId)
            => new RabbitMqConsumer(new RabbitMqConnector(_connectionProvider, LoggerFactory.CreateLogger($"{typeof(RabbitMqConsumer).FullName}.{queueId}")), GetNameForQueue(queueId));

        public IRabbitMqProducer CreateProducer()
            => new RabbitMqProducer(new RabbitMqConnector(_connectionProvider, LoggerFactory.CreateLogger<RabbitMqProducer>()));

        public string GetNameForQueue(QueueId queueId)
        {
            return _options.UseQueuePartitioning
                ? $"{queueId.GetStringNamePrefix()}-{queueId.GetNumericId()}"
                : queueId.GetStringNamePrefix();
        }
    }
}