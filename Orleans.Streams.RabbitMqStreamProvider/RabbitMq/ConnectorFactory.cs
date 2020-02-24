using Microsoft.Extensions.Logging;
using Orleans.Configuration;

namespace Orleans.Streams.RabbitMq
{
    internal class RabbitMqOnlineConnectorFactory : IRabbitMqConnectorFactory
    {
        private readonly RabbitMqOptions _options;
        
        public RabbitMqOnlineConnectorFactory(RabbitMqOptions options, ILoggerFactory loggerFactory)
        {
            _options = options;
            LoggerFactory = loggerFactory;
        }

        public ILoggerFactory LoggerFactory { get; }

        public IRabbitMqConsumer CreateConsumer(QueueId queueId)
            => new RabbitMqConsumer(new RabbitMqConnector(_options, LoggerFactory.CreateLogger($"{typeof(RabbitMqConsumer).FullName}.{queueId}")), GetNameForQueue(queueId));

        public IRabbitMqProducer CreateProducer()
            => new RabbitMqProducer(new RabbitMqConnector(_options, LoggerFactory.CreateLogger<RabbitMqProducer>()));

        public string GetNameForQueue(QueueId queueId)
        {
            return _options.UseQueuePartitioning
                ? $"{queueId.GetStringNamePrefix()}-{queueId.GetNumericId()}"
                : queueId.GetStringNamePrefix();
        }
    }
}