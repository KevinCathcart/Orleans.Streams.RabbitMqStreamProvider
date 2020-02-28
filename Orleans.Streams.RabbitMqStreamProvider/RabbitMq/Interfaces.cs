using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Orleans.Streams.RabbitMq
{
    internal interface IRabbitMqConnectorFactory
    {
        ILoggerFactory LoggerFactory { get; }
        IRabbitMqConsumer CreateConsumer(QueueId queueId);
        IRabbitMqProducer CreateProducer();
    }

    internal interface IRabbitMqConsumer : IDisposable
    {
        Task AckAsync(object channel, ulong deliveryTag, bool multiple);
        Task NackAsync(object channel, ulong deliveryTag, bool requeue);
        Task<RabbitMqMessage> ReceiveAsync();
    }

    internal interface IRabbitMqProducer : IDisposable
    {
        Task SendAsync(RabbitMqMessage message);
    }

    public interface ITopologyProvider
    {
        string GetNameForQueue(QueueId queueId);
        RabbitMqQueueProperties GetQueueProperties(string queueName);
    }
}
