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
        void Ack(object channel, ulong deliveryTag, bool multiple);
        void Nack(object channel, ulong deliveryTag);
        RabbitMqMessage Receive();
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
