using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Orleans.Streams.RabbitMq
{
    public interface IRabbitMqConnectorFactory
    {
        ILoggerFactory LoggerFactory { get; }
        IRabbitMqConsumer CreateConsumer(QueueId queueId);
        IRabbitMqProducer CreateProducer();
        IRabbitMqConnector CreateGenericConnector(string name);
    }

    public interface IRabbitMqConnector : IDisposable
    {
        IModel Channel { get; }
        ILogger Logger { get; }
        TaskScheduler Scheduler { get; }

        event EventHandler<BasicAckEventArgs> BasicAcks;
        event EventHandler<BasicNackEventArgs> BasicNacks;
        event EventHandler<ModelCreatedEventArgs> ModelCreated;

        void EnsureChannelAvailable();
    }

    public interface IRabbitMqConsumer : IDisposable
    {
        Task AckAsync(object channel, ulong deliveryTag, bool multiple);
        Task NackAsync(object channel, ulong deliveryTag, bool requeue);
        Task<RabbitMqMessage> ReceiveAsync();
    }

    public interface IRabbitMqProducer : IDisposable
    {
        Task SendAsync(RabbitMqMessage message);
    }

    public interface ITopologyProvider
    {
        string GetNameForQueue(QueueId queueId);
        RabbitMqQueueProperties GetQueueProperties(string queueName);
        RabbitMqExchangeProperties GetExchangeProperties(string exchangeName);
    }

}
