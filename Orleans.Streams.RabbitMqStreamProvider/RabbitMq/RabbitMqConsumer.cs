using System;
using System.Threading;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Orleans.Streams.RabbitMq
{
    internal class RabbitMqConsumer : IRabbitMqConsumer
    {
        private readonly RabbitMqConnector _connection;
        private readonly string _queueName;

        public RabbitMqConsumer(RabbitMqConnector connection, string queueName)
        {
            _connection = connection;
            _connection.ModelCreated += OnModelCreated;
            _queueName = queueName;
        }

        public void Dispose()
        {
            _connection.Dispose();
        }

        public void Ack(ulong deliveryTag)
        {
            try
            {
                _connection.Logger.LogDebug($"RabbitMqConsumer: calling Ack on thread {Thread.CurrentThread.Name}.");

                _connection.Channel.BasicAck(deliveryTag, false);
            }
            catch (Exception ex)
            {
                _connection.Logger.LogError(ex, "RabbitMqConsumer: failed to call ACK!");
            }
        }

        public void Nack(ulong deliveryTag)
        {
            try
            {
                _connection.Logger.LogDebug($"RabbitMqConsumer: calling Nack on thread {Thread.CurrentThread.Name}.");

                _connection.Channel.BasicNack(deliveryTag, false, true);
            }
            catch (Exception ex)
            {
                _connection.Logger.LogError(ex, "RabbitMqConsumer: failed to call NACK!");
            }
        }

        public BasicGetResult Receive()
        {
            try
            {
                return _connection.Channel.BasicGet(_queueName, false);
            }
            catch (Exception ex)
            {
                _connection.Logger.LogError(ex, "RabbitMqConsumer: failed to call Get!");
                return null;
            }
        }

        private void OnModelCreated(object sender, ModelCreatedEventArgs args)
        {
            args.Channel.QueueDeclare(_queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
        }
    }
}