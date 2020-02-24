using System;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Orleans.Streams.RabbitMq
{
    internal class RabbitMqProducer : IRabbitMqProducer
    {
        private readonly RabbitMqConnector _connection;

        public RabbitMqProducer(RabbitMqConnector connection)
        {
            _connection = connection;
        }

        public void Dispose()
        {
            _connection.Dispose();
        }

        public void Send(string exchange, string routingKey, byte[] message)
        {
            try
            {
                _connection.Logger.LogDebug($"RabbitMqProducer: calling Send on thread {Thread.CurrentThread.Name}.");

                var basicProperties = _connection.Channel.CreateBasicProperties();
                basicProperties.MessageId = Guid.NewGuid().ToString();
                basicProperties.DeliveryMode = 2;   // persistent

                _connection.Channel.BasicPublish(exchange, routingKey, true, basicProperties, message);

                _connection.Channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(10));
            }
            catch (Exception ex)
            {
                throw new RabbitMqException("RabbitMqProducer: Send failed!", ex);
            }
        }
    }
}