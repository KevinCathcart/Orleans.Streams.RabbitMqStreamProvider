using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Orleans.Streams.RabbitMq
{
    internal class RabbitMqConsumer : IRabbitMqConsumer
    {
        private readonly RabbitMqConnector _connection;
        private readonly RabbitMqQueueProperties _queueProperties;

        public RabbitMqConsumer(RabbitMqConnector connection, RabbitMqQueueProperties queueProperties)
        {
            _connection = connection;
            _connection.ModelCreated += OnModelCreated;
            _queueProperties = queueProperties;
        }

        public void Dispose()
        {
            // Deliberate fire and forget. We are dispatching Dispose on the connection's
            // TaskScheduler to ensure we don't dispose in the middle of some method call.
            _connection.RunOnScheduler(state => ((RabbitMqConnector)state).Dispose(), _connection);
        }

        public Task AckAsync(object channel, ulong deliveryTag, bool multiple)
        {
            return _connection.RunOnScheduler(() => Ack(channel, deliveryTag, multiple));
        }

        private void Ack(object channel, ulong deliveryTag, bool multiple)
        {
            try
            {
                if(_connection.Logger.IsEnabled(LogLevel.Debug)) _connection.Logger.LogDebug($"RabbitMqConsumer: calling Ack on thread {Thread.CurrentThread.Name}.");

                var currentChannel = _connection.Channel;
                if (currentChannel == null) return; // Has been disposed

                if (channel != currentChannel)
                {
                    _connection.Logger.LogDebug($"RabbitMqConsumer: tried to Ack on old channel. Ignored.");
                    return;
                }

                currentChannel.BasicAck(deliveryTag, multiple);
            }
            catch (Exception ex)
            {
                _connection.Logger.LogError(ex, "RabbitMqConsumer: failed to call ACK!");
            }
        }

        public Task NackAsync(object channel, ulong deliveryTag)
        {
            return _connection.RunOnScheduler(() => Nack(channel, deliveryTag));
        }

        private void Nack(object channel, ulong deliveryTag)
        {
            try
            {
                if (_connection.Logger.IsEnabled(LogLevel.Debug)) _connection.Logger.LogDebug($"RabbitMqConsumer: calling Nack on thread {Thread.CurrentThread.Name}.");

                var currentChannel = _connection.Channel;
                if (currentChannel == null) return; // Has been disposed

                if (channel != currentChannel)
                {
                    _connection.Logger.LogDebug($"RabbitMqConsumer: tried to Nack on old channel. Ignored.");
                    return;
                }

                currentChannel.BasicNack(deliveryTag, multiple:false, requeue: true);
            }
            catch (Exception ex)
            {
                _connection.Logger.LogError(ex, "RabbitMqConsumer: failed to call NACK!");
            }
        }

        public Task<RabbitMqMessage> ReceiveAsync()
        {
            return _connection.RunOnScheduler(self => ((RabbitMqConsumer)self).ReceiveImpl(), this);
        }
        private RabbitMqMessage ReceiveImpl()
        {
            try
            {
                IModel currentChannel = _connection.Channel;
                if (currentChannel == null) return null; // Has been disposed
                BasicGetResult result = currentChannel.BasicGet(_queueProperties.Name, false);
                if (result == null) return null;
                return Convert(result, currentChannel);
            }
            catch (Exception ex)
            {
                _connection.Logger.LogError(ex, "RabbitMqConsumer: failed to call Get!");
                return null;
            }
        }

        private RabbitMqMessage Convert(BasicGetResult result, object channel)
        {
            return new RabbitMqMessage
            {
                AppId = result.BasicProperties.AppId,
                Body = result.Body,
                Channel = channel,
                ClusterId = result.BasicProperties.ClusterId,
                ContentEncoding = result.BasicProperties.ContentEncoding,
                ContentType = result.BasicProperties.ContentType,
                CorrelationId = result.BasicProperties.CorrelationId,
                DeliveryTag = result.DeliveryTag,
                Exchange = result.Exchange,
                Expiration = result.BasicProperties.Expiration,
                Headers = result.BasicProperties.Headers,
                MessageId = result.BasicProperties.MessageId,
                Persistent = result.BasicProperties.Persistent,
                Priority = result.BasicProperties.Priority,
                Redelivered = result.Redelivered,
                ReplyTo = result.BasicProperties.ReplyTo,
                RoutingKey = result.RoutingKey,
                Timestamp = result.BasicProperties.Timestamp.UnixTime,
                Type = result.BasicProperties.Type,
                UserId = result.BasicProperties.UserId,
            };
        }

        private void OnModelCreated(object sender, ModelCreatedEventArgs args)
        {
            if (_queueProperties.ShouldDeclare)
            {
                args.Channel.QueueDeclare(
                    _queueProperties.Name,
                    _queueProperties.Durable,
                    _queueProperties.Exclusive,
                    _queueProperties.AutoDelete,
                    _queueProperties.Arguments);
            }
        }
    }
}