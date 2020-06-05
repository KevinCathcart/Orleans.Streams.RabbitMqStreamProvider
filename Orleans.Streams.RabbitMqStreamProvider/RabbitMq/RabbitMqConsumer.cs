using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Orleans.Streams.RabbitMq
{
    internal class RabbitMqConsumer : IRabbitMqConsumer
    {
        private readonly IRabbitMqConnector _connection;
        private readonly RabbitMqQueueProperties _queueProperties;
        private readonly DeclarationHelper _declarationHelper;

        private readonly ConcurrentQueue<RabbitMqMessage> queue = new ConcurrentQueue<RabbitMqMessage>();

        public RabbitMqConsumer(IRabbitMqConnector connection, string queueName, ITopologyProvider topologyProvider)
        {
            _connection = connection;
            _connection.ModelCreated += OnModelCreated;
            _queueProperties = topologyProvider.GetQueueProperties(queueName);

            _declarationHelper = new DeclarationHelper(topologyProvider);
        }

        public void Dispose()
        {
            // Deliberate fire and forget. We are dispatching Dispose on the connection's
            // TaskScheduler to ensure we don't dispose in the middle of some method call.
            _connection.RunOnScheduler(state => ((IRabbitMqConnector)state).Dispose(), _connection);
        }

        public Task AckAsync(object channel, ulong deliveryTag, bool multiple)
        {
            return _connection.RunOnScheduler(() => Ack(channel, deliveryTag, multiple));
        }

        private void Ack(object channel, ulong deliveryTag, bool multiple)
        {
            try
            {
                if (_connection.Logger.IsEnabled(LogLevel.Debug)) _connection.Logger.LogDebug($"RabbitMqConsumer: calling Ack.");

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

        public Task NackAsync(object channel, ulong deliveryTag, bool requeue)
        {
            return _connection.RunOnScheduler(() => Nack(channel, deliveryTag, requeue));
        }

        private void Nack(object channel, ulong deliveryTag, bool requeue)
        {
            try
            {
                if (_connection.Logger.IsEnabled(LogLevel.Debug)) _connection.Logger.LogDebug($"RabbitMqConsumer: calling Nack.");

                var currentChannel = _connection.Channel;
                if (currentChannel == null) return; // Has been disposed

                if (channel != currentChannel)
                {
                    _connection.Logger.LogDebug($"RabbitMqConsumer: tried to Nack on old channel. Ignored.");
                    return;
                }

                currentChannel.BasicNack(deliveryTag, multiple: false, requeue);
            }
            catch (Exception ex)
            {
                _connection.Logger.LogError(ex, "RabbitMqConsumer: failed to call NACK!");
            }
        }

        public async Task<RabbitMqMessage> ReceiveAsync()
        {
            if (!queue.TryDequeue(out var result))
            {
                // if we are out of messages, it is probably just that we are all caught up, but
                // we call EnsureChannelAvailable, to ensure the channel is open.
                await _connection.RunOnScheduler(state => ((IRabbitMqConnector)state).EnsureChannelAvailable(), _connection);
            }
            else
            {
                //Force asynchronous completion, since otherwise too much code in the pulling agent can end up running synchronously
                await Task.Yield();
            }
            return result;
        }

        private RabbitMqMessage Convert(BasicDeliverEventArgs result, object channel)
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
            IModel channel = args.Channel;
            if (_queueProperties.ShouldDeclare)
            {
                _declarationHelper.Clear();
                _declarationHelper.DeclareQueue(_queueProperties.Name, channel);
            }

            channel.BasicQos(0, _queueProperties.PrefetchLimit, false);

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (ch, ea) =>
            {
                var msg = Convert(ea, ((IBasicConsumer)ch).Model);
                queue.Enqueue(msg);
            };
            channel.BasicConsume(_queueProperties.Name, false, consumer);
        }
    }
}