using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Orleans.Streams.RabbitMq
{
    // This is single threaded except the OnBasic* events and timeouts, which execute from other threads.
    internal class RabbitMqProducer : IRabbitMqProducer
    {
        private readonly RabbitMqConnector _connection;
        private readonly ConcurrentDictionary<ulong, MessageInFlight> messagesInFlight = new ConcurrentDictionary<ulong, MessageInFlight>();

        public RabbitMqProducer(RabbitMqConnector connection)
        {
            _connection = connection;
            _connection.BasicAcks += OnBasicAck;
            _connection.BasicNacks += OnBasicNack;
            _connection.ModelCreated += OnModelCreated;
        }

        private void OnModelCreated(object sender, ModelCreatedEventArgs e)
        {
            // This occurs on a new channel. Therefore previous in-flight messages will never be confirmed but may have duplicate sequence numbers.
            // Hence we clear them out. These messages will still time out like normal.
            messagesInFlight.Clear();
        }

        private void OnBasicAck(object sender, BasicAckEventArgs e)
        {
            if (!e.Multiple)
            {
                messagesInFlight.TryGetValue(e.DeliveryTag, out var message);
                message?.Complete((IModel)sender);
            }
            else
            {
                foreach (var message in messagesInFlight.Values)
                {
                    // Messages added while we are iterated might not be seen, but that is OK.
                    // No such message could be impacted by the acknowledgment, since that must happen
                    // after the message is sent, and messages are added to the queue before they get sent.
                    //
                    // A message may get removed by a timeout while iterating. This might or might not
                    // get skipped by the iterator. If it is skipped, that is fine, since it is already timed out.
                    // If not skipped, MessageInFlight is designed to handle that safely.

                    if (message.SequenceNumber <= e.DeliveryTag)
                    {
                        message.Complete((IModel)sender);
                    }
                }
            }
        }

        private void OnBasicNack(object sender, BasicNackEventArgs e)
        {
            if (!e.Multiple)
            {
                messagesInFlight.TryGetValue(e.DeliveryTag, out var message);
                message?.Nack((IModel)sender);
            }
            else
            {
                foreach (var message in messagesInFlight.Values)
                {
                    // See comment in Basic Ack event handler
                    if (message.SequenceNumber <= e.DeliveryTag)
                    {
                        message.Nack((IModel)sender);
                    }
                }
            }
        }

        public void Dispose()
        {
            _connection.Dispose();
        }

        public Task SendAsync(string exchange, string routingKey, byte[] message, bool shouldConfirm, bool persistent)
        {
            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            try
            {
                _connection.Logger.LogDebug($"RabbitMqProducer: calling Send on thread {Thread.CurrentThread.Name}.");
                
                var basicProperties = _connection.Channel.CreateBasicProperties();
                basicProperties.MessageId = Guid.NewGuid().ToString();
                basicProperties.Persistent = persistent;

                var channel = _connection.Channel;
                if (shouldConfirm)
                {
                    var seqNo = _connection.Channel.NextPublishSeqNo;
                    var mif = new MessageInFlight(tcs, channel, this);
                    this.AddMessageInFlight(mif, TimeSpan.FromSeconds(10));
                }

                channel.BasicPublish(exchange, routingKey, true, basicProperties, message);
                
                if (!shouldConfirm)
                {
                    return Task.CompletedTask;
                }
            }
            catch (Exception ex)
            {
                tcs.SetException(new RabbitMqException("RabbitMqProducer: Send failed!", ex));
            }
            return tcs.Task;
        }

        internal void AddMessageInFlight(MessageInFlight msg, TimeSpan timeout)
        {
            msg.SetupTimeout(timeout);
            messagesInFlight.TryAdd(msg.SequenceNumber, msg);
        }

        internal void RemoveMessageInFlight(MessageInFlight msg)
        {
            //It is normal for the message to not be present, so we are ignoring the return value.
            messagesInFlight.TryRemove(msg.SequenceNumber, out _);
        }

        internal sealed class MessageInFlight
        {
            public MessageInFlight(TaskCompletionSource<object> taskCompletionSource, IModel channel, RabbitMqProducer producer)
            {
                this.taskCompletionSource = taskCompletionSource;
                SequenceNumber = channel.NextPublishSeqNo;
                this.producer = producer;
                this.channel = channel;
                cancellationTokenSource = new CancellationTokenSource();
            }

            public void SetupTimeout(TimeSpan timeout)
            {
                cancellationTokenSource.Token.Register(Timeout, useSynchronizationContext: false);
                cancellationTokenSource.CancelAfter(timeout);
            }

            public void Complete(IModel channel)
            {
                // This check avoids a theoretical race condition where the channel
                // gets closed, and a new one opened and messages sent before some
                // ack events from the first channel are finished processing. 
                if(this.channel != channel) return; 

                cancellationTokenSource.Dispose();
                producer.RemoveMessageInFlight(this);
                taskCompletionSource.TrySetResult(null);
            }

            public void Nack(IModel channel)
            {
                // This check avoids a theoretical race condition where the channel
                // gets closed, and a new one opened and messages sent before some
                // ack events from the first channel are finished processing. 
                if (this.channel != channel) return;

                cancellationTokenSource.Dispose();
                producer.RemoveMessageInFlight(this);
                taskCompletionSource.TrySetException(new IOException("Nack Received."));
            }

            public void Timeout()
            {
                producer.RemoveMessageInFlight(this);
                taskCompletionSource.TrySetException(new IOException("Timed out waiting for acks, during RabbitMq publication."));
                cancellationTokenSource.Dispose();
            }

            private readonly CancellationTokenSource cancellationTokenSource;
            private readonly RabbitMqProducer producer;
            private readonly IModel channel;
            private readonly TaskCompletionSource<object> taskCompletionSource;

            public ulong SequenceNumber { get; }
        }
    }
}