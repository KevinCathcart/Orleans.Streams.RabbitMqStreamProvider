﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Streams.RabbitMq;

namespace Orleans.Streams
{
    internal class RabbitMqAdapterReceiver : IQueueAdapterReceiver
    {
        private readonly IRabbitMqConnectorFactory _rmqConnectorFactory;
        private readonly QueueId _queueId;
        private readonly IStreamQueueMapper _mapper;
        private readonly IQueueDataAdapter<RabbitMqMessage, IEnumerable<IBatchContainer>> _dataAdapter;
        private readonly ILogger _logger;
        private long _sequenceId;
        private IRabbitMqConsumer _consumer;
        private readonly List<PendingDelivery> pending;
        private Queue<IBatchContainer> _currentGroup;

        public RabbitMqAdapterReceiver(IRabbitMqConnectorFactory rmqConnectorFactory, QueueId queueId, IStreamQueueMapper mapper, IQueueDataAdapter<RabbitMqMessage, IEnumerable<IBatchContainer>> dataAdapter)
        {
            _rmqConnectorFactory = rmqConnectorFactory;
            _queueId = queueId;
            _mapper = mapper;
            _dataAdapter = dataAdapter;
            _logger = _rmqConnectorFactory.LoggerFactory.CreateLogger($"{typeof(RabbitMqAdapterReceiver).FullName}.{queueId}");
            _sequenceId = 0;
            pending = new List<PendingDelivery>();
            _currentGroup = new Queue<IBatchContainer>();
        }

        public Task Initialize(TimeSpan timeout)
        {
            _consumer = _rmqConnectorFactory.CreateConsumer(_queueId);
            return Task.CompletedTask;
        }

        public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            var consumer = _consumer; // store direct ref, in case we are somehow asked to shutdown while we are receiving.
            if (consumer == null) return new List<IBatchContainer>();

            var multibatch = new List<IBatchContainer>();
            for (int count = 0; count < maxCount || maxCount == QueueAdapterConstants.UNLIMITED_GET_QUEUE_MSG; count++)
            {
                if (!await GetNextGroupIfNeeded(consumer))
                {
                    // Ran out of messages.
                    break;
                }

                var batch = _currentGroup.Dequeue();
                multibatch.Add(batch);
            }

            return multibatch;
        }

        private async Task<bool> GetNextGroupIfNeeded(IRabbitMqConsumer consumer)
        {
            // Repeat until we run out of messages, or we have set _currentGroup to a non-empty collection.
            while (_currentGroup.Count == 0)
            {
                var item = await consumer.ReceiveAsync();
                if (item == null) return false;

                IEnumerable<IBatchContainer> batches;
                try
                {
                    batches =_dataAdapter.FromQueueMessage(item, _sequenceId++).ToList();
                    _sequenceId = batches.LastOrDefault()?.SequenceToken?.SequenceNumber ?? _sequenceId;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "GetQueueMessagesAsync: failed to deserialize the message! The message will be thrown away.");
                    await consumer.NackAsync(item.Channel, item.DeliveryTag, requeue: false);
                    continue;
                }

                // Filter out any decoded batches whose streams that do not belong to the queue. (Can happen with custom data adapters,
                // and unusual topologies. If it does we don't want to try to deliver those because those messages should also appear
                // in the correct queue, and thus would get needlessly double delivered.)
                var filteredBatches = batches.Where(b => _mapper.GetQueueForStream(b.StreamGuid, b.StreamNamespace).Equals(_queueId)).ToList();

                if (filteredBatches.Count == 0)
                {
                    // If a RabbitMQ message maps to zero Orleans messages, then we can acknowledge it immediately.
                    await consumer.AckAsync(item.Channel, item.DeliveryTag, multiple: false);
                }
                else
                {
                    foreach (var batch in filteredBatches)
                    {
                        pending.Add(new PendingDelivery(batch.SequenceToken, item.DeliveryTag, item.Channel, item.RequeueOnFailure));
                    }
                }

                _currentGroup = new Queue<IBatchContainer>(filteredBatches);
            }

            return true;
        }

        public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            var consumer = _consumer; // store direct ref, in case we are somehow asked to shutdown while we are receiving.
            if (messages.Count == 0 || consumer == null) return;

            List<StreamSequenceToken> deliveredTokens = messages.Select(message => message.SequenceToken).ToList();

            StreamSequenceToken newest = deliveredTokens.Max();

            newest = HandlePartiallyProcessedGroup(deliveredTokens, newest);
            if(newest == null)
            {
                return;
            }

            // finalize all pending messages at or before the newest
            List<PendingDelivery> finalizedDeliveries = pending
                .Where(pendingDelivery => !pendingDelivery.Token.Newer(newest))
                .ToList();

            // remove all finalized deliveries from pending, regardless of if it was delivered or not.
            pending.RemoveRange(0, finalizedDeliveries.Count);

            var groups = finalizedDeliveries.GroupBy(x => new { x.Channel, x.DeliveryTag});

            var groupsByDeliveryStatus = groups.ToLookup(
                g => g.All(m => deliveredTokens.Contains(m.Token)),
                g => new
                {
                    g.Key.Channel,
                    g.Key.DeliveryTag,
                    // First is safe because and all messages in the same group will have the same value.
                    g.First().RequeueOnFailure
                });

            var incompletelyDeliveredGroups = groupsByDeliveryStatus[false];

            // Nack any message groups that were not completely delivered
            foreach (var group in incompletelyDeliveredGroups)
            {
                if (_logger.IsEnabled(LogLevel.Debug)) _logger.LogDebug("MessagesDeliveredAsync NACK #{deliveryTag} Requeue: {requeue}", group.DeliveryTag, group.RequeueOnFailure);
                await consumer.NackAsync(group.Channel, group.DeliveryTag, group.RequeueOnFailure);
            }

            var fullyDeliveredGroups = groupsByDeliveryStatus[true];

            // Ack all the rest
            var maxTagsByChannel = fullyDeliveredGroups
                .GroupBy(m => m.Channel)
                .Select(g => new { Channel = g.Key, DeliveryTag = g.Max(m => m.DeliveryTag) });

            foreach (var maxTag in maxTagsByChannel)
            {
                if (_logger.IsEnabled(LogLevel.Debug)) _logger.LogDebug("MessagesDeliveredAsync ACK #{deliveryTag}", maxTag.DeliveryTag);
                await consumer.AckAsync(maxTag.Channel, maxTag.DeliveryTag, multiple: true);
            }
        }

        private StreamSequenceToken HandlePartiallyProcessedGroup(List<StreamSequenceToken> deliveredTokens, StreamSequenceToken newest)
        {
            // If newest is part of a group of batches that came from a single rabbit message and not all of them have tokens <= newest,
            // then adjust newest to be largest value not part of that group.
            PendingDelivery top = pending.First(m => m.Token == newest);
            List<PendingDelivery> topGroup = pending.Where(m => m.Channel == top.Channel && m.DeliveryTag == top.DeliveryTag).ToList();
            if (topGroup.Any(x => x.Token.Newer(newest)))
            {
                var remainder = pending.Where(x => !x.Token.Newer(newest)).Where(x => !topGroup.Contains(x)).ToList();
                if (!remainder.Any())
                {
                    // If topGroup is the only group with tokens <= newest, remove any delivered messages from
                    // pending, and return early. (We need to keep any unsuccessfully delivered messages so that we can
                    // Nack the group once it is finished).
                    var delivered = topGroup.Where(msg => deliveredTokens.Contains(msg.Token)).ToList();
                    pending.RemoveAll(delivered.Contains);
                    return null;
                }
                newest = topGroup.Max(x => x.Token);
            }
            return newest;
        }

        public Task Shutdown(TimeSpan timeout)
        {
            var consumer = _consumer;
            _consumer = null;
            consumer?.Dispose();
            return Task.CompletedTask;
        }

        private class PendingDelivery
        {
            public PendingDelivery(StreamSequenceToken token, ulong deliveryTag, object channel, bool requeueOnFailure)
            {
                this.Token = token;
                this.DeliveryTag = deliveryTag;
                this.Channel = channel;
                this.RequeueOnFailure = requeueOnFailure;
            }

            public ulong DeliveryTag { get; }
            public object Channel { get; }
            public StreamSequenceToken Token { get; }
            public bool RequeueOnFailure { get;}
        }
    }
}