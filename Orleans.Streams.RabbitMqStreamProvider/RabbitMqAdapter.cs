using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Channels;
using Orleans.Configuration;
using Orleans.Streams.RabbitMq;

using ThreadSafeChannel = System.Threading.Channels.Channel;

namespace Orleans.Streams
{
    /// <summary>
    /// For RMQ client, it is necessary the Model (channel) is not accessed by multiple threads at once, because with each such access,
    /// the channel gets closed - this is a limitation of RMQ client, which unfortunately causes message loss.
    /// Here we handle it by creating new connection for each receiver which guarantees no overlapping calls from different threads.
    /// The real issue comes in publishing - here we need to identify the connections by thread!
    /// Otherwise it would cause a lot of trouble when publishing messages from StatelessWorkers which can run in parallel, thus
    /// overlapping calls from different threads would occur frequently.
    /// </summary>
    internal class RabbitMqAdapter : IQueueAdapter
    {
        private readonly IQueueDataAdapter<RabbitMqMessage, IEnumerable<IBatchContainer>> _dataAdapter;
        private readonly IStreamQueueMapper _mapper;
        private readonly ThreadLocal<IRabbitMqProducer> _producer; 
        private readonly IRabbitMqConnectorFactory _rmqConnectorFactory;
        private readonly RabbitMqOptions _rmqOptions;

        private readonly Channel<Func<IRabbitMqProducer, Task>> _chTask = ThreadSafeChannel.CreateUnbounded<Func<IRabbitMqProducer, Task>>(new UnboundedChannelOptions()
        {
            SingleReader = false,
            SingleWriter = false,
        });

        public RabbitMqAdapter(RabbitMqOptions rmqOptions, IQueueDataAdapter<RabbitMqMessage, IEnumerable<IBatchContainer>> dataAdapter, string providerName, IStreamQueueMapper mapper, IRabbitMqConnectorFactory rmqConnectorFactory)
        {
            _dataAdapter = dataAdapter;
            Name = providerName;
            _mapper = mapper;
            _rmqConnectorFactory = rmqConnectorFactory;
            _rmqOptions = rmqOptions;
            _producer = new ThreadLocal<IRabbitMqProducer>(() => _rmqConnectorFactory.CreateProducer());

            for( int i = 0; i < Math.Max(Environment.ProcessorCount / 2, 1); i++)
            {
                Task.Run(Runner);
            }
        }

        public string Name { get; }
        public bool IsRewindable => false;
        public StreamProviderDirection Direction => _rmqOptions.Direction;
        public IQueueAdapterReceiver CreateReceiver(QueueId queueId) => new RabbitMqAdapterReceiver(_rmqConnectorFactory, queueId, _mapper, _dataAdapter, _rmqOptions);

        public Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            if (token != null) throw new ArgumentException("RabbitMq stream provider does not support non-null StreamSequenceToken.", nameof(token));

            RabbitMqMessage message = _dataAdapter.ToQueueMessage(streamGuid, streamNamespace, events, token, requestContext);

            TaskCompletionSource<object> tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            _chTask.Writer.TryWrite(async (producer) =>
            {
                try
                {
                    await producer.SendAsync(message);
                    tcs.SetResult(null);
                }
                catch(Exception ex)
                {
                    tcs.SetException(ex);
                }
            });
            return tcs.Task;
        }

        private async Task Runner()
        {
            var reader = _chTask.Reader;
            var producer = _producer.Value;
            while (await reader.WaitToReadAsync())
            {
                while (reader.TryRead(out Func<IRabbitMqProducer, Task> cb))
                {
                    try
                    {
                        await cb(producer);
                    }
                    catch
                    {
                    }
                }
            }
        }
    }
}