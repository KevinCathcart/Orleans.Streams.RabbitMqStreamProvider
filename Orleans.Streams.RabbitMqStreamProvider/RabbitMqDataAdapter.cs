using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams.BatchContainer;
using Orleans.Streams.RabbitMq;

namespace Orleans.Streams
{

    public abstract class RabbitMqDataAdapterBase: IQueueDataAdapter<RabbitMqMessage, IBatchContainer>
    {
        private readonly IStreamQueueMapper _mapper;
        private readonly ITopologyProvider _topologyProvider;

        public RabbitMqDataAdapterBase(IStreamQueueMapper mapper, ITopologyProvider topologyProvider)
        {
            _mapper = mapper;
            _topologyProvider = topologyProvider;
        }

        public virtual IBatchContainer FromQueueMessage(RabbitMqMessage message, long sequenceId)
        {
            var batchContainer = Deserialize(message.Body);
            batchContainer.EventSequenceToken = new EventSequenceToken(sequenceId);
            return batchContainer;
        }

        public virtual RabbitMqMessage ToQueueMessage<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            var container = new RabbitMqBatchContainer(streamGuid, streamNamespace, events.Cast<object>().ToList(), requestContext);
            var serialized = Serialize(container);

            var queueName = _topologyProvider.GetNameForQueue(_mapper.GetQueueForStream(streamGuid, streamNamespace));

            var message = new RabbitMqMessage
            {
                Body = serialized,
                Exchange = string.Empty,
                RoutingKey = queueName,
                Persistent = true,
                ShouldConfirmPublish = true,
            };

            return message;
        }

        protected abstract RabbitMqBatchContainer Deserialize(byte[] data);
        protected abstract byte[] Serialize(RabbitMqBatchContainer container);
    }

    public class RabbitMqDataAdapter: RabbitMqDataAdapterBase
    {
        private readonly SerializationManager _serializatonManager;

        public RabbitMqDataAdapter(SerializationManager serializationManager, IStreamQueueMapper mapper, ITopologyProvider topologyProvider) :base(mapper, topologyProvider)
        {
            _serializatonManager = serializationManager;
        }

        protected override byte[] Serialize(RabbitMqBatchContainer container)
        {
            return _serializatonManager.SerializeToByteArray(container);
        }

        protected override RabbitMqBatchContainer Deserialize(byte[] data)
        {
            return _serializatonManager.DeserializeFromByteArray<RabbitMqBatchContainer>(data);
        }

        internal static RabbitMqDataAdapter Create(IServiceProvider services, string name)
        {
            var topologyFactory = services.GetRequiredService<ITopologyProviderFactory>();
            var mapperFactory = services.GetRequiredService<IRabbitMqStreamQueueMapperFactory>();
            return ActivatorUtilities.CreateInstance<RabbitMqDataAdapter>(services, mapperFactory.Get(name), topologyFactory.Get(name));
        }
    }
}