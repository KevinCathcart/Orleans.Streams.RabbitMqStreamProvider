using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams.RabbitMq;

namespace Orleans.Streams
{
    public class RabbitMqAdapterFactory : IQueueAdapterFactory
    {
        private readonly IQueueAdapterCache _cache;
        private readonly IStreamQueueMapper _mapper;
        private readonly Task<IStreamFailureHandler> _failureHandler;
        private readonly IQueueAdapter _adapter;
        
        public RabbitMqAdapterFactory(
            string providerName,
            IOptionsMonitor<RabbitMqOptions> rmqOptionsAccessor,
            IOptionsMonitor<SimpleQueueCacheOptions> cachingOptionsAccessor,
            IServiceProvider serviceProvider,
            ILoggerFactory loggerFactory,
            IRabbitMqStreamQueueMapperFactory streamQueueMapperFactory,
            ITopologyProviderFactory topologyProviderFactory)
        {

            if (string.IsNullOrEmpty(providerName)) throw new ArgumentNullException(nameof(providerName));
            if (rmqOptionsAccessor == null) throw new ArgumentNullException(nameof(rmqOptionsAccessor));
            if (cachingOptionsAccessor == null) throw new ArgumentNullException(nameof(cachingOptionsAccessor));
            if (serviceProvider == null) throw new ArgumentNullException(nameof(serviceProvider));
            if (loggerFactory == null) throw new ArgumentNullException(nameof(loggerFactory));

            var rmqOptions = rmqOptionsAccessor.Get(providerName);
            var cachingOptions = cachingOptionsAccessor.Get(providerName);

            _cache = new SimpleQueueAdapterCache(cachingOptions, providerName, loggerFactory);
            _mapper = streamQueueMapperFactory.Get(providerName);
            _failureHandler = Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler(false));

            var topologyProvider = topologyProviderFactory.Get(providerName);

            var dataAdapter = serviceProvider.GetServiceByName<IQueueDataAdapter<RabbitMqMessage, IEnumerable<IBatchContainer>>>(providerName) ??
                    RabbitMqDataAdapter.Create(serviceProvider, providerName);

            _adapter = new RabbitMqAdapter(rmqOptions, dataAdapter, providerName, loggerFactory, topologyProvider);
        }

        public Task<IQueueAdapter> CreateAdapter() => Task.FromResult(_adapter);
        public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId) => _failureHandler;
        public IQueueAdapterCache GetQueueAdapterCache() => _cache;
        public IStreamQueueMapper GetStreamQueueMapper() => _mapper;

        public static RabbitMqAdapterFactory Create(IServiceProvider services, string name)
            => ActivatorUtilities.CreateInstance<RabbitMqAdapterFactory>(
                services,
                name);
    }
}