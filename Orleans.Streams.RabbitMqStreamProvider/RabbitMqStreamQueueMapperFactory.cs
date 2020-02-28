using System;
using System.Collections.Concurrent;
using Orleans.Configuration;
using Orleans.Runtime;

namespace Orleans.Streams
{
    public interface IRabbitMqStreamQueueMapperFactory
    {
        IStreamQueueMapper Get(string providerName);
    }

    public class RabbitMqStreamQueueMapperFactory : IRabbitMqStreamQueueMapperFactory
    {
        private readonly IServiceProvider serviceProvider;
        private readonly ConcurrentDictionary<string, IStreamQueueMapper> map = new ConcurrentDictionary<string, IStreamQueueMapper>();

        public RabbitMqStreamQueueMapperFactory(IServiceProvider serviceProvider)
        {
            this.serviceProvider = serviceProvider;
        }

        public IStreamQueueMapper Get(string providerName)
        {
            return map.GetOrAdd(providerName, Create);
        }

        private IStreamQueueMapper Create(string providerName)
        {
            var mapper = serviceProvider.GetServiceByName<IStreamQueueMapper>(providerName);
            if (mapper != null)
            {
                return mapper;
            }

            // Default
            var rmqOptions = serviceProvider.GetOptionsByName<RabbitMqOptions>(providerName);
            var mapperOptions = new HashRingStreamQueueMapperOptions { TotalQueueCount = rmqOptions.UseQueuePartitioning ? rmqOptions.NumberOfQueues : 1 };
            return new HashRingBasedStreamQueueMapper(mapperOptions, rmqOptions.QueueNamePrefix);
        }
    }
}