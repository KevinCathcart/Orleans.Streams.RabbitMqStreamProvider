using System;
using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;
using Orleans.Streams.RabbitMq;

namespace Orleans.Streams
{
    public interface ITopologyProviderFactory
    {
        ITopologyProvider Get(string providerName);
    }

    public class TopologyProviderFactory : ITopologyProviderFactory
    {
        private readonly IServiceProvider serviceProvider;
        private readonly ConcurrentDictionary<string, ITopologyProvider> map = new ConcurrentDictionary<string, ITopologyProvider>();

        public TopologyProviderFactory(IServiceProvider serviceProvider)
        {
            this.serviceProvider = serviceProvider;
        }

        public ITopologyProvider Get(string providerName)
        {
            return map.GetOrAdd(providerName, Create);
        }

        private ITopologyProvider Create(string providerName)
        {
            return serviceProvider.GetServiceByName<ITopologyProvider>(providerName) ??
                DefaultTopologyProvider.Create(serviceProvider, providerName);
        }
    }
}