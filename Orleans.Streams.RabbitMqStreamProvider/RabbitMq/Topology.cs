using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;

namespace Orleans.Streams.RabbitMq
{
    public class RabbitMqQueueProperties
    {
        public string Name { get; set; }
        public bool ShouldDeclare { get; set; }
        public bool PassiveDeclare { get; set; }
        public bool Durable { get; set; }
        public bool Exclusive { get; set; }
        public bool AutoDelete { get; set; }
        public IDictionary<string, object> Arguments { get; set; }
    }

    public class DefaultTopologyProvider : ITopologyProvider
    {
        private readonly RabbitMqOptions options;

        public DefaultTopologyProvider(string providerName, IOptionsMonitor<RabbitMqOptions> optionsAccessor)
        {
            this.options = optionsAccessor.Get(providerName);
        }

        public virtual RabbitMqQueueProperties GetQueueProperties(string queueName)
        {

            // you can declare bindings here to have them created when declaring this
            // queue. Referenced exchanges will also be declared.
            return new RabbitMqQueueProperties
            {
                Name = queueName,
                ShouldDeclare = true,
                PassiveDeclare = false,
                Durable = true,
                Exclusive = false,
                AutoDelete = false,
            };
        }

        public virtual string GetNameForQueue(QueueId queueId)
        {
            return options.UseQueuePartitioning
                ? $"{queueId.GetStringNamePrefix()}-{queueId.GetNumericId()}"
                : queueId.GetStringNamePrefix();
        }

        internal static DefaultTopologyProvider Create(IServiceProvider services, string name)
        {
            return ActivatorUtilities.CreateInstance<DefaultTopologyProvider>(services, name);
        }
    }
}