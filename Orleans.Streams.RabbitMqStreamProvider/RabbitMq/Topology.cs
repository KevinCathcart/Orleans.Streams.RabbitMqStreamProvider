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
        public List<RabbitMqBinding> Bindings { get; set; }
    }

    public class RabbitMqBinding
    {
        public RabbitMqBindingType Type { get; set; }
        public string Source { get; set; }
        public string Destination { get; set; }
        public string RoutingKey { get; set; }
        public IDictionary<string, object> Arguments { get; set; }
    }

    public class RabbitMqExchangeProperties
    {
        public string Name { get; set; }
        public bool ShouldDeclare { get; set; }
        public bool PassiveDeclare { get; set; }
        public RabbitMqExchangeType Type { get; set; } = RabbitMqExchangeType.Direct;
        public bool Durable { get; set; } = true;
        public bool AutoDelete { get; set; }
        public IDictionary<string, object> Arguments { get; set; }
        public List<RabbitMqBinding> Bindings { get; set; }
    }

    public class RabbitMqExchangeType
    {
        // Standard
        public static readonly RabbitMqExchangeType Direct = new RabbitMqExchangeType("direct");
        public static readonly RabbitMqExchangeType Fanout = new RabbitMqExchangeType("fanout");
        public static readonly RabbitMqExchangeType Headers = new RabbitMqExchangeType("headers");
        public static readonly RabbitMqExchangeType Topic = new RabbitMqExchangeType("topic");

        // Plugins
        public static readonly RabbitMqExchangeType ConsistentHash = new RabbitMqExchangeType("x-consistent-hash");
        public static readonly RabbitMqExchangeType DelayedMessage = new RabbitMqExchangeType("x-delayed-message");
        public static readonly RabbitMqExchangeType JMSTopic = new RabbitMqExchangeType("x-jms-topic");
        public static readonly RabbitMqExchangeType LastValueCaching = new RabbitMqExchangeType("x-lvc");
        public static readonly RabbitMqExchangeType Management = new RabbitMqExchangeType("x-management");
        public static readonly RabbitMqExchangeType Random = new RabbitMqExchangeType("x-random");
        public static readonly RabbitMqExchangeType RecentHistory = new RabbitMqExchangeType("x-recent-history");
        public static readonly RabbitMqExchangeType ReverseTopic = new RabbitMqExchangeType("x-rtopic");
              
        public static RabbitMqExchangeType Custom(string type) => new RabbitMqExchangeType(type);

        private RabbitMqExchangeType(string type)
        {
            Value = type;
        }

        public string Value { get; }
    }

    public enum RabbitMqBindingType
    {
        Queue = 0,
        Exchange = 1
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

        public virtual RabbitMqExchangeProperties GetExchangeProperties(string exchangeName)
        {
            // The default does not use exchanges.

            // if you provide bindings here, they will be created when declaring this exchange.
            // Any referenced exchanges or queues will also be created.
            return new RabbitMqExchangeProperties 
            { 
                Name = exchangeName,
                ShouldDeclare = false
            };
        }

        internal static DefaultTopologyProvider Create(IServiceProvider services, string name)
        {
            return ActivatorUtilities.CreateInstance<DefaultTopologyProvider>(services, name);
        }
    }
}