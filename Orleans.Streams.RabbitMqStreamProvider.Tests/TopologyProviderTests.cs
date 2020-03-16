using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NUnit.Framework;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Serialization;
using Orleans.Streams;
using Orleans.Streams.RabbitMq;

using Orleans.TestingHost;

namespace RabbitMqStreamTests
{
    [TestFixture]
    class TopologyProviderTests
    {
        public static readonly IReadOnlyList<string> CustomQueueNames = new List<string> { "test-CustomName", "test-OtherCustomName" };

        [Test]
        public async Task TestCustomNameTopology()
        {
            RmqHelpers.DeleteQueues(CustomQueueNames);
            using var _cluster = new TestClusterBuilder()
                .AddSiloBuilderConfigurator<CustomNameTopologyClusterConfigurator>()
                .AddClientBuilderConfigurator<CustomNameTopologyClusterConfigurator>()
                .Build();

            await _cluster.DeployAsync();
            await _cluster.TestRmqStreamProviderOnFly(
                setupProxy: null,
                nMessages: 1000,
                itersToWait: 20);
        }

        [Test]
        public async Task TestExchangeBasedTopology()
        {
            using var _cluster = new TestClusterBuilder()
                .AddSiloBuilderConfigurator<ExchangeTopologyClusterConfigurator>()
                .AddClientBuilderConfigurator<ExchangeTopologyClusterConfigurator>()
                .Build();

            await _cluster.DeployAsync();
            await _cluster.TestRmqStreamProviderOnFly(
                setupProxy: null,
                nMessages: 1000,
                itersToWait: 20);
        }

        public class CustomNameTopologyClusterConfigurator : ISiloConfigurator, IClientBuilderConfigurator
        {
            public void Configure(ISiloBuilder siloBuilder)
            {
                siloBuilder
                    .AddMemoryGrainStorage("PubSubStore")
                    .AddRabbitMqStream(Globals.StreamProviderNameDefault, configurator =>
                    {
                        configurator.ConfigureRabbitMq(ob =>
                            ob.Configure(options =>
                            {
                                options.QueueNamePrefix = Globals.StreamNameSpaceDefault;
                                options.NumberOfQueues = CustomQueueNames.Count;
                            }));
                        configurator.ConfigureCacheSize(100);
                        configurator.ConfigureTopologyProvider(CustomNameTopologyProvider.Create);
                    })
                    .ConfigureLogging(log => log.AddTestDebugLogging());
            }

            public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
            {
                clientBuilder.ConfigureLogging(log => log.AddTestDebugLogging());
            }
        }

        public class ExchangeTopologyClusterConfigurator : ISiloConfigurator, IClientBuilderConfigurator
        {
            public void Configure(ISiloBuilder siloBuilder)
            {
                siloBuilder
                    .AddMemoryGrainStorage("PubSubStore")
                    .AddRabbitMqStream(Globals.StreamProviderNameDefault, configurator =>
                    {
                        configurator.ConfigureRabbitMq(ob =>
                                ob.Configure(options =>
                                {
                                    options.QueueNamePrefix = "test-exclusive";
                                    options.NumberOfQueues = 3;
                                }));
                        configurator.ConfigureCacheSize(100);
                        configurator.ConfigureTopologyProvider(ExchangeBasedTopologyProvider.Create);
                        configurator.ConfigureQueueDataAdapter(ExchangeDataAdapter.Create);
                    })
                    .ConfigureLogging(log => log.AddTestDebugLogging());
            }

            public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
            {
                clientBuilder.ConfigureLogging(log => log.AddTestDebugLogging());
            }
        }


        public class CustomNameTopologyProvider : DefaultTopologyProvider
        {
            public CustomNameTopologyProvider(string providerName, IOptionsMonitor<RabbitMqOptions> optionsAccessor, IOptionsMonitor<SimpleQueueCacheOptions> cacheOptionsAccessor) : base(providerName, optionsAccessor, cacheOptionsAccessor)
            {
            }

            public override string GetNameForQueue(QueueId queueId)
            {
                return CustomQueueNames[(int)queueId.GetNumericId()];
            }

            internal static CustomNameTopologyProvider Create(IServiceProvider services, string name)
            {
                return ActivatorUtilities.CreateInstance<CustomNameTopologyProvider>(services, name);
            }
        }

        public class ExchangeBasedTopologyProvider : ITopologyProvider
        {
            private readonly RabbitMqOptions options;
            private readonly SimpleQueueCacheOptions cacheOptions;

            public ExchangeBasedTopologyProvider(string providerName, IOptionsMonitor<RabbitMqOptions> optionsAccessor, IOptionsMonitor<SimpleQueueCacheOptions> cacheOptionsAccessor)
            {
                this.options = optionsAccessor.Get(providerName);
                this.cacheOptions = cacheOptionsAccessor.Get(providerName);
            }

            public virtual RabbitMqQueueProperties GetQueueProperties(string queueName)
            {
                return new RabbitMqQueueProperties
                {
                    Name = queueName,
                    ShouldDeclare = true,
                    PassiveDeclare = false,
                    Durable = true,
                    AutoDelete = true,
                    Exclusive = true,
                    PrefetchLimit = (ushort)cacheOptions.CacheSize,
                    Bindings = new List<RabbitMqBinding>
                    {
                        new RabbitMqBinding
                        {
                            Type = RabbitMqBindingType.Queue,
                            Source = queueName, //using same name for queue and exchange to enable proper sharding
                            Destination = queueName
                        }
                    }
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
                if(exchangeName==string.Empty)
                {
                    return new RabbitMqExchangeProperties
                    {
                        Name = exchangeName,
                        ShouldDeclare = false
                    };
                }

                return new RabbitMqExchangeProperties
                {
                    Name = exchangeName,
                    ShouldDeclare = true,
                    AutoDelete = true,
                    Type = RabbitMqExchangeType.Fanout
                };
            }

            internal static ExchangeBasedTopologyProvider Create(IServiceProvider services, string name)
            {
                return ActivatorUtilities.CreateInstance<ExchangeBasedTopologyProvider>(services, name);
            }
        }

        public class ExchangeDataAdapter : RabbitMqDataAdapter
        {
            public ExchangeDataAdapter(SerializationManager serializationManager, IStreamQueueMapper mapper, ITopologyProvider topologyProvider) :
                base(serializationManager, mapper, topologyProvider)
            { }

            public override RabbitMqMessage ToQueueMessage<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
            {
                var message = base.ToQueueMessage(streamGuid, streamNamespace, events, token, requestContext);
                message.Exchange = message.RoutingKey; // Set the exchange name to be the name of the queue the base class was routing to.
                message.RoutingKey = "";
                return message;
            }

            internal static ExchangeDataAdapter Create(IServiceProvider services, string name)
            {
                var topologyFactory = services.GetRequiredService<ITopologyProviderFactory>();
                var mapperFactory = services.GetRequiredService<IRabbitMqStreamQueueMapperFactory>();
                return ActivatorUtilities.CreateInstance<ExchangeDataAdapter>(services, mapperFactory.Get(name), topologyFactory.Get(name));
            }
        }
    }
}
