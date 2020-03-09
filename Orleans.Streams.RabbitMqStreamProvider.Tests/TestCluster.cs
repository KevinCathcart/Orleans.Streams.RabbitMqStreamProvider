using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Hosting;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.TestingHost;

namespace RabbitMqStreamTests
{
    public static class TestClusterExtensions
    {
        public static async Task StartPullingAgents(this TestCluster cluster)
        {
            await cluster.Client
                .GetGrain<IManagementGrain>(0)
                .SendControlCommandToProvider(
                    typeof(PersistentStreamProvider).FullName,
                    Globals.StreamProviderNameDefault,
                    (int)PersistentStreamProviderCommand.StartAgents);

            await cluster.Client
                .GetGrain<IManagementGrain>(0)
                .SendControlCommandToProvider(
                    typeof(PersistentStreamProvider).FullName,
                    Globals.StreamProviderNameProtoBuf,
                    (int)PersistentStreamProviderCommand.StartAgents);
        }

        public static async Task StopPullingAgents(this TestCluster cluster)
        {
            await cluster.Client
                .GetGrain<IManagementGrain>(0)
                .SendControlCommandToProvider(
                    typeof(PersistentStreamProvider).FullName,
                    Globals.StreamProviderNameDefault,
                    (int)PersistentStreamProviderCommand.StopAgents);

            await cluster.Client
                .GetGrain<IManagementGrain>(0)
                .SendControlCommandToProvider(
                    typeof(PersistentStreamProvider).FullName,
                    Globals.StreamProviderNameProtoBuf,
                    (int)PersistentStreamProviderCommand.StopAgents);
        }
    }

    public class TestClusterConfigurator : ISiloConfigurator, IClientBuilderConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder
                .AddMemoryGrainStorage("PubSubStore")
                .AddRabbitMqStream(Globals.StreamProviderNameDefault, configurator =>
                {
                    configurator.ConfigureRabbitMq(host: "localhost", port: ToxiProxyHelpers.RmqProxyPort,
                        virtualHost: "/", user: "guest", password: "guest", queueName: Globals.StreamNameSpaceDefault);
                    configurator.ConfigureCacheSize(100);
                    configurator.ConfigureStreamPubSub(StreamPubSubType.ImplicitOnly);
                    configurator.ConfigurePullingAgent(ob => ob.Configure(
                        options =>
                        {
                            options.GetQueueMsgsTimerPeriod = TimeSpan.FromMilliseconds(100);
                        }));
                })
                .AddRabbitMqStream(Globals.StreamProviderNameProtoBuf, configurator =>
                {
                    configurator.ConfigureQueueDataAdapter(ProtoBufDataAdapter.Create);
                    configurator.ConfigureRabbitMq(host: "localhost", port: ToxiProxyHelpers.RmqProxyPort,
                        virtualHost: "/", user: "guest", password: "guest", queueName: Globals.StreamNameSpaceProtoBuf);
                    configurator.ConfigureCacheSize(100);
                    configurator.ConfigureStreamPubSub(StreamPubSubType.ImplicitOnly);
                    configurator.ConfigurePullingAgent(ob => ob.Configure(
                        options =>
                        {
                            options.GetQueueMsgsTimerPeriod = TimeSpan.FromMilliseconds(100);
                        }));
                })
                .ConfigureLogging(log => log
                    .ClearProviders()
                    .SetMinimumLevel(LogLevel.Trace)
                    .AddConsole()
                    .AddDebug());
        }

        public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
        {
            clientBuilder
                .AddRabbitMqStream(Globals.StreamProviderNameDefault, configurator =>
                {
                    configurator.ConfigureRabbitMq(host: "localhost", port: ToxiProxyHelpers.RmqProxyPort,
                        virtualHost: "/", user: "guest", password: "guest", queueName: Globals.StreamNameSpaceDefault);
                })
                .AddRabbitMqStream(Globals.StreamProviderNameProtoBuf, configurator =>
                {
                    configurator.ConfigureQueueDataAdapter(ProtoBufDataAdapter.Create);
                    configurator.ConfigureRabbitMq(host: "localhost", port: ToxiProxyHelpers.RmqProxyPort,
                        virtualHost: "/", user: "guest", password: "guest", queueName: Globals.StreamNameSpaceProtoBuf);
                })
                .ConfigureLogging(log => log
                    .ClearProviders()
                    .SetMinimumLevel(LogLevel.Trace)
                    .AddConsole()
                    .AddDebug());
        }
    }
}
