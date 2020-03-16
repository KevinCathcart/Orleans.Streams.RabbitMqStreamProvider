using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Concurrency;
using Orleans.Streams;

namespace RabbitMqStreamTests
{
    public interface IReceiverGrain : IGrainWithGuidKey
    {
    }

    public interface IProtoBufReceiverGrain : IGrainWithGuidKey
    {
    }

    [ImplicitStreamSubscription(Globals.StreamNameSpaceDefault)]
    public class ReceiverGrain : ReceiverGrainBase, IReceiverGrain
    {
        public override string ProviderName => Globals.StreamProviderNameDefault;

        public override string Namespace => Globals.StreamNameSpaceDefault;
    }

    [ImplicitStreamSubscription(Globals.StreamNameSpaceProtoBuf)]
    public class ProtoBufRecieverGrain : ReceiverGrainBase, IProtoBufReceiverGrain
    {
        public override string ProviderName => Globals.StreamProviderNameProtoBuf;

        public override string Namespace => Globals.StreamNameSpaceProtoBuf;
    }

    public abstract class ReceiverGrainBase : Grain
    {
        private ILogger _logger;
        private StreamSubscriptionHandle<Message> _subscriptionDefault;

        public abstract string ProviderName { get; }
        public abstract string Namespace { get; }

        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            _logger = ServiceProvider.GetRequiredService<ILoggerFactory>().CreateLogger($"{GetType().FullName}.{this.GetPrimaryKey()}");
            _logger.LogInformation($"OnActivateAsync [{RuntimeIdentity}],[{IdentityString}][{this.GetPrimaryKey()}] from thread {Thread.CurrentThread.Name}");
            _subscriptionDefault = await GetStreamProvider(ProviderName)
                .GetStream<Message>(this.GetPrimaryKey(), Namespace)
                .SubscribeAsync(OnNextAsync);
        }

        public override async Task OnDeactivateAsync()
        {
            _logger.LogInformation($"OnDeactivateAsync [{RuntimeIdentity}],[{IdentityString}][{this.GetPrimaryKey()}] from thread {Thread.CurrentThread.Name}");
            await _subscriptionDefault.UnsubscribeAsync();
            await base.OnDeactivateAsync();
        }

        private async Task OnNextAsync(Message message, StreamSequenceToken token = null)
        {
            _logger.LogInformation($"OnNextAsync in #{message.Id} [{RuntimeIdentity}],[{IdentityString}][{this.GetPrimaryKey()}] from thread {Thread.CurrentThread.Name}");
            await Task.Delay(TimeSpan.FromMilliseconds(message.WorkTimeOutMillis));
            await GrainFactory.GetGrain<IAggregatorGrain>(Guid.Empty).MessageReceived(message.CreateDelivered($"[{RuntimeIdentity}],[{IdentityString}]").AsImmutable());
            DeactivateOnIdle();
            _logger.LogInformation($"OnNextAsync out #{message.Id} [{RuntimeIdentity}],[{IdentityString}][{this.GetPrimaryKey()}] from thread {Thread.CurrentThread.Name}");
        }
    }
}