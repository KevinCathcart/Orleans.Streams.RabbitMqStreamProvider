# Orleans.Streams.RabbitMqStreamProvider
Orleans persistent stream provider for RabbitMQ. This provider is reliable by
default, thus ACKing messages only after they are processed by a consumer.

## Replaceable Components

By replacing the Data Adapter, this provider supports mapping an Orleans
message to an arbitrary RabbitMQ message, and mapping a RabbitMq message to
arbitrary batches of Orleans messages. (One RabbitMQ message can be converted
to messages on multiple different virtual streams.)

This is especially useful for consuming messages that came from a non-Orleans
source, or sending messages that will be consumed by a non-Orleans application.
In that case, you will probably want to set the `UseQueuePartitioning` option
to false, so that only one queue is used (or use a custom `QueueStreamMapper`),
and should consider setting the `Direction` Setting to `ReadOnly` or 
`WriteOnly` as applicable.

You can replace just the serializer used by deriving from the
`RabbitMqDataAdapterBase` class.

You can customize the names of queues (or their properties), set up bindings,
and customize exchange properties by writing a replacement `ITopologyProvider`
implementation. Note that to send to an exchange you would also need a
customized Data Adapter.

You can replace the `StreamQueueMapper` implementation if you want to change
the mapping between streams and queues. You might, for example, want certain
high-priority low-volume streams to have a separate queue to ensure they do
not get held up if the main queues are a little behind.

For all of these replaceable components, the configurator extension method
takes either a generic parameter, or a factory delegate. The latter is needed
if your replacement needs a named service injected (like another part of the
provider). The factory methods are usually set up as static method named
`Create` on the replacement type.

## Services available to your application.

If you need direct access to RabbitMQ (perhaps to handle dynamic topology
changes), you can retrieve `IRabbitMqConnectorFactory` as a named service. You
can then use its `CreateGenericConnector` method to get an `IRabbitMqConnector`,
which wraps RabbitMQ's channel (`IModel`). Each time you access the `Channel`
property of the connector, it will make sure the channel is still open, and
will reopen it (or even the whole connection) if needed.

Please note that the Channel is not thread safe, and is synchronous, and
thus will block the task scheduler they are running on. Furthermore events
and callbacks do not run on the original task scheduler, so touching the
channel from them is risky at best. To avoid these problems the connector
has a `TaskScheduler` property that exposes an exclusive task scheduler, on
which you can run all channel operations.

## Configuration

Example configuration:
```
var silo = new SiloHostBuilder()
    .UseLocalhostClustering()
    .AddMemoryGrainStorage("PubSubStore")
    .AddRabbitMqStream("RMQProvider", configurator =>
    {
        configurator.ConfigureRabbitMq(ob =>
        {
            ob.Configure(options =>
            {
                // Connection is the actual RabbitMq ConnectionFactory, and can be configured however is needed.
                options.Connection.HostName = "localhost";
                options.Connection.UserName = "guest";
                options.Connection.Password = "guest";

                options.ConnectionName = "Example Orleans Silo";
                options.QueueNamePrefix = "test";
                options.UseQueuePartitioning = true;
                options.NumberOfQueues = 4;
            });
        });
                    
        // All calls below here are optional
        configurator.ConfigureCacheSize(1000);

        // Replaceable Components:
                    
        configurator.ConfigureQueueDataAdapter<CustomDataAdapter>();
        // or configurator.ConfigureQueueDataAdapter(CustomDataAdapter.Create);

        configurator.ConfigureTopologyProvider<CustomTopologyProvider>();
        // or configurator.ConfigureTopologyProvider(CustomTopologyProvider.Create);

        configurator.ConfigureStreamQueueMapper<CustomStreamQueueMapper>();
        // or configurator.ConfigureStreamQueueMapper(CustomStreamQueueMapper.Create);
    })
    .ConfigureLogging(log => log.AddConsole())
    .Build();

await silo.StartAsync();
```
