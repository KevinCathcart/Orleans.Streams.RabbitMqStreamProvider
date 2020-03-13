using System;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.Streams.RabbitMq;
using RabbitMQ.Client;

namespace Orleans.Configuration
{
    public class RabbitMqOptions
    {
        public ConnectionFactory Connection { get; set; } = new ConnectionFactory();
        public string QueueNamePrefix { get; set; }
        public bool UseQueuePartitioning { get; set; } = DefaultUseQueuePartitioning;
        public int NumberOfQueues { get; set; } = DefaultNumberOfQueues;
        public StreamProviderDirection Direction { get; set; } = StreamProviderDirection.ReadWrite;

        public const bool DefaultUseQueuePartitioning = true;
        public const int DefaultNumberOfQueues = 8;
    }

    public class RabbitMqOptionsValidator : IConfigurationValidator
    {
        private readonly RabbitMqOptions options;
        private readonly string name;

        public RabbitMqOptionsValidator(RabbitMqOptions options, string name)
        {
            this.options = options;
            this.name = name;
        }

        public void ValidateConfiguration()
        {
            if (string.IsNullOrEmpty(options.QueueNamePrefix)) ThrowMissing(nameof(options.QueueNamePrefix));
            if (options.UseQueuePartitioning && options.NumberOfQueues <= 0) ThrowNotPositive(nameof(options.NumberOfQueues));
        }

        private void ThrowMissing(string parameterName)
            => throw new OrleansConfigurationException($"Missing required parameter `{parameterName}` on stream provider {name}!");

        private void ThrowNotPositive(string parameterName)
            => throw new OrleansConfigurationException($"Value of parameter `{parameterName}` must be positive!");

        public static IConfigurationValidator Create(IServiceProvider services, string name)
        {
            RabbitMqOptions options = services.GetOptionsByName<RabbitMqOptions>(name);
            return new RabbitMqOptionsValidator(options, name);
        }
    }
}