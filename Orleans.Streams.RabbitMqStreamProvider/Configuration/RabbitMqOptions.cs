using System;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Configuration
{
    public class RabbitMqOptions
    {
        public string HostName { get; set; }
        public int Port { get; set; }
        public string VirtualHost { get; set; }
        public string UserName { get; set; }
        public string Password { get; set; }

        public string QueueNamePrefix { get; set; }
        public bool UseQueuePartitioning { get; set; } = DefaultUseQueuePartitioning;
        public int NumberOfQueues { get; set; } = DefaultNumberOfQueues;
        public StreamProviderDirection Direction { get; set; } = StreamProviderDirection.ReadWrite;

        public const bool DefaultUseQueuePartitioning = false;
        public const int DefaultNumberOfQueues = 1;
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
            if (string.IsNullOrEmpty(options.HostName)) ThrowMissing(nameof(options.HostName));
            if (string.IsNullOrEmpty(options.VirtualHost)) ThrowMissing(nameof(options.VirtualHost));
            if (string.IsNullOrEmpty(options.UserName)) ThrowMissing(nameof(options.UserName));
            if (string.IsNullOrEmpty(options.Password)) ThrowMissing(nameof(options.Password));
            if (string.IsNullOrEmpty(options.QueueNamePrefix)) ThrowMissing(nameof(options.QueueNamePrefix));
            if (options.Port <= 0) ThrowNotPositive(nameof(options.Port));
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