using System;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Orleans.Streams.RabbitMq
{

    public class ModelCreatedEventArgs
    {
        public IModel Channel { get; }

        public ModelCreatedEventArgs(IModel channel)
        {
            Channel = channel;
        }
    }

    internal class RabbitMqConnector : IDisposable
    {
        public readonly ILogger Logger;
        private readonly RabbitMqConnectionProvider _connectionProvider;

        private IModel _channel;

        public event EventHandler<ModelCreatedEventArgs> ModelCreated;

        public IModel Channel
        {
            get
            {
                EnsureChannel();
                return _channel;
            }
        }

        public RabbitMqConnector(RabbitMqConnectionProvider connectionProvider, ILogger logger)
        {
            _connectionProvider = connectionProvider;
            Logger = logger;
        }

        private void EnsureChannel()
        {
            if (_channel?.IsOpen != true)
            {
                Logger.LogDebug("Creating a model.");

                _channel = _connectionProvider.Connection.CreateModel();
                ModelCreated?.Invoke(this, new ModelCreatedEventArgs(_channel));

                _channel.ConfirmSelect();   // manual (N)ACK
                Logger.LogDebug("Model created.");
            }
        }

        public void Dispose()
        {
            try
            {
                if (_channel?.IsClosed == false)
                {
                    _channel.Close();
                }
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Error during RMQ connection disposal.");
            }
        }
    }
}