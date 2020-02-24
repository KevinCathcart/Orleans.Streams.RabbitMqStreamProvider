using System;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

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

        private readonly RabbitMqOptions _options;
        private IConnection _connection;
        private IModel _channel;

        public event EventHandler<ModelCreatedEventArgs> ModelCreated;

        public IModel Channel
        {
            get
            {
                EnsureConnection();
                return _channel;
            }
        }

        public RabbitMqConnector(RabbitMqOptions options, ILogger logger)
        {
            _options = options;
            Logger = logger;
        }

        private void EnsureConnection()
        {
            if (_connection?.IsOpen != true)
            {
                Logger.LogDebug("Opening a new RMQ connection...");
                var factory = new ConnectionFactory
                {
                    HostName = _options.HostName,
                    VirtualHost = _options.VirtualHost,
                    Port = _options.Port,
                    UserName = _options.UserName,
                    Password = _options.Password,
                    UseBackgroundThreadsForIO = false,
                    AutomaticRecoveryEnabled = false,
                    NetworkRecoveryInterval = TimeSpan.FromSeconds(10)
                };

                _connection = factory.CreateConnection();
                Logger.LogDebug("Connection created.");
                _connection.ConnectionShutdown += OnConnectionShutdown;
                _connection.ConnectionBlocked += OnConnectionBlocked;
                _connection.ConnectionUnblocked += OnConnectionUnblocked;
            }

            if (_channel?.IsOpen != true)
            {
                Logger.LogDebug("Creating a model.");
                _channel = _connection.CreateModel();
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
                _connection?.Close();
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Error during RMQ connection disposal.");
            }
        }

        private void OnConnectionShutdown(object connection, ShutdownEventArgs reason)
        {
            Logger.LogWarning($"Connection was shut down: [{reason.ReplyText}]");
        }

        private void OnConnectionBlocked(object connection, ConnectionBlockedEventArgs reason)
        {
            Logger.LogWarning($"Connection is blocked: [{reason.Reason}]");
        }

        private void OnConnectionUnblocked(object connection, EventArgs args)
        {
            Logger.LogWarning("Connection is not blocked any more.");
        }
    }
}