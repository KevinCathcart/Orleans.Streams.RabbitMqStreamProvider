using System;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Orleans.Streams.RabbitMq
{
    // Effectively a singleton per provider. Needs to be thread safe!
    internal class RabbitMqConnectionProvider
    {
        public readonly ILogger Logger;

        private readonly RabbitMqOptions _options;
        private IConnection _connection;

        public RabbitMqConnectionProvider(RabbitMqOptions options, ILogger logger)
        {
            _options = options;
            Logger = logger;
        }

        public IConnection Connection
        {
            get
            {
                EnsureConnection();
                return _connection;
            }
        }

        private void EnsureConnection()
        {
            if (_connection?.IsOpen != true)
            {
                lock (this)
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
                            UseBackgroundThreadsForIO = true,
                            AutomaticRecoveryEnabled = false,
                            NetworkRecoveryInterval = TimeSpan.FromSeconds(10)
                        };

                        _connection = factory.CreateConnection();
                        Logger.LogDebug("Connection created.");

                        _connection.ConnectionShutdown += OnConnectionShutdown;
                        _connection.ConnectionBlocked += OnConnectionBlocked;
                        _connection.ConnectionUnblocked += OnConnectionUnblocked;
                    }
                }
            }
        }

        private void OnConnectionShutdown(object connection, ShutdownEventArgs reason)
        {
            Logger.LogWarning("Connection was shut down: [{reason}]", reason.ReplyText);
        }

        private void OnConnectionBlocked(object connection, ConnectionBlockedEventArgs reason)
        {
            Logger.LogWarning("Connection is blocked: [{reason}]", reason.Reason);
        }

        private void OnConnectionUnblocked(object connection, EventArgs args)
        {
            Logger.LogWarning("Connection is not blocked any more.");
        }
    }
}