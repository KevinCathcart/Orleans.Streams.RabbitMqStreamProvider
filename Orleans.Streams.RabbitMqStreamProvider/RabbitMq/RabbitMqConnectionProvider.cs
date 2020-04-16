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

            _options.Connection.AutomaticRecoveryEnabled = false;

            // Force UseBackgroundThreadsForIO to be true if it exists. Using reflection, because I expect this property will get removed in a future version of the
            // rabbitmq Client, since it already does nothing in the v6.0 prerelease, and I don't want this provider to break just because of that.
            _options.Connection.GetType().GetProperty("UseBackgroundThreadsForIO")?.SetValue(_options.Connection, true);
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

                        _connection = _options.Connection.CreateConnection(_options.ConnectionName);
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