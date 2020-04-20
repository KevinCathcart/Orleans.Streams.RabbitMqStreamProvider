using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
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

    internal class RabbitMqConnector : IRabbitMqConnector
    {
        public ILogger Logger { get; }
        private readonly RabbitMqConnectionProvider _connectionProvider;

        private bool _disposed;
        private IModel _channel;

        public event EventHandler<ModelCreatedEventArgs> ModelCreated;

        public IModel Channel
        {
            get
            {
                EnsureChannelAvailable();
                return _channel;
            }
        }

        public TaskScheduler Scheduler { get; }

        public RabbitMqConnector(RabbitMqConnectionProvider connectionProvider, ILogger logger)
        {
            _connectionProvider = connectionProvider;
            Logger = logger;
            Scheduler = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        }

        public void EnsureChannelAvailable()
        {
            if (_disposed)
            {
                _channel = null;
                return;
            }
            if (_channel?.IsOpen != true)
            {
                Logger.LogDebug("Creating a model.");

                _channel = _connectionProvider.Connection.CreateModel();
                ModelCreated?.Invoke(this, new ModelCreatedEventArgs(_channel));

                _channel.BasicAcks += (channel, args) => BasicAcks?.Invoke(channel, args);
                _channel.BasicNacks += (channel, args) => BasicNacks?.Invoke(channel, args);

                _channel.ConfirmSelect();   // manual (N)ACK
                Logger.LogDebug("Model created.");
            }
        }

        public event EventHandler<BasicAckEventArgs> BasicAcks;
        public event EventHandler<BasicNackEventArgs> BasicNacks;

        public void Dispose()
        {
            try
            {
                if (_channel?.IsClosed == false)
                {
                    _channel.Close();
                    _disposed = true;
                }
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Error during RMQ connection disposal.");
            }
        }
    }

    public static class RabbitMqConnectorExtensions
    {
        public static Task RunOnScheduler(this IRabbitMqConnector connector, Action action)
        {
            return Task.Factory.StartNew(action, CancellationToken.None, TaskCreationOptions.DenyChildAttach, connector.Scheduler);
        }

        public static Task RunOnScheduler(this IRabbitMqConnector connector, Action<object> action, object state)
        {
            return Task.Factory.StartNew(action, state, CancellationToken.None, TaskCreationOptions.DenyChildAttach, connector.Scheduler);
        }

        public static Task<TResult> RunOnScheduler<TResult>(this IRabbitMqConnector connector, Func<TResult> function)
        {
            return Task.Factory.StartNew(function, CancellationToken.None, TaskCreationOptions.DenyChildAttach, connector.Scheduler);
        }

        public static Task<TResult> RunOnScheduler<TResult>(this IRabbitMqConnector connector, Func<object, TResult> function, object state)
        {
            return Task.Factory.StartNew(function, state, CancellationToken.None, TaskCreationOptions.DenyChildAttach, connector.Scheduler);
        }

        public static Task RunOnScheduler(this IRabbitMqConnector connector, Func<Task> function)
        {
            return connector.RunOnScheduler<Task>(function).Unwrap();
        }

        public static Task RunOnScheduler(this IRabbitMqConnector connector, Func<object,Task> function, object state)
        {
            return connector.RunOnScheduler<Task>(function, state).Unwrap();
        }

        public static Task<TResult> RunOnScheduler<TResult>(this IRabbitMqConnector connector, Func<Task<TResult>> function)
        {
            return connector.RunOnScheduler<Task<TResult>>(function).Unwrap();
        }

        public static Task<TResult> RunOnScheduler<TResult>(this IRabbitMqConnector connector, Func<object, Task<TResult>> function, object state)
        {
            return connector.RunOnScheduler<Task<TResult>>(function, state).Unwrap();
        }

    }
}