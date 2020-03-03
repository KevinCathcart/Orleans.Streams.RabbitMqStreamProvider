using System;
using System.Collections.Generic;
using RabbitMQ.Client;

namespace Orleans.Streams.RabbitMq
{
    internal class DeclarationHelper
    {
        private readonly ITopologyProvider _topologyProvider;
        private readonly HashSet<string> _declaredQueues = new HashSet<string>();
        private readonly HashSet<string> _declaredExchanges = new HashSet<string>();

        public DeclarationHelper(ITopologyProvider topologyProvider)
        {
            _topologyProvider = topologyProvider;
        }

        public void DeclareExchange(string name, IModel channel)
        {
            if (_declaredExchanges.Contains(name)) return;

            _declaredExchanges.Add(name);
            var properties = _topologyProvider.GetExchangeProperties(name);

            if (!properties.ShouldDeclare) return;

            if(properties.PassiveDeclare)
            {
                channel.ExchangeDeclarePassive(name);
            }
            else
            {
                channel.ExchangeDeclare(name, properties.Type.Value, properties.Durable, properties.AutoDelete, properties.Arguments);
            }

            ProcessBindings(properties.Bindings, channel);
        }

        public void DeclareQueue(string name, IModel channel)
        {
            if (_declaredQueues.Contains(name)) return;

            _declaredQueues.Add(name);
            var properties = _topologyProvider.GetQueueProperties(name);

            if (!properties.ShouldDeclare) return;

            if (properties.PassiveDeclare)
            {
                channel.QueueDeclarePassive(name);
            }
            else
            {
                channel.QueueDeclare(name, properties.Durable, properties.Exclusive, properties.AutoDelete, properties.Arguments);
            }

            ProcessBindings(properties.Bindings, channel);
        }

        private void ProcessBindings(List<RabbitMqBinding> bindings, IModel channel)
        {
            if (bindings == null) return;
            foreach(var binding in bindings)
            {
                DeclareExchange(binding.Source, channel);

                if (binding.Type == RabbitMqBindingType.Exchange)
                {
                    DeclareExchange(binding.Destination, channel);
                    channel.ExchangeBind(binding.Destination, binding.Source, binding.RoutingKey, binding.Arguments);
                }
                else if (binding.Type == RabbitMqBindingType.Queue)
                {
                    DeclareQueue(binding.Destination, channel);
                    channel.QueueBind(binding.Destination, binding.Source, binding.RoutingKey ?? string.Empty, binding.Arguments);
                }
            }
        }

        internal void Clear()
        {
            _declaredExchanges.Clear();
            _declaredQueues.Clear();
        }
    }
}