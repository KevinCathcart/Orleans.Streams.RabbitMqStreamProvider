﻿using System;
using System.Collections.Generic;
using RabbitMQ.Client;

namespace RabbitMqStreamTests
{
    public enum RmqSerializer
    {
        Default,
        ProtoBuf
    }

    public static class RmqHelpers
    {
        public static void EnsureEmptyQueue()
        {
            var factory = new ConnectionFactory
            {
                HostName = "localhost",
                VirtualHost = "/",
                Port = ToxiProxyHelpers.ClientPort,
                UserName = "guest",
                Password = "guest"
            };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                foreach (var queue in new[] { Globals.StreamNameSpaceDefault, Globals.StreamNameSpaceProtoBuf })
                {
                    try
                    {
                        channel.QueuePurge(queue);
                    }
                    catch (Exception)
                    {
                        // queue probably just not created yet, which is not an error.
                    }
                }
            }
        }

        public static void DeleteQueues(IEnumerable<string> queues)
        {
            var factory = new ConnectionFactory
            {
                HostName = "localhost",
                VirtualHost = "/",
                Port = ToxiProxyHelpers.ClientPort,
                UserName = "guest",
                Password = "guest"
            };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                foreach(var queue in queues)
                {
                    channel.QueueDelete(queue);
                }
            }
        }
    }
}
