using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Toxiproxy.Net;
using Toxiproxy.Net.Toxics;

namespace RabbitMqStreamTests
{
    internal static class ToxiProxyHelpers
    {
        private const string RmqProxyName = "RMQ";
        private const int RmqPort = 5672;
        private const int RmqProxyPort = 56720;
        public static int ClientPort => CanRunProxy ? RmqProxyPort : RmqPort;

        public static bool CanRunProxy => RuntimeInformation.IsOSPlatform(OSPlatform.Windows) && RuntimeInformation.OSArchitecture == Architecture.X64;

        public static Process StartProxy()
        {
            if (!CanRunProxy) return null;

            StopProxyIfRunning();

            var proxyProcess = new Process
            {
                StartInfo = new ProcessStartInfo("bin-toxiproxy/toxiproxy-server-2.1.2-windows-amd64.exe")
            };
            proxyProcess.Start();

            new Connection().Client().AddAsync(new Proxy
            {
                Name = RmqProxyName,
                Enabled = true,
                Listen = $"localhost:{ClientPort}",
                Upstream = $"localhost:{RmqPort}"
            }).GetAwaiter().GetResult();

            return proxyProcess;
        }

        public static void StopProxyIfRunning()
        {
            foreach (var process in Process.GetProcessesByName("toxiproxy-server-2.1.2-windows-amd64"))
            {
                process.Terminate();
            }
        }

        public static void AddLimitDataToRmqProxy(Connection connection, ToxicDirection direction, double toxicity, int timeout)
        {
            var proxy = connection.Client().FindProxyAsync(RmqProxyName).GetAwaiter().GetResult();
            proxy.AddAsync(new LimitDataToxic
            {
                Name = "Timeout",
                Toxicity = toxicity,
                Stream = direction,
                Attributes = new LimitDataToxic.ToxicAttributes
                {
                    Bytes = timeout
                }
            }).GetAwaiter().GetResult();
            proxy.UpdateAsync().GetAwaiter().GetResult();
        }

        public static void AddLatencyToRmqProxy(Connection connection, ToxicDirection direction, double toxicity, int latency, int jitter)
        {
            var proxy = connection.Client().FindProxyAsync(RmqProxyName).GetAwaiter().GetResult();
            proxy.AddAsync(new LatencyToxic
            {
                Name = "Latency",
                Toxicity = toxicity,
                Stream = direction,
                Attributes = new LatencyToxic.ToxicAttributes
                {
                    Latency = latency,
                    Jitter = jitter
                }
            }).GetAwaiter().GetResult();
            proxy.UpdateAsync().GetAwaiter().GetResult();
        }
    }
}