using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Toxiproxy.Net;
using Toxiproxy.Net.Toxics;

namespace RabbitMqStreamTests
{
    internal static class ToxiProxyHelpers
    {
        public const string RmqPortEnvVar = "RMQ_PORT";
        public const string ProxyPortEnvVar = "RMQ_TOXI_PORT";
        private const string RmqProxyName = "RMQ";

        public static readonly int RmqPort = int.TryParse(Environment.GetEnvironmentVariable(RmqPortEnvVar), out var port) ? port : 5672;
        public static readonly int RmqProxyPort = int.TryParse(Environment.GetEnvironmentVariable(ProxyPortEnvVar), out var port) ? port : 5670;
        public static int ClientPort => CanRunProxy ? RmqProxyPort : RmqPort;

        public static bool UseDockerProxy => !string.IsNullOrEmpty(Environment.GetEnvironmentVariable(ProxyPortEnvVar));

        public static bool CanRunProxy => CanStartProxy;

        public static bool CanStartProxy => RuntimeInformation.IsOSPlatform(OSPlatform.Windows) && RuntimeInformation.OSArchitecture == Architecture.X64;

        public static Process StartProxy()
        {
            if (UseDockerProxy || !CanStartProxy) return null;

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