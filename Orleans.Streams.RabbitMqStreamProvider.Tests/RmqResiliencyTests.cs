using System;
using System.Diagnostics;
using System.Threading.Tasks;
using NUnit.Framework;
using Orleans.TestingHost;
using Toxiproxy.Net.Toxics;
using static RabbitMqStreamTests.ToxiProxyHelpers;

// Note: receiving seems to be more sensitive to network errors than sending, thus reducing latency in some of the test cases
namespace RabbitMqStreamTests
{
    [TestFixture]
    public class RmqResiliencyTests
    {
        #region LimitData

        const int LargeDataLimit = 30000; //used for connections that include message data
        const int SmallDataLimit = 3000; // used for connections that are mostly acknowledgements, etc.

        [Test]
        public async Task TestRmqLimitDataUpstreamWhileSending()
        {
            // tests send call
            await _cluster.TestRmqStreamProviderWithPrefilledQueue(
                conn => AddLimitDataToRmqProxy(conn, ToxicDirection.UpStream, 1.0, LargeDataLimit), //large data limit
                conn => { },
                1000, 10);
        }

        [Test]
        public async Task TestRmqLimitDataDownstreamWhileSending()
        {
            // tests (n)ack from the rmq to the client
            await _cluster.TestRmqStreamProviderWithPrefilledQueue(
                conn => AddLimitDataToRmqProxy(conn, ToxicDirection.DownStream, 1.0, SmallDataLimit),
                conn => { },
                1000, 10);
        }

        [Test]
        public async Task TestRmqLimitDataUpstreamWhileReceiving()
        {
            // tests (n)ack from the client to the rmq
            await _cluster.TestRmqStreamProviderWithPrefilledQueue(
                conn => { },
                conn => AddLimitDataToRmqProxy(conn, ToxicDirection.UpStream, 1.0, SmallDataLimit),
                1000, 10);
        }

        [Test]
        public async Task TestRmqLimitDataDownstreamWhileReceiving()
        {
            // tests receive call
            await _cluster.TestRmqStreamProviderWithPrefilledQueue(
                conn => { },
                conn => AddLimitDataToRmqProxy(conn, ToxicDirection.DownStream, 1.0, LargeDataLimit),
                1000, 10);
        }

        [Test]
        public async Task TestRmqLimitDataUpstreamOnFly()
        {
            // tests (n)ack from the client to the rmq
            await _cluster.TestRmqStreamProviderOnFly(
                conn => AddLimitDataToRmqProxy(conn, ToxicDirection.UpStream, 1.0, LargeDataLimit),
                1000, 60);
        }

        [Test]
        public async Task TestRmqLimitDataDownstreamOnFly()
        {
            // tests receive call
            await _cluster.TestRmqStreamProviderOnFly(
                conn => AddLimitDataToRmqProxy(conn, ToxicDirection.DownStream, 1.0, LargeDataLimit),
                1000, 60);
        }

        #endregion

        #region Latency

        [Test]
        public async Task TestRmqLatencyUpstreamWhileSending()
        {
            // tests send call
            await _cluster.TestRmqStreamProviderWithPrefilledQueue(
                conn => AddLatencyToRmqProxy(conn, ToxicDirection.UpStream, 1.0, 5000, 5000),
                conn => { },
                100, 60);
        }

        [Test]
        public async Task TestRmqLatencyDownstreamWhileSending()
        {
            // tests (n)ack from the rmq to the client
            await _cluster.TestRmqStreamProviderWithPrefilledQueue(
                conn => AddLatencyToRmqProxy(conn, ToxicDirection.DownStream, 1.0, 5000, 5000),
                conn => { },
                100, 60);
        }

        [Test]
        public async Task TestRmqLatencyUpstreamWhileReceiving()
        {
            // tests (n)ack from the client to the rmq
            await _cluster.TestRmqStreamProviderWithPrefilledQueue(
                conn => { },
                conn => AddLatencyToRmqProxy(conn, ToxicDirection.UpStream, 1.0, 3000, 3000),
                100, 60);
        }

        [Test]
        public async Task TestRmqLatencyDownstreamWhileReceiving()
        {
            // tests receive call
            await _cluster.TestRmqStreamProviderWithPrefilledQueue(
                conn => { },
                conn => AddLatencyToRmqProxy(conn, ToxicDirection.DownStream, 1.0, 3000, 3000),
                100, 60);
        }

        [Test]
        public async Task TestRmqLatencyUpstreamOnFly()
        {
            // tests (n)ack from the client to the rmq
            await _cluster.TestRmqStreamProviderOnFly(
                conn => AddLatencyToRmqProxy(conn, ToxicDirection.UpStream, 1.0, 3000, 3000),
                100, 60);
        }

        [Test]
        public async Task TestRmqLatencyDownstreamOnFly()
        {
            // tests receive call
            await _cluster.TestRmqStreamProviderOnFly(
                conn => AddLatencyToRmqProxy(conn, ToxicDirection.DownStream, 1.0, 3000, 3000),
                100, 60);
        }

        #endregion

        #region Test class setup

        private static TestCluster _cluster;
        private static Process _proxyProcess;

        [SetUp]
        public void TestInitialize()
        {
            RmqHelpers.EnsureEmptyQueue();
        }

        [OneTimeSetUp]
        public static async Task ClassInitialize()
        {
            if(!CanRunProxy)
            {
                Assert.Ignore("Resiliency tests not enabled on this platform");
            }
            // ToxiProxy
            _proxyProcess = StartProxy();

            // Orleans cluster
            _cluster = new TestClusterBuilder()
                .AddSiloBuilderConfigurator<TestClusterConfigurator>()
                .AddClientBuilderConfigurator<TestClusterConfigurator>()
                .Build();

            await _cluster.DeployAsync();
        }

        [OneTimeTearDown]
        public static void ClassCleanup()
        {
            // close first to avoid a case where Silo hangs, I stop the test and the proxy process keeps running
            _proxyProcess?.Terminate();

            _cluster.Dispose();
        }

        #endregion
    }
}