namespace Microsoft.HBase.Client.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using Microsoft.HBase.Client.LoadBalancing;
    using Microsoft.HBase.Client.Tests.Utilities;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using org.apache.hadoop.hbase.rest.protobuf.generated;

    [TestClass]
    public class VirtualNetworkLoadBalancerTests : DisposableContextSpecification 
    {
        protected override void Context()
        {
        }

        [TestCleanup]
        public override void TestCleanup()
        {
        }

        [TestMethod]
        public void TestRoundRobinLoadBalancer()
        {
            int numServers = 4;

            var balancer = new LoadBalancerRoundRobin(numRegionServers: numServers);

            var expectedServersList = BuildServersList(numServers);

            Assert.AreEqual(balancer.GetWorkersCount(), numServers);
            
            Assert.IsTrue(CompareLists(balancer._allEndpoints, expectedServersList));
            Assert.IsTrue(CompareLists(balancer._availableEndpoints, expectedServersList));
            Assert.IsNull(balancer._activeEndpoint);
            Assert.IsTrue(CompareLists(balancer.GetEndpoints(balancer._failedEndpoints), new List<string>()));

            int expectedNumFailedEndpoints = 0;
            int expectedNumAvailableEndpoints = numServers;
            for (int i = 0; i < 2 * numServers; i++)
            {
                var endpoint = balancer.GetWorkerNodeEndPointBaseNext();
                Assert.IsNotNull(endpoint);
                var endpointFoundInExpectedList = expectedServersList.FirstOrDefault(e => e.Equals(endpoint.OriginalString, StringComparison.OrdinalIgnoreCase));
                Assert.IsTrue(endpointFoundInExpectedList != default(string));
                
                if ((expectedNumAvailableEndpoints > 0) && (i > 0)) 
                {
                    expectedNumFailedEndpoints++;
                }
                else
                {
                    expectedNumFailedEndpoints = 0;
                }
                
                expectedNumAvailableEndpoints = (expectedNumAvailableEndpoints == 0) ? (numServers - 1) : expectedNumAvailableEndpoints - 1;

                Assert.AreEqual(balancer._failedEndpoints.Count, expectedNumFailedEndpoints);
                Assert.AreEqual(balancer._availableEndpoints.Count, expectedNumAvailableEndpoints);
            }
        }

        [TestMethod]
        public void TestFailedEndpointsExpiry()
        {
            int numServers = 5;
            int numFailedEndpointsToRefresh = 3;

            Uri activeEndpoint;
            int expectedNumFailedEndpoints;
            int expectedNumAvailableEndpoints;

            var balancer = new LoadBalancerRoundRobin(numRegionServers: numServers, refreshIntervalInMilliseconds: 10);

            for (int i = 0; i < numFailedEndpointsToRefresh; i++)
            {
                activeEndpoint = balancer.GetWorkerNodeEndPointBaseNext();
                Assert.IsNotNull(activeEndpoint);
                Assert.IsNotNull(balancer._activeEndpoint);

                expectedNumFailedEndpoints = i;
                expectedNumAvailableEndpoints = (numServers - 1) - expectedNumFailedEndpoints;

                Assert.AreEqual(expectedNumFailedEndpoints, balancer._failedEndpoints.Count);
                Assert.AreEqual(expectedNumAvailableEndpoints, balancer._availableEndpoints.Count);
            }

            Thread.Sleep(100);
            
            activeEndpoint = balancer.GetWorkerNodeEndPointBaseNext();
            Assert.IsNotNull(activeEndpoint);
            Assert.IsNotNull(balancer._activeEndpoint);

            expectedNumFailedEndpoints = 1;
            expectedNumAvailableEndpoints = (numServers - 1) - expectedNumFailedEndpoints;
            Assert.AreEqual(expectedNumFailedEndpoints, balancer._failedEndpoints.Count);
            Assert.AreEqual(expectedNumAvailableEndpoints, balancer._availableEndpoints.Count);
        }

        private List<string> BuildServersList(int n)
        {
            var list = new List<string>();
            for (int i = 0; i < n; i++)
            {
                list.Add(string.Format("http://{0}{1}:{2}", "workernode", i, 8090));
            }
            return list;
        }
        
        private bool CompareLists(List<string> a, List<string> b)
        {
            if((a == null) && (b != null))
            {
                return false;
            }

            if ((a != null) && (b == null))
            {
                return false;
            }

            if (a.Count != b.Count)
            {
                return false;
            }

            foreach (var aElem in a)
            {
                if (b.FirstOrDefault(bElem => bElem.Equals(aElem, StringComparison.OrdinalIgnoreCase)) == default (string))
                {
                    return false;
                }
            }

            return true;
        }
    }
}
