// Copyright (c) Microsoft Corporation
// All rights reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License.  You may obtain a copy
// of the License at http://www.apache.org/licenses/LICENSE-2.0
// 
// THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED
// WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A PARTICULAR PURPOSE,
// MERCHANTABLITY OR NON-INFRINGEMENT.
// 
// See the Apache Version 2.0 License for specific language governing
// permissions and limitations under the License.

namespace Microsoft.HBase.Client.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
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
        public void TestConfigInit()
        {
            var balancer = new LoadBalancerRoundRobin();

            Assert.AreEqual(LoadBalancerRoundRobin._workerHostNamePrefix, "workernode");
            Assert.AreEqual(LoadBalancerRoundRobin._workerRestEndpointPort, 8090);
            Assert.AreEqual(LoadBalancerRoundRobin._refreshInterval.TotalMilliseconds, 10.0);
        }


        private async Task<int> EmitIntAsync(int count)
        {
            return await Task.FromResult<int>(count);
        }
        private async Task NoOpTask()
        {
            await Task.FromResult<int>(0);
        }
    
        [TestMethod]
        public void TestExecuteAndGetWithVirtualNetworkLoadBalancing()
        {
            int numServers = 4;
            var client = new HBaseClient(numServers);

            int count = 0;
            Func<string, Task<int>> f = (endpoint) =>
            {
                count++;
                
                if (count < 4)
                {
                    throw new TimeoutException();
                }
                
                return EmitIntAsync(count);
            };
            
            var output = client.ExecuteAndGetWithVirtualNetworkLoadBalancing<int>(f);

            Assert.AreEqual(count, 4);
            Assert.AreEqual(output, 4);
        }

        [TestMethod]
        public void TestExecuteAndGetWithVirtualNetworkLoadBalancingOneArg()
        {
            int numServers = 4;
            var client = new HBaseClient(numServers);

            var arg1Value = "arg1";

            int count = 0;

            Func<string, string, Task<int>> f = (arg1, endpoint) =>
            {
                Assert.AreEqual(arg1Value, arg1);
                count++;

                if (count < 4)
                {
                    throw new TimeoutException();
                }

                return EmitIntAsync(count);
            };

            var output = client.ExecuteAndGetWithVirtualNetworkLoadBalancing<string, int>(f, arg1Value);

            Assert.AreEqual(count, 4);
            Assert.AreEqual(output, 4);
        }

        [TestMethod]
        public void TestExecuteAndGetWithVirtualNetworkLoadBalancingTwoArgs()
        {
            int numServers = 4;
            var client = new HBaseClient(numServers);

            var arg1Value = "arg1";
            var arg2Value = "arg2";

            int count = 0;

            Func<string, string, string, Task<int>> f = (arg1, arg2, endpoint) =>
            {
                Assert.AreEqual(arg1Value, arg1);
                Assert.AreEqual(arg2Value, arg2);

                count++;

                if (count < 4)
                {
                    throw new TimeoutException();
                }

                return EmitIntAsync(count);
            };

            var output = client.ExecuteAndGetWithVirtualNetworkLoadBalancing<string, string, int>(f, arg1Value, arg2Value);

            Assert.AreEqual(count, 4);
            Assert.AreEqual(output, 4);
        }
        
        [TestMethod]
        public void TestExecuteWithVirtualNetworkLoadBalancing()
        {
            int numServers = 4;
            var client = new HBaseClient(numServers);

            int count = 0;
            Func<string, Task> f = (endpoint) =>
            {
                count++;

                if (count < 4)
                {
                    throw new TimeoutException();
                }

                return NoOpTask();
            };

            client.ExecuteWithVirtualNetworkLoadBalancing(f);

            Assert.AreEqual(count, 4);
        }

        [TestMethod]
        public void TestExecuteWithVirtualNetworkLoadBalancingOneArg()
        {
            int numServers = 4;
            var client = new HBaseClient(numServers);
            
            var arg1Value = "arg1";

            int count = 0;
            Func<string, string, Task> f = (arg1, endpoint) =>
            {
                Assert.AreEqual(arg1Value, arg1);

                count++;

                if (count < 4)
                {
                    throw new TimeoutException();
                }

                return NoOpTask();
            };

            client.ExecuteWithVirtualNetworkLoadBalancing(f, arg1Value);

            Assert.AreEqual(count, 4);
        }

        [TestMethod]
        public void TestExecuteWithVirtualNetworkLoadBalancingTwoArgs()
        {
            int numServers = 4;
            var client = new HBaseClient(numServers);

            var arg1Value = "arg1";
            var arg2Value = "arg2";

            int count = 0;
            Func<string, string, string, Task> f = (arg1, arg2, endpoint) =>
            {
                Assert.AreEqual(arg1Value, arg1);
                Assert.AreEqual(arg2Value, arg2);

                count++;

                if (count < 4)
                {
                    throw new TimeoutException();
                }

                return NoOpTask();
            };

            client.ExecuteWithVirtualNetworkLoadBalancing(f, arg1Value, arg2Value);

            Assert.AreEqual(count, 4);
        }

        [TestMethod]
        public void TestFailedEndpointsExpiry()
        {
            int numServers = 5;
            int numFailedEndpointsToRefresh = 3;

            Uri activeEndpoint;
            int expectedNumFailedEndpoints;
            int expectedNumAvailableEndpoints;

            var balancer = new LoadBalancerRoundRobin(numRegionServers: numServers);

            Assert.AreEqual(LoadBalancerRoundRobin._refreshInterval.TotalMilliseconds, 10.0);

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
