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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Text.RegularExpressions;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.HBase.Client.Internal;
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
        public void TestLoadBalancerEndpointsInitialization()
        {
            int numServers = 4;

            var balancer = new LoadBalancerRoundRobin(numRegionServers: numServers);
            Assert.AreEqual(balancer.GetNumAvailableEndpoints(), numServers);
            
            var expectedServersList = BuildServersList(numServers);

            var actualServersList = new List<string>();

            foreach(var endpoint in balancer._allEndpoints)
            {
                actualServersList.Add(endpoint.OriginalString);
            }
            
            Assert.IsTrue(CompareLists(actualServersList, expectedServersList));
        }

        [TestMethod]
        public void TestConfigInit()
        {
            var balancer = new LoadBalancerRoundRobin();

            Assert.AreEqual(LoadBalancerRoundRobin._workerHostNamePrefix, "workernode");
            Assert.AreEqual(LoadBalancerRoundRobin._workerRestEndpointPort, 8090);
            Assert.AreEqual(LoadBalancerRoundRobin._refreshInterval.TotalMilliseconds, 10.0);
        }
        
        [TestMethod]
        public void TestLoadBalancerRetriesExhausted()
        {
            int numServers = 4;
            int numFailures = 5;
            var client = new HBaseClient(numServers);

            int count = 0;
            Func<string, Task<int>> f = (endpoint) =>
            {
                count++;

                if (count < numFailures)
                {
                    throw new TimeoutException(count.ToString());
                }

                return EmitIntAsync(count);
            };

            try
            {
                var output = client.ExecuteAndGetWithVirtualNetworkLoadBalancing(f);
            }
            catch (TimeoutException ex)
            {
                Assert.AreEqual(ex.Message, numServers.ToString());

                return;
            }

            Assert.Fail();
        }

        protected class IgnoreBlackListedEndpointsPolicy : IEndpointIgnorePolicy
        {

            private List<string> _blackListedEndpoints;

            public IgnoreBlackListedEndpointsPolicy(List<string> blacklistedEndpoints)
            {
                _blackListedEndpoints = blacklistedEndpoints;
            }

            public IEndpointIgnorePolicy InnerPolicy { get; private set; }
            public void OnEndpointAccessStart(Uri endpointUri)
            {
            }

            public void OnEndpointAccessCompletion(Uri endpointUri, EndpointAccessResult accessResult)
            {
            }

            public bool ShouldIgnoreEndpoint(Uri endpoint)
            {
                if (_blackListedEndpoints == null)
                {
                    return false;
                }

                var blackListedEndpointFound = _blackListedEndpoints.Find(x => x.Equals(endpoint.OriginalString));

                return (blackListedEndpointFound != null);
            }

            public void RefreshIgnoredList()
            {
            }
        }

        [TestMethod]
        public void TestLoadBalancerRoundRobin()
        {
            int numServers = 10;
            
            var balancer = new LoadBalancerRoundRobin(numServers);
            int initRRIdx = balancer._endpointIndex;

            for (int i = 0; i < 2 * numServers; i++)
            {
                Uri selectedEndpoint = null;

                selectedEndpoint = balancer.GetEndpoint();

                var expectedRRIdx = (initRRIdx + i) % numServers;
                var expectedEndpoint = balancer._allEndpoints[expectedRRIdx];
                Assert.IsTrue(selectedEndpoint.OriginalString.Equals(expectedEndpoint.OriginalString));

                balancer.RecordSuccess(selectedEndpoint);
            }
        }

        [TestMethod]
        public void TestLoadBalancerConcurrency()
        {
            int numServers = 20;
            var balancer = new LoadBalancerRoundRobin(numServers);
          
            var uniqueEndpointsFetched = new ConcurrentDictionary<string, bool>();

            Parallel.For(0, numServers, (i) =>
            {
                var endpoint = balancer.GetEndpoint();
                Assert.IsNotNull(endpoint);

                balancer.RecordSuccess(endpoint);

                Assert.IsFalse(uniqueEndpointsFetched.ContainsKey(endpoint.OriginalString));
                Assert.IsTrue(uniqueEndpointsFetched.TryAdd(endpoint.OriginalString, true));
            });
        }

        [TestMethod]
        public void TestLoadBalancerConfigInitialization()
        {
            string stringConfigInitial = Guid.NewGuid().ToString();
            string stringConfigDefault = Guid.NewGuid().ToString();
            string stringConfigExpected = "LoadBalancerTestConfigValue";
            string stringConfigValidKey = "LoadBalancerTestConfigString";
            string stringConfigInvalidKey = "LoadBalancerTestConfigStringInvalid";

            string stringReadInvalid = stringConfigInitial;
            stringReadInvalid = LoadBalancerRoundRobin.ReadFromConfig<string>(stringConfigInvalidKey, string.Copy, stringConfigDefault);
            Assert.AreEqual(stringReadInvalid, stringConfigDefault);

            string stringReadValid = stringConfigInitial;
            stringReadValid = LoadBalancerRoundRobin.ReadFromConfig<string>(stringConfigValidKey, string.Copy, stringConfigDefault);
            Assert.AreEqual(stringReadValid, stringConfigExpected);

            var rnd = new Random();

            int intConfigInitial = rnd.Next();
            int intConfigDefault = rnd.Next();
            int intConfigExpected = 10;
            string intConfigValidKey = "LoadBalancerTestConfigInt";
            string intConfigInvalidKey = "LoadBalancerTestConfigIntInvalid";

            int intReadInvalid = intConfigInitial;
            intReadInvalid = LoadBalancerRoundRobin.ReadFromConfig<int>(intConfigInvalidKey, Int32.Parse, intConfigDefault);
            Assert.AreEqual(intReadInvalid, intConfigDefault);

            int intReadValid = intConfigInitial;
            intReadValid = LoadBalancerRoundRobin.ReadFromConfig<int>(intConfigValidKey, Int32.Parse, intConfigDefault);
            Assert.AreEqual(intReadValid, intConfigExpected);

            double doubleConfigInitial = rnd.NextDouble();
            double doubleConfigDefault = rnd.NextDouble();
            double doubleConfigExpected = 20.0;
            string doubleConfigValidKey = "LoadBalancerTestConfigDouble";
            string doubleConfigInvalidKey = "LoadBalancerTestConfigDoubleInvalid";

            double doubleReadInvalid = doubleConfigInitial;
            doubleReadInvalid = LoadBalancerRoundRobin.ReadFromConfig<double>(doubleConfigInvalidKey, Double.Parse, doubleConfigDefault);
            Assert.AreEqual(doubleReadInvalid, doubleConfigDefault);

            double doubleReadValid = doubleConfigInitial;
            doubleReadValid = LoadBalancerRoundRobin.ReadFromConfig<double>(doubleConfigValidKey, Double.Parse, doubleConfigDefault);
            Assert.AreEqual(doubleReadValid, doubleConfigExpected);
            
        }
    


        [TestMethod]
        public void TestLoadBalancerIgnorePolicy()
        {
            int numServers = 10;
            int numBlackListedServers = 8;
            var balancer = new LoadBalancerRoundRobin(numServers);

            var blackListedServersList = BuildServersList(numBlackListedServers);

            balancer._endpointIgnorePolicy = new IgnoreBlackListedEndpointsPolicy(blackListedServersList);
            
            for(int i = 0; i < 2*numServers; i++)
            {
                Uri selectedEndpoint = null;
                
                selectedEndpoint = balancer.GetEndpoint();
                var selectedEndpointFoundInBlackList = blackListedServersList.Find(x => x.Equals(selectedEndpoint.OriginalString));
                Assert.IsNull(selectedEndpointFoundInBlackList);
                
                balancer.RecordSuccess(selectedEndpoint);
            }
        }
    
        [TestMethod]
        public void TestLoadBalancerIgnoreBlackListedEndpoints()
        {
            int numServers = 10;
            int numBlackListedServers = 8;

            var client = new HBaseClient(numServers);

            var blackListedServers = BuildServersList(numBlackListedServers);

            int result = 0;
            int numFailures = 0;

            Func<string, Task<int>> f = (endpoint) =>
            {
                var blacklistedEndpointFound = blackListedServers.Find(x => x.Equals(endpoint));
                if (blacklistedEndpointFound != null)
                {
                    numFailures++;
                    throw new TimeoutException();
                }
                result = 100;
                
                return EmitIntAsync(result);
            };

            var resultInOutput = client.ExecuteAndGetWithVirtualNetworkLoadBalancing<int>(f);

            Assert.IsTrue(resultInOutput == 100);

            Assert.IsTrue(numFailures >= 0);
        }

        [TestMethod]
        public void TestExecuteAndGetWithVirtualNetworkLoadBalancing()
        {
            int numServers = 4;
            int numFailures = 3;
            var client = new HBaseClient(numServers);

            int count = 0;
            Func<string, Task<int>> f = (endpoint) =>
            {
                count++;

                if (count < numFailures)
                {
                    throw new TimeoutException();
                }
                
                return EmitIntAsync(count);
            };
            
            var output = client.ExecuteAndGetWithVirtualNetworkLoadBalancing<int>(f);

            Assert.AreEqual(count, numFailures);
            Assert.AreEqual(output, numFailures);
        }

        [TestMethod]
        public void TestExecuteAndGetWithVirtualNetworkLoadBalancingOneArg()
        {
            int numServers = 4;
            int numFailures = 3;
            var client = new HBaseClient(numServers);

            var arg1Value = "arg1";

            int count = 0;

            Func<string, string, Task<int>> f = (arg1, endpoint) =>
            {
                Assert.AreEqual(arg1Value, arg1);
                count++;

                if (count < numFailures)
                {
                    throw new TimeoutException();
                }

                return EmitIntAsync(count);
            };

            var output = client.ExecuteAndGetWithVirtualNetworkLoadBalancing<string, int>(f, arg1Value);

            Assert.AreEqual(count, numFailures);
            Assert.AreEqual(output, numFailures);
        }

        [TestMethod]
        public void TestExecuteAndGetWithVirtualNetworkLoadBalancingTwoArgs()
        {
            int numServers = 4;
            int numFailures = 3;
            var client = new HBaseClient(numServers);

            var arg1Value = "arg1";
            var arg2Value = "arg2";

            int count = 0;

            Func<string, string, string, Task<int>> f = (arg1, arg2, endpoint) =>
            {
                Assert.AreEqual(arg1Value, arg1);
                Assert.AreEqual(arg2Value, arg2);

                count++;

                if (count < numFailures)
                {
                    throw new TimeoutException();
                }

                return EmitIntAsync(count);
            };

            var output = client.ExecuteAndGetWithVirtualNetworkLoadBalancing<string, string, int>(f, arg1Value, arg2Value);

            Assert.AreEqual(count, numFailures);
            Assert.AreEqual(output, numFailures);
        }
        
        [TestMethod]
        public void TestExecuteWithVirtualNetworkLoadBalancing()
        {
            int numServers = 4;
            int numFailures = 3;
            var client = new HBaseClient(numServers);

            int count = 0;
            Func<string, Task> f = (endpoint) =>
            {
                count++;

                if (count < numFailures)
                {
                    throw new TimeoutException();
                }

                return NoOpTask();
            };

            client.ExecuteWithVirtualNetworkLoadBalancing(f);

            Assert.AreEqual(count, numFailures);
        }

        [TestMethod]
        public void TestExecuteWithVirtualNetworkLoadBalancingOneArg()
        {
            int numServers = 4;
            int numFailures = 3;
            var client = new HBaseClient(numServers);
            
            var arg1Value = "arg1";

            int count = 0;
            Func<string, string, Task> f = (arg1, endpoint) =>
            {
                Assert.AreEqual(arg1Value, arg1);

                count++;

                if (count < numFailures)
                {
                    throw new TimeoutException();
                }

                return NoOpTask();
            };

            client.ExecuteWithVirtualNetworkLoadBalancing(f, arg1Value);

            Assert.AreEqual(count, numFailures);
        }

        [TestMethod]
        public void TestExecuteWithVirtualNetworkLoadBalancingTwoArgs()
        {
            int numServers = 4;
            int numFailures = 3;
            var client = new HBaseClient(numServers);

            var arg1Value = "arg1";
            var arg2Value = "arg2";

            int count = 0;
            Func<string, string, string, Task> f = (arg1, arg2, endpoint) =>
            {
                Assert.AreEqual(arg1Value, arg1);
                Assert.AreEqual(arg2Value, arg2);

                count++;

                if (count < numFailures)
                {
                    throw new TimeoutException();
                }

                return NoOpTask();
            };

            client.ExecuteWithVirtualNetworkLoadBalancing(f, arg1Value, arg2Value);

            Assert.AreEqual(count, numFailures);
        }

        [TestMethod]
        public void TestFailedEndpointsExpiry()
        {
            int numServers = 5;

            Uri activeEndpoint;
            int expectedNumFailedEndpoints = 0;
            int expectedNumAvailableEndpoints = numServers;

            var balancer = new LoadBalancerRoundRobin(numRegionServers: numServers);

            Assert.AreEqual(LoadBalancerRoundRobin._refreshInterval.TotalMilliseconds, 10.0);

            for (int i = 0; i < numServers; i++)
            {
                activeEndpoint = balancer.GetEndpoint();
                Assert.IsNotNull(activeEndpoint);
                balancer.RecordFailure(activeEndpoint);

                expectedNumFailedEndpoints++;
                expectedNumAvailableEndpoints--;
            }

            var endpointsInfoList = (balancer._endpointIgnorePolicy as IgnoreFailedEndpointsPolicy)._endpoints.Values.ToArray();

            var failedBefore = Array.FindAll(endpointsInfoList, x => x.State == IgnoreFailedEndpointsPolicy.EndpointState.Failed);
            var availableBefore = Array.FindAll(endpointsInfoList, x => x.State == IgnoreFailedEndpointsPolicy.EndpointState.Available);

            Assert.AreEqual(failedBefore.Length, numServers);
            Assert.AreEqual(availableBefore.Length, 0);

            Thread.Sleep(100);

            var endpoint = balancer.GetEndpoint();
            Assert.IsNotNull(endpoint);
            balancer.RecordSuccess(endpoint);

            endpointsInfoList = (balancer._endpointIgnorePolicy as IgnoreFailedEndpointsPolicy)._endpoints.Values.ToArray();

            var failedAfter = Array.FindAll(endpointsInfoList, x => x.State == IgnoreFailedEndpointsPolicy.EndpointState.Failed);
            var availableAfter = Array.FindAll(endpointsInfoList, x => x.State == IgnoreFailedEndpointsPolicy.EndpointState.Available);

            Assert.AreEqual(failedAfter.Length, numServers-1);
            Assert.AreEqual(availableAfter.Length, 1);
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
        
        private async Task<int> EmitIntAsync(int count)
        {
            return await Task.FromResult<int>(count);
        }
        private async Task NoOpTask()
        {
            await Task.FromResult<int>(0);
        }
    }
}
