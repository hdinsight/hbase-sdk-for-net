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


using System;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.HBase.Client.LoadBalancing
{
    using System.Configuration;
    using System.Diagnostics;

    /// <summary>
    /// Round Robin implementation of the load balancer abstraction (in a virtual network environment).
    /// 
    /// Initialization requires the user to specify the list of server endpoints to be used by the load balancer
    /// for routing the requests.
    ///  
    /// </summary>
    public class LoadBalancerRoundRobin : ILoadBalancer
    {
        internal static string _workerHostNamePrefix;
        internal static int _workerRestEndpointPort;
        internal static TimeSpan _refreshInterval;
        
        internal Uri[] _allEndpoints;
        internal IEndpointIgnorePolicy _endpointIgnorePolicy;
        internal int _endpointIndex;
        internal object lockObj;

        public LoadBalancerRoundRobin(int numRegionServers = 1)
        {
            var servers = new List<string>();
            for (uint i = 0; i < numRegionServers; i++)
            {
                servers.Add(string.Format("{0}{1}", _workerHostNamePrefix, i));
            }
            
            InitializeEndpoints(servers);
        }

        public LoadBalancerRoundRobin(List<string> regionServerHostNames)
        {
            InitializeEndpoints(regionServerHostNames);
        }

        public Uri GetEndpoint()
        {
            Uri chosenEndpoint;

            lock (lockObj)
            {
                chosenEndpoint = ChooseEndpointRoundRobin(_endpointIgnorePolicy);
            }
            
            _endpointIgnorePolicy.OnEndpointAccessStart(chosenEndpoint);

            return chosenEndpoint;  
        }

        public void RecordSuccess(Uri endpoint)
        {
            _endpointIgnorePolicy.OnEndpointAccessCompletion(endpoint, EndpointAccessResult.Success);
        }

        public void RecordFailure(Uri endpoint)
        {
            _endpointIgnorePolicy.OnEndpointAccessCompletion(endpoint, EndpointAccessResult.Failure);
        }

        public int GetNumAvailableEndpoints()
        {
            return _allEndpoints.Length;
        }

        internal Uri ChooseEndpointRoundRobin(IEndpointIgnorePolicy policy)
        {
            Uri chosenEndpoint;
            int attemptCounter = 0;
            do
            {
                chosenEndpoint = _allEndpoints[_endpointIndex++ % _allEndpoints.Length];
                attemptCounter++;
                if(attemptCounter >= _allEndpoints.Length)
                {
                    Trace.TraceWarning("All endpoints were ignored by the policy. Avoiding further skipping.....");
                    break;
                }
            } while (policy.ShouldIgnoreEndpoint(chosenEndpoint));

            Debug.WriteLine("Endpoint {0} chosen for the request", chosenEndpoint);
            
            return chosenEndpoint;
        }
        
        private void InitializeEndpoints(List<string> regionServerHostNames)
        {
            Random rnd = new Random();

            lockObj = new object();

            var endpointsList = new List<Uri>();
            _endpointIndex = rnd.Next();

            foreach (var server in regionServerHostNames)
            {
                var candidate = string.Format("http://{0}:{1}", server, _workerRestEndpointPort);
                endpointsList.Add(new Uri(candidate));
            }
            
            _allEndpoints = endpointsList.OrderBy(x => rnd.Next()).ToArray();

            _endpointIgnorePolicy = new IgnoreFailedEndpointsPolicy(endpointsList, _refreshInterval);
        }

        static LoadBalancerRoundRobin()
        {
            Configure();
        }

        internal static void Configure()
        {
            _workerHostNamePrefix = ReadFromConfig<string>(Constants.WorkerHostNamePrefixConfigKey, String.Copy, Constants.WorkerHostNamePrefixDefault);
            _workerRestEndpointPort = ReadFromConfig<int>(Constants.WorkerRestEndpointPortConfigKey, Int32.Parse, Constants.WorkerRestEndpointPortDefault);

            var refreshIntervalFromConfig = ReadFromConfig<int>(Constants.RefreshIntervalInMillisecondsConfigKey, Int32.Parse, Constants.RefreshIntervalInMillisecondsDefault);
            _refreshInterval = TimeSpan.FromMilliseconds(refreshIntervalFromConfig);
        }

        internal static T ReadFromConfig<T>(string configKey, Func<string, T> parseConfigValue, T defaultValue)
        {
            T result;

            result = defaultValue;

            try
            {
                var configuredValueStr = ConfigurationManager.AppSettings[configKey];
                if (configuredValueStr != null)
                {
                    result = parseConfigValue(configuredValueStr);
                }
            }
            catch (Exception e)
            {
                Trace.TraceWarning("Failed to configure parameter with key {0}, failling back on default value {1}", configKey, defaultValue);
            }

            return result;
        }

    }
}
