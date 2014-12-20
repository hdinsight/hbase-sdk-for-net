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
    /// The list of endpoints is populated internally and maintained in 3 partitioned sets
    /// 1. active endpoint to serve the current request
    /// 2. list of other available (not failed) endpoints other than active
    /// 3. list of endpoints that failed for recent request attempts
    /// 
    /// Each failed endpoint entry has the timestamp when it was moved to the failed set
    /// 
    /// On a given request, the failed set is inspected and entries having exceeded the expiry interval 
    /// are recycled back to the available set.
    /// 
    /// Refreshing the active endpoint involves shuffling the available endpoints and picking one for use 
    /// 
    /// </summary>
    public class LoadBalancerRoundRobin : ILoadBalancer
    {
        internal static string _workerHostNamePrefix;
        internal static int _workerRestEndpointPort;
        internal static TimeSpan _refreshInterval;
        

        internal class FailedEndpointEntry
        {
            public string Endpoint;
            public DateTime FailureDetectedTimestamp;
        }

        internal int _numRegionServers;

        internal List<string> _allEndpoints;

        internal string _activeEndpoint;
        internal List<string> _availableEndpoints;
        internal List<FailedEndpointEntry> _failedEndpoints;



        static LoadBalancerRoundRobin()
        {
            Configure();
        }

        public LoadBalancerRoundRobin(int numRegionServers = 1)
        {
            _numRegionServers = numRegionServers;

            var servers = new List<string>();
            for (uint i = 0; i < _numRegionServers; i++)
            {
                servers.Add(string.Format("{0}{1}", _workerHostNamePrefix, i));
            }

            PopulateCandidates(servers);
        }

        public LoadBalancerRoundRobin(List<string> regionServerHostNames)
        {
            _numRegionServers = regionServerHostNames.Count;

            PopulateCandidates(regionServerHostNames);
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

        private void PopulateCandidates(List<string> regionServerHostNames)
        {
            _allEndpoints = new List<string>();

            _activeEndpoint = null;
            _availableEndpoints = new List<string>();
            _failedEndpoints = new List<FailedEndpointEntry>();

            foreach (var server in regionServerHostNames)
            {
                var candidate = string.Format("http://{0}:{1}", server, _workerRestEndpointPort);
                _allEndpoints.Add(candidate);
            }

            _availableEndpoints.AddRange(_allEndpoints);

            Shuffle(_availableEndpoints);
        }

        public Uri GetWorkerNodeEndPointBaseNext()
        {
            RefreshFailedEndpoints();

            if (_activeEndpoint != null)
            {
                _failedEndpoints.Add(GetFailedEndpointEntry(_activeEndpoint));
                _activeEndpoint = null;
            }

            if (_availableEndpoints.Count == 0)
            {
                _availableEndpoints.AddRange(GetEndpoints(_failedEndpoints));
                _failedEndpoints.Clear();
            }

            _activeEndpoint = _availableEndpoints.FirstOrDefault();
            if (_activeEndpoint == null)
            {
                return null;
            }
            _availableEndpoints.Remove(_activeEndpoint);
            
            Shuffle(_availableEndpoints);

            // Console.WriteLine("Endpoint {0} chosen for the request" + _activeEndpoint);

            return new Uri(_activeEndpoint);  
        }

        public void Reset()
        {
            _activeEndpoint = null;
            _failedEndpoints.Clear();
            _availableEndpoints.Clear();

            _availableEndpoints.AddRange(_allEndpoints);
        }

        public int GetWorkersCount()
        {
            return _numRegionServers;
        }

        private void Shuffle<T>(IList<T> list)
        {
            int n = list.Count;
            Random rnd = new Random();
            while (n > 1)
            {
                int k = (rnd.Next(0, n) % n);
                n--;
                T value = list[k];
                list[k] = list[n];
                list[n] = value;
            }
        }

        internal void RefreshFailedEndpoints()
        {
            var now = DateTime.UtcNow;

            Predicate<FailedEndpointEntry> expiryCondition = e => now.Subtract(e.FailureDetectedTimestamp) > _refreshInterval;

            var refreshCandidatesList = _failedEndpoints.FindAll(expiryCondition);
            _failedEndpoints.RemoveAll(expiryCondition);

            _availableEndpoints.AddRange(GetEndpoints(refreshCandidatesList));
        }

        internal FailedEndpointEntry GetFailedEndpointEntry(string endpoint)
        {
            return new FailedEndpointEntry
            {
                Endpoint = endpoint,
                FailureDetectedTimestamp = DateTime.UtcNow
            };
        }

        internal List<string> GetEndpoints(List<FailedEndpointEntry> failedEndpointsList)
        {
            var endpoints = new List<string>();

            foreach (var entry in failedEndpointsList.Where(e => (e.Endpoint !=null)))
            {
                endpoints.Add(entry.Endpoint);
            }

            return endpoints;
        }

        internal void PrintStatusForDebugging()
        {
            Debug.Write("| Active endpoint : ");
            Debug.Write(_activeEndpoint ?? String.Empty);

            Debug.Write("| Available endpoints : ");
            foreach (var e in _availableEndpoints)
            {
                Debug.Write(e + " ");
            }

            Debug.Write("| Failed endpoints : ");
            foreach (var e in _failedEndpoints)
            {
                Debug.Write(e + " ");
            }
            Debug.WriteLine(Environment.NewLine);
        }
    }
}
