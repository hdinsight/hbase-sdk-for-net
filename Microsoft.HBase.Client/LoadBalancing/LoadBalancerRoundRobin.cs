using System;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.HBase.Client.LoadBalancing
{
    public class LoadBalancerRoundRobin : ILoadBalancer
    {
        internal const string _workerHostNamePrefix = "workernode";
        internal const int _workerRestEndpointPort = 8090;

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

        internal const int DefaultRefreshIntervalInMilliseconds = 15*60*1000;
        
        internal TimeSpan _refreshInterval;

        public LoadBalancerRoundRobin(int numRegionServers = 1, int refreshIntervalInMilliseconds = DefaultRefreshIntervalInMilliseconds)
        {
            _numRegionServers = numRegionServers;

            var servers = new List<string>();
            for (uint i = 0; i < _numRegionServers; i++)
            {
                servers.Add(string.Format("{0}{1}", _workerHostNamePrefix, i));
            }

            PopulateCandidates(servers, TimeSpan.FromMilliseconds(refreshIntervalInMilliseconds));
        }

        public LoadBalancerRoundRobin(List<string> regionServerHostNames, int refreshIntervalInMilliseconds = DefaultRefreshIntervalInMilliseconds)
        {
            _numRegionServers = regionServerHostNames.Count;

            PopulateCandidates(regionServerHostNames, TimeSpan.FromMilliseconds(refreshIntervalInMilliseconds));
        }

        private void PopulateCandidates(List<string> regionServerHostNames, TimeSpan refreshInterval)
        {
            _refreshInterval = refreshInterval;

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

        private void PrintStatus()
        {
            Console.Write("| Active endpoint : ");
            Console.Write(_activeEndpoint ?? String.Empty);

            Console.Write("| Available endpoints : ");
            foreach (var e in _availableEndpoints)
            {
                Console.Write(e + " ");
            }
            
            Console.Write("| Failed endpoints : ");
            foreach (var e in _failedEndpoints)
            {
                Console.Write(e + " ");
            }
            Console.WriteLine(Environment.NewLine);
        }
    }
}
