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

namespace Microsoft.HBase.Client.LoadBalancing
{
    using System.Diagnostics.Contracts;
    using System.Linq;
    using System.Threading.Tasks;

    public class IgnoreFailedEndpointsPolicy : IEndpointIgnorePolicy
    {
        internal enum EndpointState
        {
            Available,
            Failed
        }
    
        internal class EndpointInformation
        {
            internal object lockObj;
            public string Name;
            public EndpointState State;
            public DateTime LastUpdatedTimestamp;
            public DateTime LastFailureTimestamp;
        }

        internal readonly IEndpointIgnorePolicy _innerPolicy;
        private readonly object _innerPolicyLockObj;

        internal Dictionary<Uri, EndpointInformation> _endpoints;
        internal readonly TimeSpan _failedEndpointExpiryInterval;

        public IEndpointIgnorePolicy InnerPolicy { get { return _innerPolicy; } }

        public IgnoreFailedEndpointsPolicy(List<Uri> endpoints, TimeSpan failedEndpointExpiryInterval, IEndpointIgnorePolicy innerPolicy = null)
        {
            _failedEndpointExpiryInterval = failedEndpointExpiryInterval;
            _innerPolicy = innerPolicy;
            _innerPolicyLockObj = new object();

            PopulateEndpoints(endpoints);
        }
    
        public void OnEndpointAccessStart(Uri endpointUri)
        {
            InvokeInnerPolicyForEndpointAccessBegin(endpointUri);

            var entry = _endpoints[endpointUri];
            UpdateEndpointEntryOnAccessStart(entry);
        }

        public void OnEndpointAccessCompletion(Uri endpointUri, EndpointAccessResult accessResult)
        {
            InvokeInnerPolicyForEndpointAccessCompletion(endpointUri, accessResult);

            var entry = _endpoints[endpointUri];
            UpdateEndpointEntryOnAccessCompletion(entry, accessResult);
        }

        public bool ShouldIgnoreEndpoint(Uri endpoint)
        {
            if (!_endpoints.ContainsKey(endpoint))
            {
                return true;
            }

            var entry = _endpoints[endpoint];
            EndpointState currentState;
            lock (entry)
            {
                currentState = entry.State;
            }

            return (currentState == EndpointState.Failed);
        }

        public void RefreshIgnoredList()
        {
            var failedEntries = Array.FindAll(_endpoints.Values.ToArray(), x=> x.State == EndpointState.Failed);

            var now = DateTime.UtcNow;
            Parallel.ForEach(failedEntries, entry =>
            {
                lock (entry.lockObj)
                {
                    RefreshFailedEndpointEntry(entry, now);
                }
            });
        }

        internal void PopulateEndpoints(List<Uri> endpoints)
        {
            _endpoints = new Dictionary<Uri, EndpointInformation>();

            foreach (Uri e in endpoints)
            {
                var info = new EndpointInformation() 
                { 
                    lockObj = new object(),
                    Name = e.OriginalString,
                    State = EndpointState.Available,
                    LastUpdatedTimestamp = DateTime.UtcNow,
                    LastFailureTimestamp = default(DateTime)
                };
                _endpoints.Add(e, info);
            }
        }

        internal void InvokeInnerPolicyForEndpointAccessBegin(Uri endpointUri)
        {
            if (_innerPolicy != null)
            {
                lock (_innerPolicyLockObj)
                {
                    _innerPolicy.OnEndpointAccessStart(endpointUri);
                }
            }
        }

        internal void InvokeInnerPolicyForEndpointAccessCompletion(Uri endpointUri, EndpointAccessResult accessResult)
        {
            if (_innerPolicy != null)
            {
                lock (_innerPolicyLockObj)
                {
                    _innerPolicy.OnEndpointAccessCompletion(endpointUri, accessResult);
                }
            }
        }

        internal void UpdateEndpointEntryOnAccessStart(EndpointInformation entry)
        {
            var now = DateTime.UtcNow;
            lock (entry.lockObj)
            {
                entry.LastUpdatedTimestamp = now;
            }
        }

        internal void UpdateEndpointEntryOnAccessCompletion(EndpointInformation entry, EndpointAccessResult accessResult)
        {
            var now = DateTime.UtcNow;
            lock (entry.lockObj)
            {
                if (accessResult == EndpointAccessResult.Failure)
                {
                    entry.State = EndpointState.Failed;
                    entry.LastFailureTimestamp = now;
                }
                else if (entry.State == EndpointState.Failed)
                {
                    RefreshFailedEndpointEntry(entry, now);
                }
                entry.LastUpdatedTimestamp = now;
            }
        }
        
        // Assumes entry is locked.
        internal void RefreshFailedEndpointEntry(EndpointInformation entry, DateTime refreshTimestamp)
        {
            Contract.Assert(entry.LastFailureTimestamp != default(DateTime));

            if (refreshTimestamp.Subtract(entry.LastFailureTimestamp) > _failedEndpointExpiryInterval)
            {
                entry.State = EndpointState.Available;
            }
        }
    }
}
