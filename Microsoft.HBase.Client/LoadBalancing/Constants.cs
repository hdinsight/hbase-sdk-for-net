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

namespace Microsoft.HBase.Client.LoadBalancing
{
    public class Constants
    {
        public const string RestEndpointBase = "hbaserest/";
        public const string RestEndpointBaseZero = "hbaserest0/";

        public const string RefreshIntervalInMillisecondsConfigKey = "RefreshIntervalInMilliseconds";
        public const int RefreshIntervalInMillisecondsDefault = 15 * 60 * 1000;

        public const string WorkerHostNamePrefixConfigKey = "WorkerHostNamePrefix";
        public const string WorkerHostNamePrefixDefault = "workernode";

        public const string WorkerRestEndpointPortConfigKey = "WorkerRestEndpointPort";
        public const int WorkerRestEndpointPortDefault = 8090;

        public const int LoadBalancingHelperNumRetriesDefault = 5;

        public const int BackOffIntervalDefault = 10;
    }
}
