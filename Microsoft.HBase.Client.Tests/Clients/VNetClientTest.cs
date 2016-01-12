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

namespace Microsoft.HBase.Client.Tests.Clients
{
    using System.Collections.Generic;
    using Microsoft.HBase.Client.LoadBalancing;
    using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    //    [TestClass]
    public class VNetClientTest : HBaseClientTestBase
    {
        public override IHBaseClient CreateClient()
        {
            var regionServerHostNames = new List<string>();
            // TODO add different behaviour for linux vs. windows   
            regionServerHostNames.Add("wn0-");
            regionServerHostNames.Add("wn1-");

            var options = RequestOptions.GetDefaultOptions();
            options.RetryPolicy = RetryPolicy.NoRetry;
            options.TimeoutMillis = 30000;
            options.KeepAlive = false;
            options.Port = 8090;
            options.AlternativeEndpoint = "/";

            return new HBaseClient(null, options, new LoadBalancerRoundRobin(regionServerHostNames));
        }
    }
}
