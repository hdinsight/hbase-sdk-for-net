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
using System.Threading;

namespace Microsoft.HBase.Client.LoadBalancing
{
    using System.Diagnostics;


    /// <summary>
    /// Helper function to invoke the load balancer with a configurable policy for retry and backoff  
    /// </summary>
    public class LoadBalancingHelper
    {
        public static void Execute(Action method, IRetryPolicy retryPolicy, IBackOffScheme backOffScheme, int numRetries = Constants.LoadBalancingHelperNumRetriesDefault)
        {
            int retryCount = 0;
            bool requestSuccess = false;
            while ((!requestSuccess) && (retryCount < numRetries))
            {
                try
                {
                    method();
                    requestSuccess = true;
                }
                catch (Exception ex)
                {
                    Trace.TraceError("\tAttempt {0} failed with exception {1} - {2}", retryCount, ex.GetType(), ex.Message);

                    retryCount++;
                    if ((retryCount < numRetries) && (retryPolicy.ShouldRetryAttempt(ex)))
                    {
                        var sleepInterval = backOffScheme.GetRetryInterval(retryCount);
                        if (sleepInterval != default (TimeSpan))
                        {
                            Trace.TraceInformation("\tWill retry after {0} milliseconds......", sleepInterval.TotalMilliseconds);
                            Thread.Sleep(sleepInterval);
                        }
                    }
                    else
                    {
                        throw;
                    }
                }
            }
        }
    }
}
