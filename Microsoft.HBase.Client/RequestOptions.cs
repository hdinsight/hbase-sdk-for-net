﻿// Copyright (c) Microsoft Corporation
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

namespace Microsoft.HBase.Client
{
    using Microsoft.HBase.Client.LoadBalancing;
    using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;

    public class RequestOptions
    {
        // TODO add some validation on the setters, e.g. AlternativeEndpoint needs to end with "/", RetryPolicy shouldn't be null, timeouts not negative.

        public RetryPolicy RetryPolicy { get; set; }
        public string AlternativeEndpoint { get; set; }
        public bool KeepAlive { get; set; }
        public int Timeout { get; set; }
        public int SerializationBufferSize { get; set; }
        public int ReceiveBufferSize { get; set; }
        public bool UseNagle { get; set; }

        public static RequestOptions GetDefaultOptions()
        {
            return new RequestOptions()
            {
                RetryPolicy = RetryPolicy.DefaultExponential,
                KeepAlive = true,
                Timeout = 30000,
                ReceiveBufferSize = 1024 * 1024,
                SerializationBufferSize = 1024 * 1024 * 2,
                UseNagle = false,
                AlternativeEndpoint = Constants.RestEndpointBase
            };
        }

    }
}
