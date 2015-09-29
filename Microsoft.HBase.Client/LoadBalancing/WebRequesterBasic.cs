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
    using System;
    using System.Diagnostics;
    using System.Diagnostics.Contracts;
    using System.IO;
    using System.Net;
    using System.Threading.Tasks;
    using Microsoft.HBase.Client.Internal;
    using Microsoft.HBase.Client.LoadBalancing;

    /// <summary>
    /// 
    /// </summary>
    public sealed class WebRequesterBasic : IWebRequester
    {
        private readonly string _contentType;
        private readonly CredentialCache _credentialCache;
        
        /// <summary>
        /// Initializes a new instance of the <see cref="WebRequesterSecure"/> class.
        /// </summary>
        /// <param name="credentials">The credentials.</param>
        /// <param name="contentType">Type of the content.</param>
        public WebRequesterBasic(string contentType = "application/x-protobuf")
        {
            _contentType = contentType;
            _credentialCache = null;
            Timeout = TimeSpan.FromSeconds(100);
        }

        /// <summary>
        /// Issues the web request.
        /// </summary>
        /// <param name="endpoint">The endpoint.</param>
        /// <param name="method">The method.</param>
        /// <param name="input">The input.</param>
        /// <returns></returns>
        public HttpWebResponse IssueWebRequest(string endpoint, string method = "GET", Stream input = null)
        {
            var response =  IssueWebRequestAsync(endpoint, method, input).Result;
            
            return response;
        }
        
        /// <summary>
        /// Issues the web request asynchronous.
        /// </summary>
        /// <param name="endpoint">The endpoint.</param>
        /// <param name="method">The method.</param>
        /// <param name="input">The input.</param>
        /// <param name="alternativeEndpointBase">The alternative endpoint base.</param>
        /// <returns></returns>
        public async Task<HttpWebResponse> IssueWebRequestAsync(string endpoint, string method = "GET", Stream input = null, string alternativeEndpointBase = null)
        {
            Trace.CorrelationManager.ActivityId = Guid.NewGuid();

            Task<HttpWebResponse> result = new Task<HttpWebResponse>(() => { return null; });

            if (alternativeEndpointBase == null)
            {
                // alternativeEndpointBase needed in VNET mode. Abort!

                return await result as HttpWebResponse;
            }
            
            var target = new Uri(new Uri(alternativeEndpointBase), endpoint);

            Trace.TraceInformation("Issuing request {0} to endpoint {1}", Trace.CorrelationManager.ActivityId, target);

            HttpWebRequest httpWebRequest = WebRequest.CreateHttp(target);
            httpWebRequest.Credentials = _credentialCache;
            httpWebRequest.PreAuthenticate = true;
            httpWebRequest.Method = method;
            httpWebRequest.Accept = _contentType;
            httpWebRequest.ContentType = _contentType;
            httpWebRequest.Timeout = (int)Timeout.TotalMilliseconds;

            if (input != null)
            {
                // seek to the beginning, so we copy everything in this buffer
                input.Seek(0, SeekOrigin.Begin);
                using (Stream req = httpWebRequest.GetRequestStream())
                {
                    await input.CopyToAsync(req);
                }
            }

            Trace.TraceInformation("Waiting for response for request {0} to endpoint {1}", Trace.CorrelationManager.ActivityId, target);

            var response = (await httpWebRequest.GetResponseAsync()) as HttpWebResponse;

            Trace.TraceInformation("Web request {0} to endpoint {1} successful!", Trace.CorrelationManager.ActivityId, target);

            return response;
        }

        public TimeSpan Timeout { get; set; }
    }
}
