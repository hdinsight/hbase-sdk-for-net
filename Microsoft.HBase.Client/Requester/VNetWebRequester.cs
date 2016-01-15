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

namespace Microsoft.HBase.Client.Requester
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Net;
    using System.Threading.Tasks;
    using Microsoft.HBase.Client.LoadBalancing;

    /// <summary>
    /// 
    /// </summary>
    public sealed class VNetWebRequester : IWebRequester
    {
        private readonly ILoadBalancer _balancer;
        private readonly string _contentType;
        private readonly CredentialCache _credentialCache;

        /// <summary>
        /// Initializes a new instance of the <see cref="VNetWebRequester"/> class.
        /// </summary>
        /// <param name="balancer">the load balancer for the vnet nodes</param>
        /// <param name="contentType">Type of the content.</param>
        public VNetWebRequester(ILoadBalancer balancer, string contentType = "application/x-protobuf")
        {
            _balancer = balancer;
            _contentType = contentType;
            _credentialCache = null;
        }

        /// <summary>
        /// Issues the web request.
        /// </summary>
        /// <param name="endpoint">The endpoint.</param>
        /// <param name="method">The method.</param>
        /// <param name="input">The input.</param>
        /// <param name="options">request options</param>
        /// <returns></returns>
        public Response IssueWebRequest(string endpoint, string method, Stream input, RequestOptions options)
        {
            return IssueWebRequestAsync(endpoint, method, input, options).Result;
        }

        /// <summary>
        /// Issues the web request asynchronous.
        /// </summary>
        /// <param name="endpoint">The endpoint.</param>
        /// <param name="method">The method.</param>
        /// <param name="input">The input.</param>
        /// <param name="options">request options</param>
        /// <returns></returns>
        public async Task<Response> IssueWebRequestAsync(string endpoint, string method, Stream input, RequestOptions options)
        {
            options.Validate();
            Stopwatch watch = Stopwatch.StartNew();
            Trace.CorrelationManager.ActivityId = Guid.NewGuid();
            var balancedEndpoint = _balancer.GetEndpoint();

            UriBuilder builder = new UriBuilder(
                balancedEndpoint.Scheme,
                balancedEndpoint.Host,
                options.Port,
                options.AlternativeEndpoint + endpoint);

            var target = builder.Uri;

            try
            {
                Debug.WriteLine("Issuing request {0} to endpoint {1}", Trace.CorrelationManager.ActivityId, target);

                HttpWebRequest httpWebRequest = WebRequest.CreateHttp(target);
                httpWebRequest.ServicePoint.ReceiveBufferSize = options.ReceiveBufferSize;
                httpWebRequest.ServicePoint.UseNagleAlgorithm = options.UseNagle;
                httpWebRequest.Timeout = options.TimeoutMillis;
                httpWebRequest.KeepAlive = options.KeepAlive;
                httpWebRequest.Credentials = _credentialCache;
                httpWebRequest.PreAuthenticate = true;
                httpWebRequest.Method = method;
                httpWebRequest.Accept = _contentType;
                httpWebRequest.ContentType = _contentType;

                if (options.AdditionalHeaders != null)
                {
                    foreach (var kv in options.AdditionalHeaders)
                    {
                        httpWebRequest.Headers.Add(kv.Key, kv.Value);
                    }
                }

                if (input != null)
                {
                    // seek to the beginning, so we copy everything in this buffer
                    input.Seek(0, SeekOrigin.Begin);
                    using (Stream req = httpWebRequest.GetRequestStream())
                    {
                        await input.CopyToAsync(req);
                    }
                }

                Debug.WriteLine("Waiting for response for request {0} to endpoint {1}", Trace.CorrelationManager.ActivityId, target);

                var response = (await httpWebRequest.GetResponseAsync()) as HttpWebResponse;

                Debug.WriteLine("Web request {0} to endpoint {1} successful!", Trace.CorrelationManager.ActivityId, target);

                return new Response()
                {
                    WebResponse = response,
                    RequestLatency = watch.Elapsed,
                    PostRequestAction = (r) =>
                    {
                        if (r.WebResponse.StatusCode == HttpStatusCode.OK || r.WebResponse.StatusCode == HttpStatusCode.Created)
                        {
                            _balancer.RecordSuccess(balancedEndpoint);
                        }
                        else
                        {
                            _balancer.RecordFailure(balancedEndpoint);
                        }
                    }
                };
            }
            catch (Exception e)
            {
                _balancer.RecordFailure(balancedEndpoint);
                Debug.WriteLine("Web request {0} to endpoint {1} failed!", Trace.CorrelationManager.ActivityId, target);
                throw e;
            }
        }
    }
}
