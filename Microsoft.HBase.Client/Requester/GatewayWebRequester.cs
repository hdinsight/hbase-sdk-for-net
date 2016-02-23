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
    using Microsoft.HBase.Client.Internal;
    
    /// <summary>
    /// 
    /// </summary>
    public sealed class GatewayWebRequester : IWebRequester
    {
        private readonly string _contentType;
        private readonly CredentialCache _credentialCache;
        private readonly ClusterCredentials _credentials;

        /// <summary>
        /// Initializes a new instance of the <see cref="GatewayWebRequester"/> class.
        /// </summary>
        /// <param name="credentials">The credentials.</param>
        /// <param name="contentType">Type of the content.</param>
        public GatewayWebRequester(ClusterCredentials credentials, string contentType = "application/x-protobuf")
        {
            credentials.ArgumentNotNull("credentials");

            _credentials = credentials;
            _contentType = contentType;
            _credentialCache = new CredentialCache();
            InitCache();
        }

        /// <summary>
        /// Issues the web request.
        /// </summary>
        /// <param name="endpoint">The endpoint.</param>
        /// <param name="method">The method.</param>
        /// <param name="input">The input.</param>
        /// <param name="options">request options</param>
        /// <returns></returns>
        public Response IssueWebRequest(string endpoint, string query, string method, Stream input, RequestOptions options)
        {
            return IssueWebRequestAsync(endpoint, query, method, input, options).Result;
        }

        /// <summary>
        /// Issues the web request asynchronous.
        /// </summary>
        /// <param name="endpoint">The endpoint.</param>
        /// <param name="method">The method.</param>
        /// <param name="input">The input.</param>
        /// <param name="options">request options</param>
        /// <returns></returns>
        public async Task<Response> IssueWebRequestAsync(
            string endpoint, string query, string method, Stream input, RequestOptions options)
        {
            options.Validate();
            Stopwatch watch = Stopwatch.StartNew();
            UriBuilder builder = new UriBuilder(
                _credentials.ClusterUri.Scheme,
                _credentials.ClusterUri.Host,
                options.Port,
                options.AlternativeEndpoint + endpoint);

            if (query != null)
            {
                builder.Query = query;
            }
            
            Debug.WriteLine("Issuing request {0} to endpoint {1}", Trace.CorrelationManager.ActivityId, builder.Uri);
            HttpWebRequest httpWebRequest = WebRequest.CreateHttp(builder.Uri);
            httpWebRequest.ServicePoint.ReceiveBufferSize = options.ReceiveBufferSize;
            httpWebRequest.ServicePoint.UseNagleAlgorithm = options.UseNagle;
            httpWebRequest.Timeout = options.TimeoutMillis;
            httpWebRequest.KeepAlive = options.KeepAlive;
            httpWebRequest.Credentials = _credentialCache;
            httpWebRequest.PreAuthenticate = true;
            httpWebRequest.Method = method;
            httpWebRequest.Accept = _contentType;
            httpWebRequest.ContentType = _contentType;
            // This allows 304 (NotModified) requests to catch
            //https://msdn.microsoft.com/en-us/library/system.net.httpwebrequest.allowautoredirect(v=vs.110).aspx
            httpWebRequest.AllowAutoRedirect = false;

            if (options.AdditionalHeaders != null)
            {
                foreach (var kv in options.AdditionalHeaders)
                {
                    httpWebRequest.Headers.Add(kv.Key, kv.Value);
                }
            }

            if (input != null)
            {
                // expecting the caller to seek to the beginning or to the location where it needs to be copied from
                using (Stream req = await httpWebRequest.GetRequestStreamAsync())
                {
                    await input.CopyToAsync(req);
                }
            }

            var response = (await httpWebRequest.GetResponseAsync()) as HttpWebResponse;
            return new Response()
            {
                WebResponse = response,
                RequestLatency = watch.Elapsed
            };
        }

        private void InitCache()
        {
            _credentialCache.Add(_credentials.ClusterUri, "Basic", new NetworkCredential(_credentials.UserName, _credentials.ClusterPassword));
        }
    }
}
