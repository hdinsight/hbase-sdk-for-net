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
    using System.IO;
    using System.Net;
    using System.Threading.Tasks;

    public interface IWebRequester
    {
        Response IssueWebRequest(string endpoint, string query, string method, Stream input, RequestOptions options);

        Task<Response> IssueWebRequestAsync(string endpoint, string query, string method, Stream input, RequestOptions options);
    }

    public class Response : IDisposable
    {
        public HttpWebResponse WebResponse { get; set; }
        public TimeSpan RequestLatency { get; set; }
        public Action<Response> PostRequestAction { get; set; }

        /// <summary>
        /// Used to detect redundant calls to <see cref="IDisposable.Dispose"/>.
        /// </summary>
        private bool _isDisposed; // To detect redundant calls

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources.
        /// </summary>
        /// <param name="disposing">
        /// <see langword="true"/> to release both managed and unmanaged resources; <see
        /// langword="false"/> to release only unmanaged resources.
        /// </param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_isDisposed)
            {
                if (disposing)
                {
                    if (PostRequestAction != null)
                    {
                        PostRequestAction(this);
                        PostRequestAction = null;
                    }
                    WebResponse.Dispose();
                    WebResponse = null;
                }
                _isDisposed = true;
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting
        /// unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            // Since this class is not sealed, derived classes may introduce unmanaged resources, so
            // suppress Finalize calls if the instance has been disposed.
            GC.SuppressFinalize(this);
        }
    }
}