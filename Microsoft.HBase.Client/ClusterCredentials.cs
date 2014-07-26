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
namespace Microsoft.HBase.Client
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Security;
    using System.Threading;
    using Microsoft.HBase.Client.Internal;

    /// <summary>
    /// Credentials for an HBase cluster.
    /// </summary>
    public sealed class ClusterCredentials : IDisposable
    {
        private SecureString _clusterPassword;
        private int _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClusterCredentials"/> class.
        /// </summary>
        /// <param name="clusterUri">The cluster URI.</param>
        /// <param name="userName">The username.</param>
        /// <param name="password">The password.</param>
        public ClusterCredentials(Uri clusterUri, string userName, string password)
        {
            clusterUri.ArgumentNotNull("clusterUri");
            userName.ArgumentNotNullNorEmpty("username");
            password.ArgumentNotNullNorEmpty("password");

            ClusterUri = clusterUri;
            UserName = userName;
            _clusterPassword = password.ToSecureString();
            _clusterPassword.MakeReadOnly();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ClusterCredentials"/> class.
        /// </summary>
        /// <param name="clusterUri">The cluster URI.</param>
        /// <param name="userName">The username.</param>
        /// <param name="password">The password.</param>
        public ClusterCredentials(Uri clusterUri, string userName, SecureString password)
        {
            clusterUri.ArgumentNotNull("clusterUri");
            userName.ArgumentNotNullNorEmpty("username");
            password.ArgumentNotNull("securePassword");

            ClusterUri = clusterUri;
            UserName = userName;
            _clusterPassword = password.Copy();
            _clusterPassword.MakeReadOnly();
        }

        /// <inheritdoc/>
        [SuppressMessage("Microsoft.Design", "CA1063:ImplementIDisposableCorrectly")]
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                // already disposed or disposing
                return;
            }

            Dispose(true);

            // Use SupressFinalize in case a subclass of this type implements a finalizer.
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources.
        /// </summary>
        /// <param name="disposing"> 
        /// Use <c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources. 
        /// </param>
        private void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_clusterPassword != null)
                {
                    _clusterPassword.Dispose();
                    _clusterPassword = null;
                }
            }
        }

        /// <summary>
        /// Gets the cluster password.
        /// </summary>
        /// <value>
        /// The cluster password.
        /// </value>
        public SecureString ClusterPassword
        {
            get
            {
                if (_disposed != 0)
                {
                    throw new ObjectDisposedException(GetType().Name);
                }
                return _clusterPassword.Copy();
            }
        }

        /// <summary>
        /// Gets the cluster URI.
        /// </summary>
        /// <value>
        /// The cluster URI.
        /// </value>
        public Uri ClusterUri { get; private set; }

        /// <summary>
        /// Gets the name of the user.
        /// </summary>
        /// <value>
        /// The name of the user.
        /// </value>
        public string UserName { get; private set; }
    }
}
