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

namespace Microsoft.HBase.Client.Tests.Utilities
{
    using System;

    /// <summary>
    /// Represents a retry policy.
    /// </summary>
    internal interface IRetryUtility
    {
        /// <summary>
        /// Occurs when an attempt fails.
        /// </summary>
        event EventHandler<RetryEventArgs> AttemptFailed;

        /// <summary>
        /// Occurs when an attempt is starting.
        /// </summary>
        event EventHandler<RetryEventArgs> AttemptStart;

        /// <summary>
        /// Occurs when an attempt is succeeds.
        /// </summary>
        event EventHandler<RetryEventArgs> AttemptSucceeded;

        /// <summary>
        /// Gets or sets the retry count.
        /// </summary>
        /// <remarks>
        /// The value must be greater than or equal to zero.
        /// </remarks>
        int RetryCount { get; }

        /// <summary>
        /// Gets the retry wait.
        /// </summary>
        /// <remarks>
        /// The value must be greater than or equal to TimeSpan.Zero.
        /// </remarks>
        TimeSpan RetryWait { get; }

        /// <summary>
        /// Tries the specified work with the present count and wait.
        /// </summary>
        /// <typeparam name="T">The type returned by the work.</typeparam>
        /// <param name="work">The work.</param>
        /// <returns>The object returned by the work.</returns>
        /// <exception cref="System.ArgumentNullException">work</exception>
        T Attempt<T>(Func<T> work);

        /// <summary>
        /// Tries the specified work with the preset count and wait.
        /// </summary>
        /// <param name="work">The work.</param>
        /// <exception cref="System.ArgumentNullException">work</exception>
        void Attempt(Action work);
    }
}
