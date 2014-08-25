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
    /// Represents the data for a <see cref="RetryUtility"/> event.
    /// </summary>
    [Serializable]
    public sealed class RetryEventArgs : EventArgs
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="RetryEventArgs"/> class.
        /// </summary>
        /// <param name="attemptNumber">The attempt number.</param>
        /// <param name="totalAttempts">The total attempts.</param>
        /// <param name="delay">The delay.</param>
        /// <param name="exception">The exception, if any.</param>
        public RetryEventArgs(int attemptNumber, int totalAttempts, TimeSpan delay, Exception exception)
        {
            AttemptNumber = attemptNumber;
            TotalAttempts = totalAttempts;
            Delay = delay;
            Exception = exception;
        }

        /// <summary>
        /// Gets the attempt number.
        /// </summary>
        /// <remarks>Base-1</remarks>
        public int AttemptNumber { get; private set; }

        /// <summary>
        /// Gets the delay between attempts.
        /// </summary>
        public TimeSpan Delay { get; private set; }

        /// <summary>
        /// Gets the exception, if any.
        /// </summary>
        /// <remarks>May be null</remarks>
        public Exception Exception { get; private set; }

        /// <summary>
        /// Gets the total number attempts that will be made.
        /// </summary>
        public int TotalAttempts { get; private set; }
    }
}
