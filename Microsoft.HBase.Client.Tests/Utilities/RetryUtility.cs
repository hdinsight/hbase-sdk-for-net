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
    using System.Threading;
    using Microsoft.HBase.Client.Internal;

    /// <summary>
    /// A simple retry policy.
    /// </summary>
    internal class RetryUtility : IRetryUtility
    {
        private readonly int _retryCount;
        private readonly TimeSpan _retryWait;

        /// <summary>
        /// Initializes a new instance of the <see cref="RetryUtility"/> class.
        /// </summary>
        /// <param name="retryCount">The retry count.</param>
        /// <param name="retryWait">The retry wait.</param>
        /// <exception cref="System.ArgumentOutOfRangeException">
        /// retryCount;The value must be greater than or equal to zero.
        /// or
        /// retryWait;The value must be greater than or equal to TimeSpan.Zero.
        /// </exception>
        public RetryUtility(int retryCount, TimeSpan retryWait)
        {
            if (retryCount < 0)
            {
                throw new ArgumentOutOfRangeException("retryCount", retryCount, "The value must be greater than or equal to zero.");
            }

            if (retryWait < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException("retryWait", retryWait, "The value must be greater than or equal to TimeSpan.Zero.");
            }

            _retryCount = retryCount;
            _retryWait = retryWait;
        }

        /// <inheritdoc/>
        public event EventHandler<RetryEventArgs> AttemptFailed;

        /// <inheritdoc/>
        public event EventHandler<RetryEventArgs> AttemptStart;

        /// <inheritdoc/>
        public event EventHandler<RetryEventArgs> AttemptSucceeded;

        /// <inheritdoc/>
        public int RetryCount
        {
            get { return _retryCount; }
        }

        /// <inheritdoc/>
        public TimeSpan RetryWait
        {
            get { return _retryWait; }
        }

        /// <inheritdoc/>
        public T Attempt<T>(Func<T> work)
        {
            work.ArgumentNotNull("work");

            return AttemptImplementation(work);
        }

        /// <inheritdoc/>
        public void Attempt(Action work)
        {
            work.ArgumentNotNull("work");

            // create a work out of the work to share implementation.
            Func<object> fakeFunc = () =>
            {
                work();
                return null;
            };

            AttemptImplementation(fakeFunc);
        }

        private T AttemptImplementation<T>(Func<T> workToRetry)
        {
            T rv = default(T);

            int attemptCounter = 0;

            while (attemptCounter < RetryCount + 1)
            {
                attemptCounter++;
                try
                {
                    OnAttemptStart(attemptCounter);
                    rv = workToRetry();
                    OnAttemptSucceeded(attemptCounter);
                    return rv;
                }
                catch (Exception e)
                {
                    OnAttemptFailed(attemptCounter, e);

                    if (attemptCounter >= RetryCount + 1)
                    {
                        // this is the last attempt, rethrow the exception.
                        throw;
                    }

                    Thread.Sleep(RetryWait);
                }
            }

            return rv;
        }

        private void OnAttemptFailed(int attemptNumber, Exception e)
        {
            EventHandler<RetryEventArgs> handler = AttemptFailed;
            if (handler != null)
            {
                var args = new RetryEventArgs(attemptNumber, RetryCount + 1, RetryWait, e);
                handler(this, args);
            }
        }

        private void OnAttemptStart(int attemptNumber)
        {
            EventHandler<RetryEventArgs> handler = AttemptStart;
            if (handler != null)
            {
                var args = new RetryEventArgs(attemptNumber, RetryCount + 1, RetryWait, null);
                handler(this, args);
            }
        }

        private void OnAttemptSucceeded(int attemptNumber)
        {
            EventHandler<RetryEventArgs> handler = AttemptSucceeded;
            if (handler != null)
            {
                var args = new RetryEventArgs(attemptNumber, RetryCount + 1, RetryWait, null);
                handler(this, args);
            }
        }
    }
}
