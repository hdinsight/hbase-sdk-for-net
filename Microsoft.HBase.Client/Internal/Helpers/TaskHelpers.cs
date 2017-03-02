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

namespace Microsoft.HBase.Client.Internal.Helpers
{
    using System.Threading;
    using System;
    using System.Threading.Tasks;

    internal static class TaskHelpers
    {
        internal static CancellationTokenSource CreateLinkedCancellationTokenSource(CancellationToken token)
        {
            return token.CanBeCanceled ? CancellationTokenSource.CreateLinkedTokenSource(token) : new CancellationTokenSource();
        }

        internal static Task<T> WithTimeout<T>(this Task<T> task, TimeSpan timeout, string errorMessage)
        {
            return WithTimeout(task, timeout, errorMessage, CancellationToken.None);
        }

        internal static async Task<T> WithTimeout<T>(this Task<T> task, TimeSpan timeout, string errorMessage, CancellationToken token)
        {
            using (CancellationTokenSource cts = CreateLinkedCancellationTokenSource(token))
            {
                if (task == await Task.WhenAny(task, Task.Delay(timeout, cts.Token)))
                {
                    cts.Cancel();
                    return await task;
                }
            }

            // Ignore fault from task to avoid UnobservedTaskException
            task.IgnoreFault();

            if (token.IsCancellationRequested)
            {
                token.ThrowIfCancellationRequested();
            }
            throw new TimeoutException(String.Format("{0}. Timeout: {1}.", errorMessage, timeout));
        }

        internal static Task WithTimeout(this Task task, TimeSpan timeout, string errorMessage)
        {
            return WithTimeout(task, timeout, errorMessage, CancellationToken.None);
        }

        internal static async Task WithTimeout(this Task task, TimeSpan timeout, string errorMessage, CancellationToken token)
        {
            using (CancellationTokenSource cts = CreateLinkedCancellationTokenSource(token))
            {
                if (task == await Task.WhenAny(task, Task.Delay(timeout, cts.Token)))
                {
                    cts.Cancel();
                    await task;
                    return;
                }
            }

            // Ignore fault from task to avoid UnobservedTaskException
            task.IgnoreFault();

            if (token.IsCancellationRequested)
            {
                token.ThrowIfCancellationRequested();
            }
            throw new TimeoutException(String.Format("{0}. Timeout: {1}.", errorMessage, timeout));
        }

        internal static void IgnoreFault(this Task task)
        {
            if (task.IsCompleted)
            {
                var ignored = task.Exception;
            }
            else
            {
                task.ContinueWith
                    (t =>
                    {
                        var ignored = t.Exception;
                    },
                    TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously
                    );
            }
        }
    }

}
