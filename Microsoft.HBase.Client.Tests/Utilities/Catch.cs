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
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Catches exception thrown by an action.
    /// </summary>
    /// <remarks>
    /// Originally borrowed from Machine.Specifications, licensed under MS-PL.
    /// </remarks>
    internal static class Catch
    {
        /// <summary>
        /// Executes and action, capturing the thrown exception if any.
        /// </summary>
        /// <param name = "throwingAction">
        /// The action to execute.
        /// </param>
        /// <returns>
        /// The thrown exception; otherwise null.
        /// </returns>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes", Justification = "This is a testing utility (MWP).")]
        internal static Exception Exception(Action throwingAction)
        {
            try
            {
                throwingAction();
            }
            catch (Exception exception)
            {
                return exception;
            }

            return null;
        }
    }
}
