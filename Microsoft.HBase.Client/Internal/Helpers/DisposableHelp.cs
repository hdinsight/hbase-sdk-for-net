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
namespace Microsoft.HBase.Client.Internal
{
    using System;

    /// <summary>
    /// Helper utilities for working with IDisposable.
    /// </summary>
    internal static class DisposableHelp
    {
        /// <summary>
        /// Safely creates a disposable object with a default constructor.
        /// </summary>
        /// <remarks>
        /// The motivation is to avoid <see href="http://msdn.microsoft.com/en-us/library/ms182289.aspx">CA2000</see> warnings.
        /// </remarks>
        /// <exception cref="Exception">
        /// Thrown when an exception error condition occurs.
        /// </exception>
        /// <typeparam name="T">
        /// The type of object to create.
        /// </typeparam>
        /// <returns>
        /// The disposable object that has been safely created.
        /// </returns>
        public static T SafeCreate<T>() where T : class, IDisposable, new()
        {
            T rv = null;
            try
            {
                rv = new T();
            }
            catch (Exception)
            {
                if (!ReferenceEquals(rv, null))
                {
                    rv.Dispose();
                }
                throw;
            }
            return rv;
        }

        /// <summary>
        /// Safely creates a disposable object with a custom constructor.
        /// </summary>
        /// <remarks>
        /// The motivation is to avoid <see href="http://msdn.microsoft.com/en-us/library/ms182289.aspx">CA2000</see> warnings.
        /// </remarks>
        /// <exception cref="Exception">
        /// Thrown when an exception error condition occurs.
        /// </exception>
        /// <typeparam name="T">
        /// The type of object to create.
        /// </typeparam>
        /// <param name="factory">
        /// The factory method used to construct the object.
        /// </param>
        /// <returns>
        /// The disposable object that has been safely created.
        /// </returns>
        public static T SafeCreate<T>(Func<T> factory) where T : class, IDisposable
        {
            factory.ArgumentNotNull("factory");

            T rv = null;
            try
            {
                rv = factory();
            }
            catch (Exception)
            {
                if (!ReferenceEquals(rv, null))
                {
                    rv.Dispose();
                }
                throw;
            }

            return rv;
        }
    }
}
