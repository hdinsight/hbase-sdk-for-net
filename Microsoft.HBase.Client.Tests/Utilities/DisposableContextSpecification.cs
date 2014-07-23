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
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Reflection;
    using System.Threading;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// A <see cref="ContextSpecification"/> that implements <see cref="IDisposable"/>.
    /// </summary>
    [TestClass]
    public abstract class DisposableContextSpecification : ContextSpecification, IDisposable
    {
        private int _disposed;

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
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                Type derrivedType = GetType();
                FieldInfo[] fields = derrivedType.GetFields();
                foreach (FieldInfo field in fields)
                {
                    object value = field.GetValue(this);
                    if (value.IsNotNull())
                    {
                        Type valueType = value.GetType();
                        if (!valueType.IsValueType)
                        {
                            var asCollectionOfDisposables = value as IEnumerable<IDisposable>;
                            if (!ReferenceEquals(asCollectionOfDisposables, null))
                            {
                                foreach (IDisposable disposable in asCollectionOfDisposables)
                                {
                                    disposable.Dispose();
                                }
                            }

                            var asDisposable = value as IDisposable;
                            if (!ReferenceEquals(asDisposable, null))
                            {
                                asDisposable.Dispose();
                            }
                            field.SetValue(this, null);
                        }
                    }
                }
            }
        }
    }
}
