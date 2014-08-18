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

namespace Microsoft.HBase.Client.Filters
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using Microsoft.HBase.Client.Internal;
    using org.apache.hadoop.hbase.rest.protobuf.generated;

    /// <summary>
    /// Base class for byte array comparators.
    /// </summary>
    public abstract class ByteArrayComparable
    {
        private readonly byte[] _value;

        /// <summary>
        /// Initializes a new instance of the <see cref="ByteArrayComparable"/> class.
        /// </summary>
        /// <param name="value">The value to compare.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="value"/> is null.</exception>
        protected ByteArrayComparable(byte[] value)
        {
            value.ArgumentNotNull("value");

            _value = (byte[])value.Clone();
        }

        /// <summary>
        /// Gets the value to compare.
        /// </summary>
        /// <value>
        /// The value to compare.
        /// </value>
        [SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays")]
        public byte[] Value
        {
            get { return (byte[])_value.Clone(); }
        }

        /// <summary>
        /// Generates an encoded string that can be used to as part of a <see cref="Scanner.filter"/>.
        /// </summary>
        /// <returns>
        /// A comparer string.
        /// </returns>
        public abstract string ToEncodedString();
    }
}
