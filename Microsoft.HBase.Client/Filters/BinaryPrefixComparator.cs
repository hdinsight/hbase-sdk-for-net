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
    using System.Globalization;

    /// <summary>
    /// A comparator which compares against a specified byte array, but only compares up to the length of this byte array.
    /// </summary>
    public class BinaryPrefixComparator : ByteArrayComparable
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryPrefixComparator"/> class.
        /// </summary>
        /// <param name="value">The value.</param>
        public BinaryPrefixComparator(byte[] value) : base(value)
        {
        }
        
        /// <inheritdoc/>
        public override string ToEncodedString()
        {
            const string pattern = @"""type"":""BinaryPrefixComparator"", ""value"":""{0}""";
            return string.Format(CultureInfo.InvariantCulture, pattern, Convert.ToBase64String(Value));
        }
    }
}
