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
    using System.Globalization;
    using Microsoft.HBase.Client.Internal;

    /// <summary>
    /// This filter is used to filter based on column value.
    /// </summary>
    public class ValueFilter : CompareFilter
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ValueFilter"/> class.
        /// </summary>
        /// <param name="valueCompareOp">The value compare op.</param>
        /// <param name="valueComparator">The value comparator.</param>
        public ValueFilter(CompareOp valueCompareOp, ByteArrayComparable valueComparator) : base(valueCompareOp, valueComparator)
        {
        }

        /// <inheritdoc/>
        public override string ToEncodedString()
        {
            const string filterPattern = @"{{""type"":""ValueFilter"",""op"":""{0}"",""comparator"":{{{1}}}}}";
            return string.Format(CultureInfo.InvariantCulture, filterPattern, CompareOperation.ToCodeName(), Comparator.ToEncodedString());
        }
    }
}
