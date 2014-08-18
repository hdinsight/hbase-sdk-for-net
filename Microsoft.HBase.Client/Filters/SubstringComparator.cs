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
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.Text;

    /// <summary>
    /// A comparator to use with <see cref="SingleColumnValueFilter"/>, for filtering based on the value of a given column.
    /// </summary>
    /// <remarks>
    /// Use it to test if a given substring appears in a cell value in the column.  The comparison is case insensitive.
    /// Only EQUAL or NOT_EQUAL tests are valid with this comparator.
    /// </remarks>
    public class SubstringComparator : ByteArrayComparable
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SubstringComparator"/> class.
        /// </summary>
        /// <param name="substr">The substring.</param>
        [SuppressMessage("Microsoft.Naming", "CA1704:IdentifiersShouldBeSpelledCorrectly", MessageId = "substr")]
        public SubstringComparator(string substr) : base(substr == null ? null : Encoding.UTF8.GetBytes(substr))
        {
        }

        /// <inheritdoc/>
        public override string ToEncodedString()
        {
            const string pattern = @"""type"":""SubstringComparator"", ""value"":""{0}""";
            return string.Format(CultureInfo.InvariantCulture, pattern, Encoding.UTF8.GetString(Value));
        }
    }
}
