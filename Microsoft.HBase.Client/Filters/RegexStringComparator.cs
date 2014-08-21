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
    /// This comparator is for use with CompareFilter implementations, such as RowFilter, QualifierFilter, and 
    /// ValueFilter, for filtering based on the value of a given column.
    /// </summary>
    public class RegexStringComparator : ByteArrayComparable
    {
        // note:  could not get "flags" to stringify, so it has been removed.

        /// <summary>
        /// Initializes a new instance of the <see cref="RegexStringComparator"/> class.
        /// </summary>
        /// <param name="expr">The regular expression as a string.</param>
        [SuppressMessage("Microsoft.Naming", "CA1704:IdentifiersShouldBeSpelledCorrectly", MessageId = "expr")]
        public RegexStringComparator(string expr) : base(expr == null ? null : Encoding.UTF8.GetBytes(expr))
        {
        }

        /// <inheritdoc/>
        public override string ToEncodedString()
        {
            const string pattern = @"""type"":""RegexStringComparator"", ""value"":""{0}""";
            return string.Format(CultureInfo.InvariantCulture, pattern, Encoding.UTF8.GetString(Value));
        }
    }
}
