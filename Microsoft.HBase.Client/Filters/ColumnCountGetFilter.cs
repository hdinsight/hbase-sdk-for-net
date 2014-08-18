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

    /// <summary>
    /// Simple filter that returns first N columns on row only.
    /// </summary>
    /// <remarks>
    /// Simple filter that returns first N columns on row only. This filter was written to test filters in Get and as soon as it gets its quota 
    /// of columns, filterAllRemaining() returns true. This makes this filter unsuitable as a Scan filter.
    /// </remarks>
    public class ColumnCountGetFilter : Filter
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ColumnCountGetFilter"/> class.
        /// </summary>
        /// <param name="n">The n.</param>
        [SuppressMessage("Microsoft.Naming", "CA1704:IdentifiersShouldBeSpelledCorrectly", MessageId = "n")]
        public ColumnCountGetFilter(int n)
        {
            N = n;
        }

        /// <summary>
        /// Gets the n.
        /// </summary>
        /// <value>
        /// The n.
        /// </value>
        [SuppressMessage("Microsoft.Naming", "CA1704:IdentifiersShouldBeSpelledCorrectly", MessageId = "N")]
        public int N { get; private set; }

        /// <inheritdoc/>
        public override string ToEncodedString()
        {
            const string filterPattern = @"{{""type"":""ColumnCountGetFilter"",""limit"":{0}}}";

            return string.Format(CultureInfo.InvariantCulture, filterPattern, N);
        }
    }
}
