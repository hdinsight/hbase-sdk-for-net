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

    /// <summary>
    /// A filter, based on the ColumnCountGetFilter, takes two arguments: limit and offset.
    /// </summary>
    /// <remarks>
    /// This filter can be used for row-based indexing, where references to other tables are stored across many columns, in order to efficient lookups
    /// and paginated results for end users. Only most recent versions are considered for pagination.
    /// </remarks>
    public class ColumnPaginationFilter : Filter
    {
        // could not get byte[] columnOffset to stringify, so it has been removed.

        /// <summary>
        /// Initializes a new instance of the <see cref="ColumnPaginationFilter"/> class.
        /// </summary>
        /// <param name="limit">The limit.</param>
        /// <param name="offset">The offset.</param>
        /// <remarks>
        /// Initializes filter with an integer offset and limit.
        /// </remarks>
        public ColumnPaginationFilter(int limit, int offset)
        {
            Limit = limit;
            Offset = offset;
        }

        /// <summary>
        /// Gets the limit.
        /// </summary>
        /// <value>
        /// The limit.
        /// </value>
        public int Limit { get; private set; }

        /// <summary>
        /// Gets the offset.
        /// </summary>
        /// <value>
        /// The offset.
        /// </value>
        public int Offset { get; private set; }

        /// <inheritdoc/>
        public override string ToEncodedString()
        {
            const string filterPattern = @"{{""type"":""ColumnPaginationFilter"",""limit"":{0},""offset"":{1}}}";

            return string.Format(CultureInfo.InvariantCulture, filterPattern, Limit, Offset);
        }
    }
}
