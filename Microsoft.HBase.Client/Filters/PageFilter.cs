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
    /// Implementation of Filter interface that limits results to a specific page size.
    /// </summary>
    /// <remarks>
    /// Note that this filter cannot guarantee that the number of results returned to a client are less than or equal to page size. This is because 
    /// the filter is applied separately on different region servers. It does however optimize the scan of individual HRegions by making sure that 
    /// the page size is never exceeded locally.
    /// </remarks>
    public class PageFilter : Filter
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PageFilter"/> class.
        /// </summary>
        /// <param name="pageSize">Size of the page.</param>
        public PageFilter(long pageSize)
        {
            PageSize = pageSize;
        }

        /// <summary>
        /// Gets the maximum size of a page.
        /// </summary>
        /// <value>
        /// The maximum size of a page.
        /// </value>
        public long PageSize { get; private set; }

        /// <inheritdoc/>
        public override string ToEncodedString()
        {
            const string filterPattern = @"{{""type"":""PageFilter"",""value"":""{0}""}}";
            return string.Format(CultureInfo.InvariantCulture, filterPattern, PageSize);
        }
    }
}
