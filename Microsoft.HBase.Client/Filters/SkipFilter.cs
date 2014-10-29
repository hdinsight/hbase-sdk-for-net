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
    /// A wrapper filter that filters an entire row if any of the Cell checks do not pass.
    /// </summary>
    /// <remarks>
    /// A wrapper filter that filters an entire row if any of the Cell checks do not pass. 
    /// For example, if all columns in a row represent weights of different things, with the values being the actual weights, and we want to filter 
    /// out the entire row if any of its weights are zero. In this case, we want to prevent rows from being emitted if a single key is filtered. 
    /// Combine this filter with a <see cref="ValueFilter"/> .
    /// <code>
    /// var filter = new SkipFilter(new ValueFilter(CompareFilter.CompareOp.NotEqual, new BinaryComparator(BitConverter.GetBytes(0))));
    /// </code>
    /// </remarks>
    public class SkipFilter : Filter
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SkipFilter"/> class.
        /// </summary>
        /// <param name="filter">The filter.</param>
        public SkipFilter(Filter filter)
        {
            filter.ArgumentNotNull("filter");

            Filter = filter;
        }

        /// <summary>
        /// Gets the filter.
        /// </summary>
        /// <value>
        /// The filter.
        /// </value>
        public Filter Filter { get; private set; }

        /// <inheritdoc/>
        public override string ToEncodedString()
        {
            const string filterPattern = @"{{""type"":""SkipFilter"",""filters"":[{0}]}}";
            return string.Format(CultureInfo.InvariantCulture, filterPattern, Filter.ToEncodedString());
        }
    }
}
