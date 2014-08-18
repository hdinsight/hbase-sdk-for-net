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
    using System.Globalization;
    using Microsoft.HBase.Client.Internal;

    /// <summary>
    /// A filter that checks a single column value, but does not emit the tested column.
    /// </summary>
    /// <remarks>
    /// This will enable a performance boost over <see cref="SingleColumnValueFilter"/>, if the tested column value is not actually needed as input 
    /// (besides for the filtering itself).
    /// </remarks>
    public class SingleColumnValueExcludeFilter : SingleColumnValueFilter
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SingleColumnValueExcludeFilter" /> class.
        /// </summary>
        /// <param name="family">The name of the column family.</param>
        /// <param name="qualifier">The name of the column qualifier.</param>
        /// <param name="compareOp">The operator.</param>
        /// <param name="value">The value to compare column values against.</param>
        /// <param name="filterIfMissing">
        /// When <c>true</c>, the entire row will be skipped if the column is not found; 
        /// when <c>false</c>, the row will pass if the column is not found.
        /// </param>
        /// <param name="latestVersion">
        /// When <c>true</c>, the row will be returned if only the latest version of the column value matches;
        /// when <c>false</c>, the row will be returned if any version of the column value matches.
        /// </param>
        /// <remarks>
        /// Constructor for binary compare of the value of a single column.
        /// </remarks>
        public SingleColumnValueExcludeFilter(
            byte[] family,
            byte[] qualifier,
            CompareFilter.CompareOp compareOp,
            byte[] value,
            bool filterIfMissing = false,
            bool latestVersion = true) : base(family, qualifier, compareOp, value, filterIfMissing, latestVersion)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SingleColumnValueExcludeFilter"/> class.
        /// </summary>
        /// <param name="family">The name of the column family.</param>
        /// <param name="qualifier">The name of the column qualifier.</param>
        /// <param name="compareOp">The operator.</param>
        /// <param name="comparator">The comparator to use.</param>
        /// <param name="filterIfMissing">
        /// When <c>true</c>, the entire row will be skipped if the column is not found; 
        /// when <c>false</c>, the row will pass if the column is not found.
        /// </param>
        /// <param name="latestVersion">
        /// When <c>true</c>, the row will be returned if only the latest version of the column value matches;
        /// when <c>false</c>, the row will be returned if any version of the column value matches.
        /// </param>
        /// <remarks>
        /// Constructor for binary compare of the value of a single column.
        /// </remarks>
        public SingleColumnValueExcludeFilter(
            byte[] family,
            byte[] qualifier,
            CompareFilter.CompareOp compareOp,
            ByteArrayComparable comparator,
            bool filterIfMissing = false,
            bool latestVersion = true) : base(family, qualifier, compareOp, comparator, filterIfMissing, latestVersion)
        {
        }

        /// <inheritdoc/>
        [SuppressMessage("Microsoft.Globalization", "CA1308:NormalizeStringsToUppercase")]
        public override string ToEncodedString()
        {
            const string filterPattern =
                @"{{""type"":""SingleColumnValueExcludeFilter"",""op"":""{0}"",""family"":""{1}"",""qualifier"":""{2}"",""ifMissing"":{3},""comparator"":{{{4}}}}}";

            return string.Format(
                CultureInfo.InvariantCulture,
                filterPattern,
                CompareOperation.ToCodeName(),
                Convert.ToBase64String(Family),
                Convert.ToBase64String(Qualifier),
                FilterIfMissing.ToString(CultureInfo.InvariantCulture).ToLowerInvariant(),
                Comparator.ToEncodedString());
        }
    }
}
