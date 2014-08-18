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
    /// This filter is used for selecting only those keys with columns that are between minColumn to maxColumn.
    /// </summary>
    public class ColumnRangeFilter : Filter
    {
        private readonly byte[] _maxColumn;
        private readonly byte[] _minColumn;

        /// <summary>
        /// Initializes a new instance of the <see cref="ColumnRangeFilter"/> class.
        /// </summary>
        /// <param name="minColumn">The minimum column.</param>
        /// <param name="minColumnInclusive">if set to <c>true</c> include the minimum column in the range.</param>
        /// <param name="maxColumn">The maximum column.</param>
        /// <param name="maxColumnInclusive">if set to <c>true</c> include the maximum column in the range.</param>
        public ColumnRangeFilter(byte[] minColumn, bool minColumnInclusive, byte[] maxColumn, bool maxColumnInclusive)
        {
            minColumn.ArgumentNotNull("minColumn");
            maxColumn.ArgumentNotNull("maxColumn");

            _minColumn = (byte[])minColumn.Clone();
            MinColumnInclusive = minColumnInclusive;
            _maxColumn = (byte[])maxColumn.Clone();
            MaxColumnInclusive = maxColumnInclusive;
        }

        /// <summary>
        /// Gets the maximum column.
        /// </summary>
        /// <value>
        /// The maximum column.
        /// </value>
        [SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays")]
        public byte[] MaxColumn
        {
            get { return (byte[])_maxColumn.Clone(); }
        }

        /// <summary>
        /// Gets a value indicating whether or not the maximum column is in the range.
        /// </summary>
        /// <value>
        /// <c>true</c> if maximum column is in the range; otherwise, <c>false</c>.
        /// </value>
        public bool MaxColumnInclusive { get; private set; }

        /// <summary>
        /// Gets the minimum column.
        /// </summary>
        /// <value>
        /// The minimum column.
        /// </value>
        [SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays")]
        public byte[] MinColumn
        {
            get { return (byte[])_minColumn.Clone(); }
        }

        /// <summary>
        /// Gets a value indicating whether or not the minimum column is in the range.
        /// </summary>
        /// <value>
        /// <c>true</c> if the minimum column is in the range; otherwise, <c>false</c>.
        /// </value>
        public bool MinColumnInclusive { get; private set; }

        /// <inheritdoc/>
        [SuppressMessage("Microsoft.Globalization", "CA1308:NormalizeStringsToUppercase")]
        public override string ToEncodedString()
        {
            const string filterPattern =
                @"{{""type"":""ColumnRangeFilter"",""minColumn"":""{0}"",""minColumnInclusive"":{1},""maxColumn"":""{2}"",""maxColumnInclusive"":{3}}}";

            return string.Format(
                CultureInfo.InvariantCulture,
                filterPattern,
                Convert.ToBase64String(MinColumn),
                MinColumnInclusive.ToString(CultureInfo.InvariantCulture).ToLowerInvariant(),
                Convert.ToBase64String(MaxColumn),
                MaxColumnInclusive.ToString(CultureInfo.InvariantCulture).ToLowerInvariant());
        }
    }
}
