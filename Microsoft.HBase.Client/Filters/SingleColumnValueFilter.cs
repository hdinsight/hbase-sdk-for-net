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
    using System.ComponentModel;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using Microsoft.HBase.Client.Internal;

    /// <summary>
    /// This filter is used to filter cells based on value.
    /// </summary>
    public class SingleColumnValueFilter : Filter
    {
        private readonly byte[] _family;
        private readonly byte[] _qualifier;

        /// <summary>
        /// Initializes a new instance of the <see cref="SingleColumnValueFilter" /> class.
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
        public SingleColumnValueFilter(
            byte[] family,
            byte[] qualifier,
            CompareFilter.CompareOp compareOp,
            byte[] value,
            bool filterIfMissing = false,
            bool latestVersion = true) : this(family, qualifier, compareOp, new BinaryComparator(value), filterIfMissing, latestVersion)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SingleColumnValueFilter"/> class.
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
        public SingleColumnValueFilter(
            byte[] family,
            byte[] qualifier,
            CompareFilter.CompareOp compareOp,
            ByteArrayComparable comparator,
            bool filterIfMissing = false,
            bool latestVersion = true)
        {
            family.ArgumentNotNull("family");
            qualifier.ArgumentNotNull("qualifier");

            if (!Enum.IsDefined(typeof(CompareFilter.CompareOp), compareOp))
            {
                throw new InvalidEnumArgumentException("compareOp", (int)compareOp, typeof(CompareFilter.CompareOp));
            }

            comparator.ArgumentNotNull("comparator");

            _family = (byte[])family.Clone();
            _qualifier = (byte[])qualifier.Clone();
            CompareOperation = compareOp;
            Comparator = comparator;

            FilterIfMissing = filterIfMissing;
            LatestVersion = latestVersion;
        }

        /// <summary>
        /// Gets the comparator.
        /// </summary>
        /// <value>
        /// The comparator.
        /// </value>
        public ByteArrayComparable Comparator { get; private set; }

        /// <summary>
        /// Gets the compare operation.
        /// </summary>
        /// <value>
        /// The compare operation.
        /// </value>
        public CompareFilter.CompareOp CompareOperation { get; private set; }

        /// <summary>
        /// Gets the name of the column family.
        /// </summary>
        /// <value>
        /// The name of the column family.
        /// </value>
        [SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays")]
        public byte[] Family
        {
            get { return (byte[])_family.Clone(); }
        }

        /// <summary>
        /// Gets a value indicating whether the entire row should be filtered if column is not found.
        /// </summary>
        /// <value>
        ///   <c>true</c> if the entire row should be filtered if column is not found; otherwise, <c>false</c>.
        /// </value>
        public bool FilterIfMissing { get; private set; }

        /// <summary>
        /// Gets a value indicating whether or not only the latest version will be tested.
        /// </summary>
        /// <value>
        /// When <c>true</c>, the row will be returned if only the latest version of the column value matches;
        /// when <c>false</c>, the row will be returned if any version of the column value matches.
        /// </value>
        public bool LatestVersion { get; private set; }

        /// <summary>
        /// Gets the name of the column qualifier.
        /// </summary>
        /// <value>
        /// The name of the column qualifier.
        /// </value>
        [SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays")]
        public byte[] Qualifier
        {
            get { return (byte[])_qualifier.Clone(); }
        }

        /// <inheritdoc/>
        [SuppressMessage("Microsoft.Globalization", "CA1308:NormalizeStringsToUppercase")]
        public override string ToEncodedString()
        {
            const string filterPattern =
                @"{{""type"":""SingleColumnValueFilter"",""op"":""{0}"",""family"":""{1}"",""qualifier"":""{2}"",""ifMissing"":{3},""latestVersion"":{4},""comparator"":{{{5}}}}}";

            return string.Format(
                CultureInfo.InvariantCulture,
                filterPattern,
                CompareOperation.ToCodeName(),
                Convert.ToBase64String(Family),
                Convert.ToBase64String(Qualifier),
                FilterIfMissing.ToString(CultureInfo.InvariantCulture).ToLowerInvariant(),
                LatestVersion.ToString(CultureInfo.InvariantCulture).ToLowerInvariant(),
                Comparator.ToEncodedString());
        }
    }
}
