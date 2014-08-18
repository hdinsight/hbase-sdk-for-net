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
    /// A filter for adding inter-column timestamp matching Only cells with a correspondingly timestamped entry in the target column will be retained 
    /// Not compatible with Scan.setBatch as operations need full rows for correct filtering.
    /// </summary>
    public class DependentColumnFilter : CompareFilter
    {
        private readonly byte[] _family;
        private readonly byte[] _qualifier;

        /// <summary>
        /// Initializes a new instance of the <see cref="DependentColumnFilter" /> class.
        /// </summary>
        /// <param name="family">The name of the column family.</param>
        /// <param name="qualifier">The name of the column qualifier.</param>
        /// <param name="dropDependentColumn">Specifies whether or not the dependent column should be dropped.</param>
        /// <param name="valueCompareOp">The operator.</param>
        /// <param name="valueComparator">The value comparator.</param>
        public DependentColumnFilter(
            byte[] family,
            byte[] qualifier,
            bool dropDependentColumn,
            CompareOp valueCompareOp,
            ByteArrayComparable valueComparator)
        {
            family.ArgumentNotNull("family");
            qualifier.ArgumentNotNull("qualifier");

            if (!Enum.IsDefined(typeof(CompareOp), valueCompareOp))
            {
                throw new InvalidEnumArgumentException("valueCompareOp", (int)valueCompareOp, typeof(CompareOp));
            }

            valueComparator.ArgumentNotNull("valueComparator");

            _family = (byte[])family.Clone();
            _qualifier = (byte[])qualifier.Clone();
            DropDependentColumn = dropDependentColumn;
            CompareOperation = valueCompareOp;
            ValueComparator = valueComparator;
        }

        /// <summary>
        /// Gets the compare operation.
        /// </summary>
        /// <value>
        /// The compare operation.
        /// </value>
        public CompareOp CompareOperation { get; private set; }

        /// <summary>
        /// Gets a value indicating whether or not the dependent column should be dropped.
        /// </summary>
        /// <value>
        ///   <c>true</c> if the dependent column should be dropped; otherwise, <c>false</c>.
        /// </value>
        public bool DropDependentColumn { get; private set; }

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

        /// <summary>
        /// Gets the value comparator.
        /// </summary>
        /// <value>
        /// The value comparator.
        /// </value>
        public ByteArrayComparable ValueComparator { get; private set; }

        /// <inheritdoc/>
        [SuppressMessage("Microsoft.Globalization", "CA1308:NormalizeStringsToUppercase")]
        public override string ToEncodedString()
        {
            const string filterPattern =
                @"{{""type"":""DependentColumnFilter"",""op"":""{0}"",""family"":""{1}"",""qualifier"":""{2}"",""dropDependentColumn"":{3},""comparator"":{{{4}}}}}";

            return string.Format(
                CultureInfo.InvariantCulture,
                filterPattern,
                CompareOperation.ToCodeName(),
                Convert.ToBase64String(Family),
                Convert.ToBase64String(Qualifier),
                DropDependentColumn.ToString(CultureInfo.InvariantCulture).ToLowerInvariant(),
                ValueComparator.ToEncodedString());
        }
    }
}
