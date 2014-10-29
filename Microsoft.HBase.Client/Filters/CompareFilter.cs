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
    using Microsoft.HBase.Client.Internal;

    /// <summary>
    /// This is a generic filter to be used to filter by comparison.
    /// </summary>
    public abstract class CompareFilter : Filter
    {
        /// <summary>
        /// Represents comparison operators.
        /// </summary>
        public enum CompareOp
        {
            /// <summary>
            /// No operation.
            /// </summary>
            NoOperation = 0,

            /// <summary>
            /// Equals.
            /// </summary>
            Equal = 1,

            /// <summary>
            /// Not equal.
            /// </summary>
            NotEqual = 2,

            /// <summary>
            /// Greater than.
            /// </summary>
            GreaterThan = 3,

            /// <summary>
            /// Greater than or equal to.
            /// </summary>
            GreaterThanOrEqualTo = 4,

            /// <summary>
            /// Less than.
            /// </summary>
            LessThan = 5,

            /// <summary>
            /// Less than or equal to.
            /// </summary>
            LessThanOrEqualTo = 6,
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CompareFilter"/> class.
        /// </summary>
        /// <param name="compareOp">The compare op.</param>
        /// <param name="comparator">The comparator.</param>
        protected CompareFilter(CompareOp compareOp, ByteArrayComparable comparator)
        {
            if (!Enum.IsDefined(typeof(CompareOp), compareOp))
            {
                throw new InvalidEnumArgumentException("compareOp", (int)compareOp, typeof(CompareOp));
            }

            comparator.ArgumentNotNull("comparator");

            CompareOperation = compareOp;
            Comparator = comparator;
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
        public CompareOp CompareOperation { get; private set; }
    }
}
