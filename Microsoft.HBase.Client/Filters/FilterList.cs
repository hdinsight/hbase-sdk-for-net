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
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using Microsoft.HBase.Client.Internal;

    /// <summary>
    /// Implementation of <see cref="Filter"/> that represents an ordered List of Filters which will be evaluated with a specified boolean operator 
    /// FilterList.Operator.MUST_PASS_ALL (AND) or FilterList.Operator.MUST_PASS_ONE (OR).
    /// </summary>
    public class FilterList : Filter
    {
        private readonly List<Filter> _rowFilters;

        [SuppressMessage("Microsoft.Naming", "CA1716:IdentifiersShouldNotMatchKeywords", MessageId = "Operator")]
        public enum Operator
        {
            /// <summary>
            /// MUST_PASS_ALL
            /// </summary>
            /// <remarks>
            /// Represents "and".
            /// </remarks>
            MustPassAll = 0,

            /// <summary>
            /// MUST_PASS_ONE
            /// </summary>
            /// <remarks>
            /// Represents "or".
            /// </remarks>
            MustPassOne = 1,
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FilterList"/> class.
        /// </summary>
        /// <param name="rowFilters">The row filters.</param>
        public FilterList(params Filter[] rowFilters) : this(Operator.MustPassAll, rowFilters.ToList())
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FilterList"/> class.
        /// </summary>
        /// <param name="op">The op.</param>
        public FilterList(Operator op) : this(op, new List<Filter>())
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FilterList"/> class.
        /// </summary>
        /// <param name="op">The op.</param>
        /// <param name="rowFilters">The row filters.</param>
        public FilterList(Operator op, params Filter[] rowFilters) : this(op, rowFilters.ToList())
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FilterList"/> class.
        /// </summary>
        /// <param name="op">The op.</param>
        /// <param name="rowFilters">The row filters.</param>
        /// <exception cref="System.ComponentModel.InvalidEnumArgumentException">op</exception>
        public FilterList(Operator op, IEnumerable<Filter> rowFilters)
        {
            if (!Enum.IsDefined(typeof(Operator), op))
            {
                throw new InvalidEnumArgumentException("op", (int)op, typeof(Operator));
            }

            rowFilters.ArgumentNotNull("rowFilters");

            _rowFilters = new List<Filter>();
            foreach (Filter f in rowFilters)
            {
                if (ReferenceEquals(f, null))
                {
                    throw new ArgumentContainsNullException("rowFilters", null, null);
                }

                _rowFilters.Add(f);
            }

            Op = op;
        }

        /// <summary>
        /// Gets the filters.
        /// </summary>
        /// <value>
        /// The filters.
        /// </value>
        public IEnumerable<Filter> Filters
        {
            get { return _rowFilters; }
        }

        /// <summary>
        /// Gets the op.
        /// </summary>
        /// <value>
        /// The op.
        /// </value>
        public Operator Op { get; private set; }

        /// <summary>
        /// Adds the filter.
        /// </summary>
        /// <param name="filter">The filter.</param>
        public void AddFilter(Filter filter)
        {
            filter.ArgumentNotNull("filter");

            _rowFilters.Add(filter);
        }


        /// <inheritdoc/>
        public override string ToEncodedString()
        {
            const string filterPattern = @"{{""type"":""FilterList"",""op"":""{0}"",""filters"":[{1}]}}";
            return string.Format(CultureInfo.InvariantCulture, filterPattern, Op.ToCodeName(), FiltersToCsvString());
        }

        private string FiltersToCsvString()
        {
            if (_rowFilters.Count == 0)
            {
                return string.Empty;
            }

            var working = new StringBuilder();
            foreach (Filter f in _rowFilters)
            {
                working.AppendFormat(@"{0},", f.ToEncodedString());
            }

            // remove the trailing ','
            return working.ToString(0, working.Length - 1);
        }
    }
}
