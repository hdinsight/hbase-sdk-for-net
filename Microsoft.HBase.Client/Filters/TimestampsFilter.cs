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
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using Microsoft.HBase.Client.Internal;

    /// <summary>
    /// Filter that returns only cells whose timestamp (version) is in the specified list of timestamps (versions).
    /// </summary>
    /// <remarks>
    /// Use of this filter overrides any time range/time stamp options specified using Get.setTimeRange(long, long), Scan.setTimeRange(long, long), 
    /// Get.setTimeStamp(long), or Scan.setTimeStamp(long).
    /// </remarks>
    public class TimestampsFilter : Filter
    {
        private readonly HashSet<long> _timestamps;

        /// <summary>
        /// Initializes a new instance of the <see cref="TimestampsFilter"/> class.
        /// </summary>
        /// <param name="timestamps">The timestamps.</param>
        public TimestampsFilter(IEnumerable<long> timestamps)
        {
            timestamps.ArgumentNotNull("timestamps");

            _timestamps = new HashSet<long>(timestamps);
        }

        /// <summary>
        /// Gets the timestamps.
        /// </summary>
        /// <value>
        /// The timestamps.
        /// </value>
        public IEnumerable<long> Timestamps
        {
            get { return _timestamps.ToList(); }
        }

        /// <inheritdoc/>
        public override string ToEncodedString()
        {
            const string filterPattern = @"{{""type"":""TimestampsFilter"",""timestamps"":[{0}]}}";
            return string.Format(CultureInfo.InvariantCulture, filterPattern, ToCsvStringWithDoubleQuotedValues(_timestamps));
        }

        internal string ToCsvStringWithDoubleQuotedValues(IEnumerable<long> values)
        {
            values.ArgumentNotNull("values");

            var working = new StringBuilder();
            foreach (long v in values)
            {
                working.AppendFormat(@"""{0}"",", v);
            }

            // remove the trailing ','
            return working.ToString(0, working.Length - 1);
        }
    }
}
