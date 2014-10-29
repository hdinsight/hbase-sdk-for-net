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
    using System.Globalization;
    using System.Text;
    using Microsoft.HBase.Client.Internal;

    /// <summary>
    ///  This filter is used for selecting only those keys with columns that matches a particular prefix. 
    /// </summary>
    public class MultipleColumnPrefixFilter : Filter
    {
        private readonly HashSet<byte[]> _prefixes;

        /// <summary>
        /// Initializes a new instance of the <see cref="MultipleColumnPrefixFilter"/> class.
        /// </summary>
        /// <param name="prefixes">The prefixes.</param>
        public MultipleColumnPrefixFilter(IEnumerable<byte[]> prefixes)
        {
            prefixes.ArgumentNotNull("prefixes");

            _prefixes = new HashSet<byte[]>(new ByteArrayEqualityComparer());
            foreach (byte[] p in prefixes)
            {
                if (ReferenceEquals(p, null))
                {
                    throw new ArgumentContainsNullException("prefixes", null, null);
                }
                _prefixes.Add((byte[])p.Clone());
            }
        }

        /// <summary>
        /// Gets the prefixes.
        /// </summary>
        /// <value>
        /// The prefixes.
        /// </value>
        public IEnumerable<byte[]> Prefixes
        {
            get
            {
                var rv = new List<byte[]>();
                foreach (byte[] p in _prefixes)
                {
                    rv.Add((byte[])p.Clone());
                }

                return rv;
            }
        }

        /// <inheritdoc/>
        public override string ToEncodedString()
        {
            const string filterPattern = @"{{""type"":""MultipleColumnPrefixFilter"",""prefixes"":[{0}]}}";
            return string.Format(CultureInfo.InvariantCulture, filterPattern, ToCsvStringWithDoubleQuotedEncodedValues(_prefixes));
        }

        internal string ToCsvStringWithDoubleQuotedEncodedValues(IEnumerable<byte[]> values)
        {
            values.ArgumentNotNull("values");

            var working = new StringBuilder();
            foreach (byte[] v in values)
            {
                working.AppendFormat(@"""{0}"",", Convert.ToBase64String(v));
            }

            // remove the trailing ','
            return working.ToString(0, working.Length - 1);
        }
    }
}
