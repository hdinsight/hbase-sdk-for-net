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
    ///  A filter that includes rows based on a chance. 
    /// </summary>
    public class RandomRowFilter : Filter
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="RandomRowFilter"/> class.
        /// </summary>
        /// <param name="chance">The chance.</param>
        public RandomRowFilter(float chance)
        {
            Chance = chance;
        }

        /// <summary>
        /// Gets the chance.
        /// </summary>
        /// <value>
        /// The chance.
        /// </value>
        public float Chance { get; private set; }

        /// <inheritdoc/>
        public override string ToEncodedString()
        {
            const string filterPattern = @"{{""type"":""RandomRowFilter"",""chance"":{0}}}";
            return string.Format(CultureInfo.InvariantCulture, filterPattern, Chance);
        }
    }
}
