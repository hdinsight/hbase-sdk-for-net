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
    }
}
