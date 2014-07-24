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

namespace Microsoft.HBase.Client.Tests.Utilities
{
    /// <summary>
    /// Represents the general types of comparisons the 
    /// system can perform.
    /// </summary>
    internal enum GeneralComparisonOperator
    {
        /// <summary>
        /// Represents a Reference Equality comparison.
        /// </summary>
        ReferenceEqual,

        /// <summary>
        /// Represents an Equal comparison.
        /// </summary>
        Equal,

        /// <summary>
        /// Represents a Not Equal comparison.
        /// </summary>
        NotEqual,

        /// <summary>
        /// Represents a Less Than comparison.
        /// </summary>
        LessThan,

        /// <summary>
        /// Represents a Less Than Or Equal comparison.
        /// </summary>
        LessThanOrEqual,

        /// <summary>
        /// Represents a Greater Than comparison.
        /// </summary>
        GreaterThan,

        /// <summary>
        /// Represents a Greater Than Or Equal comparison.
        /// </summary>
        GreaterThanOrEqual
    }
}
