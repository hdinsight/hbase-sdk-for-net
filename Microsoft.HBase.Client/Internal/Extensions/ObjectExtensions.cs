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

namespace Microsoft.HBase.Client.Internal
{
    using System.ComponentModel;
    using Microsoft.HBase.Client.Filters;

    /// <summary>
    /// Provides extensions methods to the Object class.
    /// </summary>
    internal static class ObjectExtensions
    {
        /// <summary>
        /// Evaluates type compatibility.
        /// </summary>
        /// <typeparam name = "T">
        /// The type to evaluate against.
        /// </typeparam>
        /// <param name = "inputValue">
        /// The object to evaluate compatibility for.
        /// </param>
        /// <returns>
        /// True if the object is compatible otherwise false.
        /// </returns>
        internal static bool Is<T>(this object inputValue)
        {
            return inputValue is T;
        }

        /// <summary>
        /// Determines whether the specified object is not null.
        /// </summary>
        /// <param name = "inputValue">The object.</param>
        /// <returns>
        /// <c>true</c> if the specified object is not null; otherwise, <c>false</c>.
        /// </returns>
        internal static bool IsNotNull([ValidatedNotNull] this object inputValue)
        {
            return !ReferenceEquals(inputValue, null);
        }

        /// <summary>
        /// Determines whether the specified object is null.
        /// </summary>
        /// <param name = "inputValue">The object.</param>
        /// <returns>
        /// <c>true</c> if the specified object is null; otherwise, <c>false</c>.
        /// </returns>
        internal static bool IsNull([ValidatedNotNull] this object inputValue)
        {
            return ReferenceEquals(inputValue, null);
        }

        internal static string ToCodeName(this BitComparator.BitwiseOp value)
        {
            switch (value)
            {
                case BitComparator.BitwiseOp.And:
                    return "AND";

                case BitComparator.BitwiseOp.Or:
                    return "OR";

                case BitComparator.BitwiseOp.Xor:
                    return "XOR";

                default:
                    throw new InvalidEnumArgumentException("value", (int)value, typeof(BitComparator.BitwiseOp));
            }
        }

        internal static string ToCodeName(this CompareFilter.CompareOp value)
        {
            switch (value)
            {
                case CompareFilter.CompareOp.NoOperation:
                    return "NO_OP";

                case CompareFilter.CompareOp.Equal:
                    return "EQUAL";

                case CompareFilter.CompareOp.NotEqual:
                    return "NOT_EQUAL";

                case CompareFilter.CompareOp.GreaterThan:
                    return "GREATER";

                case CompareFilter.CompareOp.GreaterThanOrEqualTo:
                    return "GREATER_OR_EQUAL";

                case CompareFilter.CompareOp.LessThan:
                    return "LESS";

                case CompareFilter.CompareOp.LessThanOrEqualTo:
                    return "LESS_OR_EQUAL";

                default:
                    throw new InvalidEnumArgumentException("value", (int)value, typeof(CompareFilter.CompareOp));
            }
        }
    }
}
