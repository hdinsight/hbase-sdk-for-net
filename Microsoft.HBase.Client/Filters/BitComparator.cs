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
    using System.Globalization;
    using Microsoft.HBase.Client.Internal;

    /// <summary>
    ///  Comparator which performs the specified bitwise operation on each of the bytes with the specified byte array.
    /// </summary>
    public class BitComparator : ByteArrayComparable
    {
        /// <summary>
        /// Represents bitwise operations.
        /// </summary>
        public enum BitwiseOp
        {
            /// <summary>
            /// And.
            /// </summary>
            And = 0,

            /// <summary>
            /// Or.
            /// </summary>
            Or = 1,

            /// <summary>
            /// Exclusive or.
            /// </summary>
            Xor = 2,
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BitComparator"/> class.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="bitOperator">The bit operator.</param>
        /// <exception cref="InvalidEnumArgumentException">The value of <paramref name="bitOperator"/> is not recognized.</exception>
        public BitComparator(byte[] value, BitwiseOp bitOperator) : base(value)
        {
            if (!Enum.IsDefined(typeof(BitwiseOp), bitOperator))
            {
                throw new InvalidEnumArgumentException("bitOperator", (int)bitOperator, typeof(BitwiseOp));
            }

            BitOperator = bitOperator;
        }

        /// <summary>
        /// Gets the bit operator.
        /// </summary>
        /// <value>
        /// The bit operator.
        /// </value>
        public BitwiseOp BitOperator { get; private set; }

        /// <inheritdoc/>
        public override string ToEncodedString()
        {
            const string pattern = @"""type"":""BitComparator"", ""value"":""{0}"", ""op"":""{1}""";
            return string.Format(CultureInfo.InvariantCulture, pattern, Convert.ToBase64String(Value), BitOperator.ToCodeName());
        }
    }
}
