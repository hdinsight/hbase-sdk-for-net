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

namespace Microsoft.HBase.Client
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Runtime.Serialization;
    using System.Security;

    /// <summary>
    /// The exception that is thrown when an empty argument is passed to a method that does not accept it as a valid argument.
    /// </summary>
    [Serializable]
    public sealed class ArgumentEmptyException : ArgumentException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ArgumentEmptyException"/> class.
        /// </summary>
        public ArgumentEmptyException() : this(null, null, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ArgumentEmptyException"/> class.
        /// </summary>
        /// <param name="message">The error message that explains the reason for this exception. </param>
        public ArgumentEmptyException(string message) : this(null, message, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ArgumentEmptyException"/> class.
        /// </summary>
        /// <param name="message">The error message that explains the reason for the exception.</param>
        /// <param name="innerException">
        /// The exception that is the cause of the current exception, or a null reference if no inner exception is specified. 
        /// </param>
        public ArgumentEmptyException(string message, Exception innerException) : this(null, message, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ArgumentEmptyException"/> class.
        /// </summary>
        /// <param name="paramName">The name of the parameter that caused the exception. </param>
        /// <param name="message">The error message that explains the reason for this exception. </param>
        /// <param name="innerException">The inner exception.</param>
        [SuppressMessage("Microsoft.Naming", "CA1704:IdentifiersShouldBeSpelledCorrectly", MessageId = "param")]
        public ArgumentEmptyException(string paramName, string message, Exception innerException)
            : base(message ?? "The value must not be empty.", paramName, innerException)
        {
        }

        [SecurityCritical]
        private ArgumentEmptyException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
