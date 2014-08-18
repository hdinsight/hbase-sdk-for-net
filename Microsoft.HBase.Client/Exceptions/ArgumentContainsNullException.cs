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
    /// The exception that is thrown when a collection containing a null reference is passed to a method that does not accept collections
    /// that contain null as a valid argument.
    /// </summary>
    [Serializable]
    public sealed class ArgumentContainsNullException : ArgumentException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ArgumentContainsNullException"/> class.
        /// </summary>
        public ArgumentContainsNullException() : this(null, null, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ArgumentContainsNullException"/> class with the name of the parameter that caused this
        /// exception.
        /// </summary>
        /// <param name="message">
        /// The error message that explains the reason for this exception.
        /// </param>
        public ArgumentContainsNullException(string message) : this(null, message, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ArgumentContainsNullException"/> class with a specified error message and the exception
        /// that is the cause of this exception.
        /// </summary>
        /// <param name="message">
        /// The error message that explains the reason for this exception.
        /// </param>
        /// <param name="innerException">
        /// The exception that is the cause of the current exception, or a null reference if no inner exception is specified.
        /// </param>
        public ArgumentContainsNullException(string message, Exception innerException) : this(null, message, innerException)
        {
        }

        /// <summary>
        /// Initializes an instance of the <see cref="ArgumentContainsNullException"/> class with a specified error message and the name of the
        /// parameter that causes this exception.
        /// </summary>
        /// <param name="paramName">
        /// The name of the parameter that caused the exception.
        /// </param>
        /// <param name="message">
        /// The error message that explains the reason for this exception.
        /// </param>
        /// <param name="innerException">
        /// The exception that is the cause of the current exception, or a null reference if no inner exception is specified.
        /// </param>
        [SuppressMessage("Microsoft.Naming", "CA1704:IdentifiersShouldBeSpelledCorrectly", MessageId = "param")]
        public ArgumentContainsNullException(string paramName, string message, Exception innerException)
            : base(message ?? "The value must not contain null values.", paramName, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ArgumentContainsNullException"/> class with serialized data.
        /// </summary>
        /// <param name="info">The object that holds the serialized object data. </param>
        /// <param name="context">An object that describes the source or destination of the serialized data. </param>
        [SecurityCritical]
        private ArgumentContainsNullException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
