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
    using System.Collections;
    using System.Linq;

    internal static class ArgumentGuardExtensions
    {
        internal static void ArgumentNotNull([ValidatedNotNull] this object value, string argumentName)
        {
            if (ReferenceEquals(value, null))
            {
                throw new ArgumentNullException(argumentName ?? string.Empty);
            }
        }

        internal static void ArgumentNotNullNorEmpty([ValidatedNotNull] this IEnumerable value, string paramName)
        {
            value.ArgumentNotNull(paramName);
            if (!value.Cast<object>().Any())
            {
                throw new ArgumentEmptyException(paramName ?? string.Empty, null, null);
            }
        }
    }
}
