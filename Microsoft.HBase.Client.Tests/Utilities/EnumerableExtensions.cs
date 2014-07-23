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
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using Microsoft.HBase.Client.Tests.Utilities;

    internal static class EnumerableExtensions
    {
        ///<summary>Finds the index of the first item matching an expression in an enumerable.</summary>
        ///<param name="items">The enumerable to search.</param>
        ///<param name="predicate">The expression to test the items against.</param>
        ///<returns>The index of the first matching item, or -1 if no items match.</returns>
        internal static int FindIndex<T>(this IEnumerable<T> items, Func<T, bool> predicate)
        {
            if (items == null)
            {
                throw new ArgumentNullException("items");
            }
            if (predicate == null)
            {
                throw new ArgumentNullException("predicate");
            }

            int retVal = 0;
            foreach (T item in items)
            {
                if (predicate(item))
                {
                    return retVal;
                }
                retVal++;
            }
            return -1;
        }

        internal static bool IsEmpty(this IEnumerable thisValue)
        {
            return !(thisValue).IsNull() && !thisValue.Cast<object>().Any();
        }

        internal static bool IsNotEmpty(this IEnumerable thisValue)
        {
            return (thisValue).IsNull() || thisValue.Cast<object>().Any();
        }

        internal static bool IsNotNullOrEmpty(this IEnumerable thisValue)
        {
            return (thisValue).IsNotNull() && thisValue.Cast<object>().Any();
        }

        internal static bool IsNullOrEmpty(this IEnumerable thisValue)
        {
            return (thisValue).IsNull() || !thisValue.Cast<object>().Any();
        }

        internal static IEnumerable<T> Subset<T>(this IList<T> list, int start, int end)
        {
            int i = start;
            while (i <= end)
            {
                yield return list[i];
                checked
                {
                    ++i;
                }
            }
        }
    }
}
