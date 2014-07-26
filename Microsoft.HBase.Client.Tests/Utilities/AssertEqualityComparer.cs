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

    /// <summary>
    /// Equality comparer used in assertions.
    /// </summary>
    /// <typeparam name = "T">
    /// The type of objects to compare.
    /// </typeparam>
    /// <remarks>
    /// Originally borrowed from XUnit, which is licensed under MS-PL.
    /// </remarks>
    internal sealed class AssertEqualityComparer<T> : IEqualityComparer<T>
    {
        // ReSharper disable StaticFieldInGenericType
        private static readonly AssertEqualityComparer<object> InnerComparer = new AssertEqualityComparer<object>();
        // ReSharper restore StaticFieldInGenericType

        /// <summary>
        /// Determines whether the specified objects are equal.
        /// </summary>
        /// <returns>
        /// <c>true</c> if the specified objects are equal; otherwise, <c>false</c>.
        /// </returns>
        /// <param name = "x">The first object of type <typeref name = "T" /> to compare.
        /// </param>
        /// <param name = "y">The second object of type <typeref name = "T" /> to compare.
        /// </param>
        public bool Equals(T x, T y)
        {
            Type type = typeof(T);

            // Null?
            if (!type.IsValueType || (type.IsGenericType && type.GetGenericTypeDefinition().IsAssignableFrom(typeof(Nullable<>))))
            {
                if (object.Equals(x, default(T)))
                {
                    return object.Equals(y, default(T));
                }

                if (object.Equals(y, default(T)))
                {
                    return false;
                }
            }

            // Implements IEquatable<T>?
            var equatable = x as IEquatable<T>;
            if (equatable != null)
            {
                return equatable.Equals(y);
            }

            // Implements IComparable<T>?
            var comparable1 = x as IComparable<T>;
            if (comparable1 != null)
            {
                return comparable1.CompareTo(y) == 0;
            }

            // Implements IComparable?
            var comparable2 = x as IComparable;
            if (comparable2 != null)
            {
                return comparable2.CompareTo(y) == 0;
            }

            // Enumerable?
            var enumerableX = x as IEnumerable;
            var enumerableY = y as IEnumerable;

            if (enumerableX != null && enumerableY != null)
            {
                IEnumerator enumeratorX = enumerableX.GetEnumerator();
                IEnumerator enumeratorY = enumerableY.GetEnumerator();

                while (true)
                {
                    bool hasNextX = enumeratorX.MoveNext();
                    bool hasNextY = enumeratorY.MoveNext();

                    if (!hasNextX || !hasNextY)
                    {
                        return hasNextX == hasNextY;
                    }

                    if (!InnerComparer.Equals(enumeratorX.Current, enumeratorY.Current))
                    {
                        return false;
                    }
                }
            }

            // Last case, rely on Object.Equals
            return object.Equals(x, y);
        }

        /// <summary>
        /// Returns a hash code for the specified object.
        /// </summary>
        /// <returns>
        /// A hash code for the specified object.
        /// </returns>
        /// <param name = "obj">
        /// The <see cref = "T:System.Object" /> for which a hash code is to be returned.
        /// </param>
        public int GetHashCode(T obj)
        {
            return ReferenceEquals(obj, null) ? 0 : obj.GetHashCode();
        }
    }
}
