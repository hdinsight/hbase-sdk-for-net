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
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    // ReSharper disable PossibleMultipleEnumeration
    // ReSharper disable CompareOfFloatsByEqualityOperator

    /// <summary>
    /// Assertion extension methods.
    /// </summary>
    /// <remarks>
    /// Originally borrowed from Machine.Specifications, licensed under MS-PL.
    /// </remarks>
    internal static class ShouldExtensionMethods
    {
        /// <summary>
        /// Casts the specified obj.
        /// </summary>
        /// <typeparam name="T"> The target type of the cast. </typeparam>
        /// <param name="inputValue"> The object. </param>
        /// <returns> The result of the cast. </returns>
        internal static T CastTo<T>(this object inputValue)
        {
            return (T)inputValue;
        }

        /// <summary>
        /// Attempts to convert two objects to compatible types.
        /// </summary>
        /// <param name = "left">
        /// The left object.
        /// </param>
        /// <param name = "right">
        /// The right object.
        /// </param>
        /// <returns>
        /// A pair representing the converted objects (or null if no conversion could 
        /// be made).
        /// </returns>
        internal static Pair<object, object> EquateObjects(object left, object right)
        {
            if (left == null)
            {
                throw new ArgumentNullException("left");
            }

            if (right == null)
            {
                throw new ArgumentNullException("right");
            }


            Type leftType = left.GetType();
            Type rightType = right.GetType();

            // If both types are the same, nothing to do, just return the
            // pair of objects as they were presented.
            if (leftType == rightType)
            {
                return new Pair<object, object>(left, right);
            }

            // If both types are numeric, use special handling rules for
            // equating numeric types.
            if (leftType.IsNumeric() && rightType.IsNumeric())
            {
                return EquateNumericObjects(left, right);
            }

            // if left is numeric and right is a string, try to 
            // parse the string as a numeric.
            if (leftType.IsNumeric() && rightType == typeof(string))
            {
                string rightString = right.ToString();
                if (rightString.Contains('.'))
                {
                    return EquateNumericObjects(left, rightString.ToDoubleIc());
                }

                return EquateNumericObjects(left, rightString.ToInt64Ic());
            }

            // if the right is numeric and the left is a string, try to
            // parse the left as a string.
            if (leftType == typeof(string) && rightType.IsNumeric())
            {
                string leftString = left.ToString();
                if (leftString.Contains('.'))
                {
                    return EquateNumericObjects(leftString.ToDoubleIc(), right);
                }

                return EquateNumericObjects(leftString.ToInt64Ic(), right);
            }

            // The only "simi" primitives left are String, Char, DateTime, TimeSpan, & Guid

            // Otherwise we can not equate the types, simply return null.
            return null;
        }

        /// <summary>
        /// Performs a general comparison of the left value to the right value.
        /// </summary>
        /// <param name = "left">
        /// The left value for the comparison.
        /// </param>
        /// <param name = "right">
        /// The right value for the comparison.
        /// </param>
        /// <param name = "comparisonOperator">
        /// The type of comparison to perform.
        /// </param>
        /// <returns>
        /// True if the comparison is a match otherwise false.
        /// </returns>
        internal static bool GeneralCompare<T>(T left, T right, GeneralComparisonOperator comparisonOperator)
        {
            // First handle Reference equality because it requires no special rules.
            if (comparisonOperator == GeneralComparisonOperator.ReferenceEqual)
            {
                return ReferenceEquals(left, right);
            }

            // First let the "conversion" system try to get us compatible types.
            Pair<object, object> equated = EquateObjects(left, right);
            Type leftType = left.GetType();
            Type rightType = right.GetType();

            // If we could not find compatible types, then we can not do the comparison.
            if (equated.IsNull())
            {
                throw new InvalidOperationException("Unable to equate the types ({0}) and ({1})".FormatIc(leftType.FullName, rightType.FullName));
            }

            object leftEquated = equated.First;
            object rightEquated = equated.Second;

            // Numeric comparisons are "special cased" let the numeric comparison functionality handle it.
            if (leftType.IsNumeric() && rightType.IsNumeric())
            {
                return NumericCompare(leftEquated, rightEquated, comparisonOperator);
            }

            if (leftType == typeof(DateTime) && rightType == typeof(DateTime))
            {
                return DateTimeCompare(leftEquated.CastTo<DateTime>(), rightEquated.CastTo<DateTime>(), comparisonOperator);
            }

            if (leftType == typeof(DateTime) && rightType == typeof(DateTime))
            {
                return DateTimeOffsetCompare(leftEquated.CastTo<DateTimeOffset>(), rightEquated.CastTo<DateTimeOffset>(), comparisonOperator);
            }

            if (leftType == typeof(DateTime) && rightType == typeof(DateTime))
            {
                return TimespanCompare(leftEquated.CastTo<TimeSpan>(), rightEquated.CastTo<TimeSpan>(), comparisonOperator);
            }

            // Strings are easy, lets knock them out
            if (leftType == typeof(string) && rightType == typeof(string))
            {
                return StringCompare(left.ToString(), right.ToString(), comparisonOperator);
            }

            // The rest can only be compared for equality, so use existing SafeCompare
            if (comparisonOperator == GeneralComparisonOperator.Equal || comparisonOperator == GeneralComparisonOperator.NotEqual)
            {
                return comparisonOperator == GeneralComparisonOperator.Equal ? SafeEquals(left, right) : !SafeEquals(left, right);
            }

            throw new InvalidOperationException(
                "Unable to compare: ({0}) {2} ({1})".FormatIc(leftType.FullName, rightType.FullName, comparisonOperator.ToString()));
        }

        // REFACTOR: I'm still working on everything below this line.  (MWP believes this REFACTOR NOTE was left by TISTOCKS).

        /// <summary>
        /// Asserts that the given value should be approximately equal to the expected value.
        /// </summary>
        /// <param name = "actual">
        /// The given value.
        /// </param>
        /// <param name = "expected">
        /// The expected value.
        /// </param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static float ShouldBeCloseTo(this float actual, float expected)
        {
            return actual.ShouldBeCloseTo(expected, 0.0000001f);
        }

        /// <summary>
        /// Asserts that the given value should be approximately equal to the expected value.
        /// </summary>
        /// <param name = "actual">
        /// The given value.
        /// </param>
        /// <param name = "expected">
        /// The expected value.
        /// </param>
        /// <param name = "tolerance">
        /// The deviation tolerance.
        /// </param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static float ShouldBeCloseTo(this float actual, float expected, float tolerance)
        {
            if (Math.Abs(actual - expected) > tolerance)
            {
                throw NewException("Should be within {0} of {1} but is {2}", tolerance, expected, actual);
            }

            return actual;
        }

        /// <summary>
        /// Asserts that the given value should be approximately equal to the expected value.
        /// </summary>
        /// <param name = "actual">
        /// The given value.
        /// </param>
        /// <param name = "expected">
        /// The expected value.
        /// </param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static double ShouldBeCloseTo(this double actual, double expected)
        {
            return actual.ShouldBeCloseTo(expected, 0.0000001d);
        }

        /// <summary>
        /// Asserts that the given value should be approximately equal to the expected value.
        /// </summary>
        /// <param name = "actual">
        /// The given value.
        /// </param>
        /// <param name = "expected">
        /// The expected value.
        /// </param>
        /// <param name = "tolerance">
        /// The deviation tolerance.
        /// </param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static double ShouldBeCloseTo(this double actual, double expected, double tolerance)
        {
            if (Math.Abs(actual - expected) > tolerance)
            {
                throw NewException("Should be within {0} of {1} but is {2}", tolerance, expected, actual);
            }

            return actual;
        }

        /// <summary>
        /// Asserts that the given value should be approximately equal to the expected value.
        /// </summary>
        /// <param name = "actual">
        /// The given value.
        /// </param>
        /// <param name = "expected">
        /// The expected value.
        /// </param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static DateTime ShouldBeCloseTo(this DateTime actual, DateTime expected)
        {
            return ShouldBeCloseTo(actual, expected, new TimeSpan(0, 5, 10));
        }

        /// <summary>
        /// Asserts that the given value should be approximately equal to the expected value.
        /// </summary>
        /// <param name = "actual">
        /// The given value.
        /// </param>
        /// <param name = "expected">
        /// The expected value.
        /// </param>
        /// <param name = "tolerance">
        /// The deviation tolerance.
        /// </param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static DateTime ShouldBeCloseTo(this DateTime actual, DateTime expected, TimeSpan tolerance)
        {
            if (actual.Kind == DateTimeKind.Unspecified && expected.Kind != DateTimeKind.Unspecified)
            {
                // NEIN: throwing ArgumentException, create a specific exception.
                throw new ArgumentException("DateTimeKind.Unspecified is only supported when both comparands are unspecified.", "actual");
            }

            if (actual.Kind != DateTimeKind.Unspecified && expected.Kind == DateTimeKind.Unspecified)
            {
                // NEIN: throwing ArgumentException, create a specific exception.
                throw new ArgumentException("DateTimeKind.Unspecified is only supported when both comparands are unspecified.", "expected");
            }

            if (Math.Abs(actual.ToUniversalTime().Ticks - expected.ToUniversalTime().Ticks) > tolerance.Ticks)
            {
                throw NewException("Should be within {0} of {1} but is {2}", tolerance, expected, actual);
            }

            return actual;
        }

        /// <summary>
        /// Asserts that the given collection should contain zero elements.
        /// </summary>
        /// <param name = "actual">
        /// The given value.
        /// </param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static IEnumerable ShouldBeEmpty(this IEnumerable actual)
        {
            if (!actual.IsEmpty())
            {
                throw NewException("Should be empty but contains:\n" + actual.Cast<object>().EachToUsefulString());
            }

            return actual;
        }

        /// <summary>
        /// Asserts the given string should be equal (case- and culture-insensitive) to the expected string.
        /// </summary>
        /// <param name = "actual">The given string.</param>
        /// <param name = "expected">The expected string.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static string ShouldBeEqualOrdinalIgnoreCase(this string actual, string expected)
        {
            if (expected == null)
            {
                throw new ArgumentNullException("expected");
            }

            if (actual == null)
            {
                throw NewException("Should be equal ignoring case to {0} but is [null]", expected);
            }

            if (!actual.Equals(expected, StringComparison.OrdinalIgnoreCase))
            {
                throw NewException("Should be equal ignoring case to {0} but is {1}", expected, actual);
            }

            return actual;
        }

        /// <summary>
        /// Asserts the condition should be <c>false</c>.
        /// </summary>
        /// <param name = "actual">The condition.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static bool ShouldBeFalse(this bool actual)
        {
            if (actual)
            {
                throw NewException("Should be [false] but is [true]");
            }

            // ReSharper disable ConditionIsAlwaysTrueOrFalse
            return actual;

            // ReSharper restore ConditionIsAlwaysTrueOrFalse
        }

        /// <summary>
        /// Asserts the given value is greater than the comparable value.
        /// </summary>
        /// <param name = "actual">The given value.</param>
        /// <param name = "expected">The comparable value.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static object ShouldBeGreaterThan(this object actual, object expected)
        {
            if (actual == null)
            {
                throw NewException("Should be greater than {0} but is [null]", expected);
            }

            if (!GeneralCompare(actual, expected, GeneralComparisonOperator.GreaterThan))
            {
                throw NewException("Should be greater than {0} but is {1}", expected, actual);
            }

            return actual;
        }

        /// <summary>
        /// Asserts the given value is greater than or equal to the comparable value.
        /// </summary>
        /// <param name = "actual">The given value.</param>
        /// <param name = "expected">The comparable value.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static object ShouldBeGreaterThanOrEqualTo(this object actual, object expected)
        {
            if (actual == null && expected != null)
            {
                throw NewException("Should be greater than or equal to {0} but is [null]", expected);
            }

            if (!GeneralCompare(actual, expected, GeneralComparisonOperator.GreaterThanOrEqual))
            {
                throw NewException("Should be greater than or equal to {0} but is {1}", expected, actual);
            }

            return actual;
        }

        /// <summary>
        /// Asserts the given value is less than the comparable value.
        /// </summary>
        /// <param name = "actual">The given value.</param>
        /// <param name = "expected">The comparable value.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static object ShouldBeLessThan(this object actual, object expected)
        {
            if (actual == null)
            {
                throw NewException("Should be less than {0} but is [null]", expected);
            }

            if (!GeneralCompare(actual, expected, GeneralComparisonOperator.LessThan))
            {
                throw NewException("Should be less than {0} but is {1}", expected, actual);
            }

            return actual;
        }

        /// <summary>
        /// Asserts the given value is less than or equal to the comparable value.
        /// </summary>
        /// <param name = "actual">The given value.</param>
        /// <param name = "expected">The comparable value.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static object ShouldBeLessThanOrEqualTo(this object actual, object expected)
        {
            if (actual == null && expected != null)
            {
                throw NewException("Should be less than or equal to {0} but is [null]", expected);
            }

            if (!GeneralCompare(actual, expected, GeneralComparisonOperator.LessThanOrEqual))
            {
                throw NewException("Should be less than or equal to {0} but is {1}", expected, actual);
            }

            return actual;
        }

        /// <summary>
        /// Asserts the given value is null.
        /// </summary>
        /// <param name = "actual">
        /// The given value.
        /// </param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static object ShouldBeNull(this object actual)
        {
            if (actual != null)
            {
                throw NewException("Should be [null] but is {0}", actual);
            }

            return null;
        }

        /// <summary>
        /// Asserts that the given string should be null or empty.
        /// </summary>
        /// <param name = "actual">
        /// The given string.
        /// </param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static IEnumerable ShouldBeNullOrEmpty(this IEnumerable actual)
        {
            if (actual.IsNotNullOrEmpty())
            {
                throw NewException("Should be null or empty but is {0}", actual);
            }

            return actual;
        }

        /// <summary>
        /// Asserts the given value is of the specified type.
        /// </summary>
        /// <param name = "actual">
        /// The given value.
        /// </param>
        /// <param name = "expected">
        /// The expected type.
        /// </param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static object ShouldBeOfType(this object actual, Type expected)
        {
            if (expected == null)
            {
                throw new ArgumentNullException("expected");
            }

            if (actual == null)
            {
                throw NewException("Should be of type {0} but is [null]", expected);
            }

            if (!expected.IsInstanceOfType(actual))
            {
                throw NewException("Should be of type {0} but is of type {1}", expected, actual.GetType());
            }

            return actual;
        }

        /// <summary>
        /// Asserts the given value is of the specified type.
        /// </summary>
        /// <param name = "actual">
        /// The given value.
        /// </param>
        /// <typeparam name = "T">The expected type.</typeparam>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static object ShouldBeOfType<T>(this object actual)
        {
            actual.ShouldBeOfType(typeof(T));
            return actual;
        }

        /// <summary>
        /// Asserts the given collection should be a proper subset of the provided superset collection.
        /// </summary>
        /// <param name = "actual">The given collection.</param>
        /// <param name = "superset">The collection that should contain the given collection.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static IEnumerable ShouldBeProperSubsetOf(this IEnumerable actual, IEnumerable superset)
        {
            actual.ShouldBeSubsetOf(superset);
            IEnumerable<object> actualList = ConvertEnumerableToList(actual);
            IEnumerable<object> supersetList = ConvertEnumerableToList(superset);

            actualList.Count().ShouldNotEqual(supersetList.Count());

            return actual;
        }

        /// <summary>
        /// Asserts the given collection should be a proper subset of the provided superset collection.
        /// </summary>
        /// <param name = "actual">The given collection.</param>
        /// <param name = "subset">The collection that should contain the given collection.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static IEnumerable ShouldBeProperSupersetOf(this IEnumerable actual, IEnumerable subset)
        {
            actual.ShouldBeSupersetOf(subset);
            IEnumerable<object> actualList = ConvertEnumerableToList(actual);
            IEnumerable<object> subsetList = ConvertEnumerableToList(subset);

            actualList.Count().ShouldNotEqual(subsetList.Count());

            return actual;
        }

        /// <summary>
        /// Asserts the given collection should be a subset of the provided collection.
        /// </summary>
        /// <param name = "actual">The given collection.</param>
        /// <param name = "superset">The collection that should contain the given collection.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static IEnumerable ShouldBeSubsetOf(this IEnumerable actual, IEnumerable superset)
        {
            IEnumerable<object> actualList = ConvertEnumerableToList(actual);
            IEnumerable<object> supersetList = ConvertEnumerableToList(superset);

            actualList.ShouldBeSubsetOf(supersetList);

            return actual;
        }

        /// <summary>
        /// Asserts the given collection should contain the provided items.
        /// </summary>
        /// <param name = "actual">The given collection.</param>
        /// <param name = "superset">The collection that should contain the given collection.</param>
        /// <typeparam name = "T">The type of elements.</typeparam>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static IEnumerable<T> ShouldBeSubsetOf<T>(this IEnumerable<T> actual, IEnumerable<T> superset)
        {
            var comparer = new AssertEqualityComparer<T>();

            List<T> missingItems = actual.Where(item => !superset.Contains(item, comparer)).ToList();

            if (missingItems.Any())
            {
                throw NewException(
                    "Should be a subset of: {0} \r\nsubset collection: {1}\r\nhas missing items: {2}",
                    superset.EachToUsefulString(),
                    actual.EachToUsefulString(),
                    missingItems.EachToUsefulString());
            }

            return actual;
        }

        /// <summary>
        /// Asserts the given collection should be a superset of the provided collection.
        /// </summary>
        /// <param name = "actual">The given collection.</param>
        /// <param name = "subset">The collection that should be a subset of the given collection.</param>
        /// <typeparam name = "T">The type of elements.</typeparam>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static IEnumerable<T> ShouldBeSupersetOf<T>(this IEnumerable<T> actual, IEnumerable<T> subset)
        {
            if (actual == null)
            {
                throw new ArgumentNullException("actual");
            }
            if (subset == null)
            {
                throw new ArgumentNullException("subset");
            }

            var comparer = new AssertEqualityComparer<T>();
            List<T> missingItems = subset.Where(item => !actual.Contains(item, comparer)).ToList();

            if (missingItems.Any())
            {
                string message = string.Format(
                    CultureInfo.InvariantCulture,
                    "Should be a superset of: {0} \r\nsuperset collection: {1}\r\nhas missing items: {2}",
                    subset.EachToUsefulString(),
                    actual.EachToUsefulString(),
                    missingItems.EachToUsefulString());

                throw NewException(message);
            }

            return actual;
        }

        /// <summary>
        /// Asserts the given collection should be a superset of the provided items.
        /// </summary>
        /// <param name = "actual">The given collection.</param>
        /// <param name = "subset">The collection that should be a subset of the given collection.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static IEnumerable ShouldBeSupersetOf(this IEnumerable actual, IEnumerable subset)
        {
            IEnumerable<object> actualList = ConvertEnumerableToList(actual);
            IEnumerable<object> subsetList = ConvertEnumerableToList(subset);

            actualList.ShouldBeSupersetOf(subsetList);

            return actual;
        }

        /// <summary>
        /// Asserts the given value should be delimited by the provided starting and ending delimiters.
        /// </summary>
        /// <param name = "actual">The given value.</param>
        /// <param name = "expectedStartDelimiter">The starting delimiter.</param>
        /// <param name = "expectedEndDelimiter">The ending delimiter.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static string ShouldBeSurroundedWith(this string actual, string expectedStartDelimiter, string expectedEndDelimiter)
        {
            actual.ShouldStartWith(expectedStartDelimiter);
            actual.ShouldEndWith(expectedEndDelimiter);
            return actual;
        }

        /// <summary>
        /// Asserts the given value should be delimited by the provided delimiter.
        /// </summary>
        /// <param name = "actual">The given value.</param>
        /// <param name = "expectedDelimiter">The delimiter.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static string ShouldBeSurroundedWith(this string actual, string expectedDelimiter)
        {
            return ShouldBeSurroundedWith(actual, expectedDelimiter, expectedDelimiter);
        }

        /// <summary>
        /// Asserts the given value have reference equality with the expected value.
        /// </summary>
        /// <param name = "actual">The given value.</param>
        /// <param name = "expected">The expected value.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static object ShouldBeTheSameAs(this object actual, object expected)
        {
            if (!ReferenceEquals(actual, expected))
            {
                throw NewException("Should be the same as {0} but is {1}", expected, actual);
            }

            return actual;
        }

        /// <summary>
        /// Asserts the delegate should throw an exception of the expected type.
        /// </summary>
        /// <param name = "exceptionType">The expected type of exception.</param>
        /// <param name = "method">The delegate.</param>
        /// <returns>
        /// The thrown exception.
        /// </returns>
        internal static Exception ShouldBeThrownBy(this Type exceptionType, Action method)
        {
            Exception exception = Catch.Exception(method);

            exception.ShouldNotBeNull();
            exception.ShouldBeOfType(exceptionType);
            return exception;
        }

        /// <summary>
        /// Asserts the condition should be <c>true</c>.
        /// </summary>
        /// <param name = "actual">The condition.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static bool ShouldBeTrue(this bool actual)
        {
            if (!actual)
            {
                throw NewException("Should be [true] but is [false]");
            }

            // ReSharper disable ConditionIsAlwaysTrueOrFalse
            return actual;

            // ReSharper restore ConditionIsAlwaysTrueOrFalse
        }

        /// <summary>
        /// Asserts the given string should contain the provided substring.
        /// </summary>
        /// <param name = "actual">The given string.</param>
        /// <param name = "expected">The required substring.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static string ShouldContain(this string actual, string expected)
        {
            if (expected == null)
            {
                throw new ArgumentNullException("expected");
            }

            if (actual == null)
            {
                throw NewException("Should contain {0} but is [null]", expected);
            }

            if (!actual.Contains(expected))
            {
                throw NewException("Should contain {0} but is {1}", expected, actual);
            }

            return actual;
        }

        /// <summary>
        /// Asserts the given exception contains the required message.
        /// </summary>
        /// <param name = "actual">The given exception.</param>
        /// <param name = "expected">The required message.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static Exception ShouldContainErrorMessage(this Exception actual, string expected)
        {
            if (expected == null)
            {
                throw new ArgumentNullException("expected");
            }

            actual.ShouldNotBeNull();
            actual.Message.ShouldContain(expected);
            return actual;
        }

        /// <summary>
        /// Asserts the given collection only contain the required items.
        /// </summary>
        /// <typeparam name = "T">The type of elements.</typeparam>
        /// <param name = "actual">The given collection.</param>
        /// <param name = "items">The required item.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static IEnumerable<T> ShouldContainOnly<T>(this IEnumerable<T> actual, params T[] items)
        {
            return actual.ShouldContainOnly((IEnumerable<T>)items);
        }

        /// <summary>
        /// Asserts the given collection only contain the required items.
        /// </summary>
        /// <typeparam name = "T">The type of elements.</typeparam>
        /// <param name = "actual">The given collection.</param>
        /// <param name = "items">The required item.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static IEnumerable<T> ShouldContainOnly<T>(this IEnumerable<T> actual, IEnumerable<T> items)
        {
            if (actual == null)
            {
                throw new ArgumentNullException("actual");
            }
            if (items == null)
            {
                throw new ArgumentNullException("items");
            }

            var source = new List<T>(actual);
            var missingItems = new List<T>();
            var comparer = new AssertEqualityComparer<T>();

            foreach (T item in items)
            {
                if (!source.Contains(item, comparer))
                {
                    missingItems.Add(item);
                }
                else
                {
                    source.Remove(item);
                }
            }

            if (missingItems.Any() || source.Any())
            {
                string message = string.Format(
                    CultureInfo.InvariantCulture,
                    "Should contain only: {0} \r\nentire collection: {1}",
                    items.EachToUsefulString(),
                    actual.EachToUsefulString());

                if (missingItems.Any())
                {
                    message += "\ndoes not contain: " + missingItems.EachToUsefulString();
                }

                if (source.Any())
                {
                    message += "\ndoes contain but shouldn't: " + source.EachToUsefulString();
                }

                throw NewException(message);
            }

            return actual;
        }

        /// <summary>
        /// Asserts the given string end with the expected substring.
        /// </summary>
        /// <param name = "actual">The given value.</param>
        /// <param name = "expected">The expected substring.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static string ShouldEndWith(this string actual, string expected)
        {
            if (expected == null)
            {
                throw new ArgumentNullException("expected");
            }

            if (actual == null)
            {
                throw NewException("Should end with {0} but is [null]", expected);
            }

            if (!actual.EndsWith(expected, StringComparison.OrdinalIgnoreCase))
            {
                throw NewException("Should end with {0} but is {1}", expected, actual);
            }

            return actual;
        }

        /// <summary>
        /// Asserts the given value equal the expected value.
        /// </summary>
        /// <typeparam name = "T">The type of objects to compare.</typeparam>
        /// <param name = "actual">The given value.</param>
        /// <param name = "expected">The expected value.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static T ShouldEqual<T>(this T actual, T expected)
        {
            if (!GeneralCompare(actual, expected, GeneralComparisonOperator.Equal))
            {
                throw NewException("Should equal {0} but is {1}", expected, actual);
            }

            return actual;
        }

        internal static IEnumerable<T> ShouldMatchSequence<T>(this IEnumerable<T> actual, IEnumerable<T> expected)
        {
            bool matches = expected.Zip(actual, (e, a) => GeneralCompare(a, e, GeneralComparisonOperator.Equal)).All(m => m);

            if (!matches || actual.Count() != expected.Count())
            {
                throw NewException(
                    "Should match sequences:  expected: {0}\r\nactual: {1}\r\n", expected.EachToUsefulString(), actual.EachToUsefulString());
            }

            return actual;
        }

        /// <summary>
        /// Asserts the given collection should contain at least one element.
        /// </summary>
        /// <param name = "actual">The given collection.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static IEnumerable ShouldNotBeEmpty(this IEnumerable actual)
        {
            if (!actual.IsNotEmpty())
            {
                throw NewException("Should not be empty but is");
            }

            return actual;
        }

        /// <summary>
        /// Asserts the given value not be null.
        /// </summary>
        /// <param name = "actual">The given value.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static object ShouldNotBeNull(this object actual)
        {
            if (actual == null)
            {
                throw NewException("Should be [not null] but is [null]");
            }

            return actual;
        }

        /// <summary>
        /// Asserts that the given string should not be null or empty.
        /// </summary>
        /// <param name = "actual">
        /// The given string.
        /// </param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static IEnumerable ShouldNotBeNullOrEmpty(this IEnumerable actual)
        {
            if (actual.IsNullOrEmpty())
            {
                throw NewException("Should be not null or empty but is");
            }

            return actual;
        }

        /// <summary>
        /// Asserts the given value not be of the provided type.
        /// </summary>
        /// <param name = "actual">The given value.</param>
        /// <param name = "type">The provided type.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static object ShouldNotBeOfType(this object actual, Type type)
        {
            if (type == null)
            {
                throw new ArgumentNullException("type");
            }
            if (actual == null)
            {
                throw new ArgumentNullException("actual");
            }

            Type actualType = actual.GetType();
            if (type.IsAssignableFrom(actualType))
            {
                throw NewException("Should not be of type {0} but is of type {1}", type, actualType);
            }

            return actual;
        }

        /// <summary>
        /// Asserts the given value not be of the provided type.
        /// </summary>
        /// <param name = "actual">The given value.</param>
        /// <param name = "types">The provided type.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static object ShouldNotBeOfType(this object actual, IEnumerable<Type> types)
        {
            if (actual == null)
            {
                throw new ArgumentNullException("actual");
            }

            if (types == null)
            {
                throw new ArgumentNullException("types");
            }

            Type actualType = actual.GetType();
            foreach (Type t in types.Where(t => t.IsAssignableFrom(actualType)))
            {
                throw NewException("Should not be of type {0} but is of type {1}", t, actualType);
            }

            return actual;
        }

        /// <summary>
        /// Asserts the given value not have reference equality with the reference value.
        /// </summary>
        /// <param name = "actual">The given value.</param>
        /// <param name = "reference">The disallowed value.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static object ShouldNotBeTheSameAs(this object actual, object reference)
        {
            if (ReferenceEquals(actual, reference))
            {
                throw NewException("Should not be the same as {0} but is {1}", reference, actual);
            }

            return actual;
        }

        /// <summary>
        /// Asserts the given value not have reference equality with the reference values.
        /// </summary>
        /// <param name = "actual">The given value.</param>
        /// <param name = "references">The disallowed values.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static object ShouldNotBeTheSameAs(this object actual, params object[] references)
        {
            IEnumerable<object> referenceList = references;
            return actual.ShouldNotBeTheSameAs(referenceList);
        }

        /// <summary>
        /// Asserts the given value not have reference equality with the reference values.
        /// </summary>
        /// <param name = "actual">The given value.</param>
        /// <param name = "references">The disallowed values.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static object ShouldNotBeTheSameAs(this object actual, IEnumerable<object> references)
        {
            if (references == null)
            {
                throw new ArgumentNullException("references");
            }

            foreach (object item in references)
            {
                actual.ShouldBeTheSameAs(item);
            }

            return actual;
        }

        /// <summary>
        /// Asserts the given string should not contain the provided substring.
        /// </summary>
        /// <param name = "actual">The given string.</param>
        /// <param name = "expected">The required substring.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static string ShouldNotContain(this string actual, string expected)
        {
            if (expected == null)
            {
                throw new ArgumentNullException("expected");
            }

            if (actual == null)
            {
                throw NewException("Should contain {0} but is [null]", expected);
            }

            if (actual.Contains(expected))
            {
                throw NewException("Should contain {0} but is {1}", expected, actual);
            }

            return actual;
        }

        /// <summary>
        /// Asserts the given collection should not contain any of the provided items.
        /// </summary>
        /// <param name = "actual">The given collection.</param>
        /// <param name = "items">The disallowed items.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static IEnumerable ShouldNotContain(this IEnumerable actual, params object[] items)
        {
            IEnumerable<object> actualList = actual.Cast<object>();
            IEnumerable<object> expectedList = items;

            actualList.ShouldNotContain(expectedList);
            return actual;
        }

        /// <summary>
        /// Asserts the given collection should not contain any of the provided items.
        /// </summary>
        /// <param name = "actual">The given collection.</param>
        /// <param name = "items">The disallowed items.</param>
        /// <typeparam name = "T">The type of elements.</typeparam>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static IEnumerable<T> ShouldNotContain<T>(this IEnumerable<T> actual, params T[] items)
        {
            var comparer = new AssertEqualityComparer<T>();

            List<T> contains = items.Where(item => actual.Contains(item, comparer)).ToList();

            if (contains.Any())
            {
                throw NewException(
                    "Should not contain: {0} \r\nentire collection: {1}\r\ndoes contain: {2}",
                    items.EachToUsefulString(),
                    actual.EachToUsefulString(),
                    contains.EachToUsefulString());
            }

            return actual;
        }

        /// <summary>
        /// Asserts the given value not equal the disallowed value.
        /// </summary>
        /// <typeparam name = "T">The type of objects to compare.</typeparam>
        /// <param name = "actual">The given value.</param>
        /// <param name = "expected">The disallowed value.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static object ShouldNotEqual<T>(this T actual, T expected)
        {
            if (!GeneralCompare(actual, expected, GeneralComparisonOperator.NotEqual))
            {
                throw NewException("Should not equal {0} but does: {1}", expected, actual);
            }

            return actual;
        }

        /// <summary>
        /// Asserts the given string start with the expected substring.
        /// </summary>
        /// <param name = "actual">The given value.</param>
        /// <param name = "expected">The expected substring.</param>
        /// <returns>
        /// The <paramref name = "actual" /> argument value.
        /// </returns>
        internal static string ShouldStartWith(this string actual, string expected)
        {
            if (expected == null)
            {
                throw new ArgumentNullException("expected");
            }

            if (actual == null)
            {
                throw NewException("Should start with {0} but is [null]", expected);
            }

            if (!actual.StartsWith(expected, StringComparison.OrdinalIgnoreCase))
            {
                throw NewException("Should start with {0} but is {1}", expected, actual);
            }

            return actual;
        }

        private static IEnumerable<object> ConvertEnumerableToList(IEnumerable inputEnumerable)
        {
            return inputEnumerable.Is<IEnumerable>() && !inputEnumerable.Is<string>() ? inputEnumerable.Cast<object>() : new[] { inputEnumerable };
        }

        private static bool DateTimeCompare(DateTime left, DateTime right, GeneralComparisonOperator comparisonOperator)
        {
            // the FCL assumes local time when converting unspecified date times.  Active Directory uses
            // unspecified times holding UTC values.  Force the engineers to be diligent with regard to DateTime.
            if (left.Kind == DateTimeKind.Unspecified && right.Kind != DateTimeKind.Unspecified)
            {
                // NEIN: throwing ArgumentException, create a specific exception.
                throw new ArgumentException("DateTimeKind.Unspecified is only supported when both comparands are unspecified.", "left");
            }

            if (left.Kind != DateTimeKind.Unspecified && right.Kind == DateTimeKind.Unspecified)
            {
                // NEIN: throwing ArgumentException, create a specific exception.
                throw new ArgumentException("DateTimeKind.Unspecified is only supported when both comparands are unspecified.", "right");
            }

            left = left.ToUniversalTime();
            right = right.ToUniversalTime();

            // switch on the comparison operator and perform the comparison.
            switch (comparisonOperator)
            {
                case GeneralComparisonOperator.Equal:
                    return left == right;
                case GeneralComparisonOperator.GreaterThan:
                    return left > right;
                case GeneralComparisonOperator.GreaterThanOrEqual:
                    return left >= right;
                case GeneralComparisonOperator.LessThan:
                    return left < right;
                case GeneralComparisonOperator.LessThanOrEqual:
                    return left <= right;
                default:
                    return left != right;
            }
        }

        private static bool DateTimeOffsetCompare(DateTimeOffset left, DateTimeOffset right, GeneralComparisonOperator comparisonOperator)
        {
            return DateTimeCompare(left.UtcDateTime, right.UtcDateTime, comparisonOperator);
        }

        private static bool DecimalCompare(decimal left, decimal right, GeneralComparisonOperator comparisonOperator)
        {
            // switch on the comparison operator and perform the comparison.
            switch (comparisonOperator)
            {
                case GeneralComparisonOperator.Equal:
                    return left == right;
                case GeneralComparisonOperator.GreaterThan:
                    return left > right;
                case GeneralComparisonOperator.GreaterThanOrEqual:
                    return left >= right;
                case GeneralComparisonOperator.LessThan:
                    return left < right;
                case GeneralComparisonOperator.LessThanOrEqual:
                    return left <= right;
                default:
                    return left != right;
            }
        }

        private static bool DoubleCompare(double left, double right, GeneralComparisonOperator comparisonOperator)
        {
            // switch on the comparison operator and perform the comparison.
            switch (comparisonOperator)
            {
                case GeneralComparisonOperator.Equal:
                    return left == right;
                case GeneralComparisonOperator.GreaterThan:
                    return left > right;
                case GeneralComparisonOperator.GreaterThanOrEqual:
                    return left >= right;
                case GeneralComparisonOperator.LessThan:
                    return left < right;
                case GeneralComparisonOperator.LessThanOrEqual:
                    return left <= right;
                default:
                    return left != right;
            }
        }

        private static string EachToUsefulString<T>(this IEnumerable<T> enumerable)
        {
            const int limit = 20;
            var sb = new StringBuilder();
            sb.AppendLine("{");
            sb.Append(string.Join(",\n", enumerable.Select(x => x.ToUsefulString().Tab()).Take(limit).ToArray()));
            if (enumerable.Count() > limit)
            {
                if (enumerable.Count() > limit + 1)
                {
                    sb.AppendLine(string.Format(CultureInfo.InvariantCulture, ",\n  ...({0} more elements)", enumerable.Count() - limit));
                }
                else
                {
                    sb.AppendLine(",\n" + enumerable.Last().ToUsefulString().Tab());
                }
            }
            else
            {
                sb.AppendLine();
            }

            sb.AppendLine("}");

            return sb.ToString();
        }

        private static Pair<object, object> EquateIntegerObjects(object left, object right)
        {
            Type leftType = left.GetType();
            Type rightType = right.GetType();

            // If neither type is a ulong (or it is a ulong but is less that the value of long.MaxValue), 
            // then it is safe to cast both to long and all comparisons operations will work correctly.
            if (((leftType != typeof(ulong)) || (leftType == typeof(ulong) && left.CastTo<ulong>() <= long.MaxValue)) &&
                ((rightType != typeof(ulong)) || (rightType == typeof(ulong) && right.CastTo<ulong>() <= long.MaxValue)))
            {
                return new Pair<object, object>(
                    Convert.ToInt64(left, CultureInfo.InvariantCulture), Convert.ToInt64(right, CultureInfo.InvariantCulture));
            }

            // If the left object is a ulong, leave it as a ulong (the comparison operation
            // has special cast handling for ulong types.
            if (leftType == typeof(ulong))
            {
                return new Pair<object, object>(left, Convert.ToInt64(right, CultureInfo.InvariantCulture));
            }

            // Otherwise the right object must be the ulong (because this function is never
            // called if they are both the same type), so we can safely cast the left to 
            // long and keep the right as a ulong (again, the comparison operation has
            // special handling for ulong types).
            return new Pair<object, object>(Convert.ToInt64(left, CultureInfo.InvariantCulture), right);
        }

        private static Pair<object, object> EquateNumericObjects(object left, object right)
        {
            Type leftType = left.GetType();
            Type rightType = right.GetType();

            // If either type is a Boolean value transform both types to a Boolean
            if (leftType == typeof(bool) || rightType == typeof(bool))
            {
                return new Pair<object, object>(
                    Convert.ToBoolean(left, CultureInfo.InvariantCulture), Convert.ToBoolean(right, CultureInfo.InvariantCulture));
            }

            // Okay, here's the deal (from MSDN) on decimals, they have greater precision (after 
            // the point) but a smaller range (before the decimal point).  Consequently, 
            // double.MaxValue and float.MaxValue (or double.MinValue and float.MinValue) can not fit 
            // into a decimal value.  As a result, we *have* to preference range over precision.  
            // So we therefore first try to cast to double first, then to single, then finally decimal.  

            // This hurts for decimal comparisons where precision is critical, but in that case, the 
            // answer *has* be the calling test case should ensure that both are a decimal first, then 
            // do the comparison.  (As we have established, we only get to this point if both types
            // are not exactly the same going into the comparison).

            // If either type is a double, the result is both cast to double.
            if (leftType == typeof(double) || rightType == typeof(double))
            {
                return new Pair<object, object>(
                    Convert.ToDouble(left, CultureInfo.InvariantCulture), Convert.ToDouble(right, CultureInfo.InvariantCulture));
            }

            // If either type is a float, the result is both cast to a float.
            if (leftType == typeof(float) || rightType == typeof(float))
            {
                return new Pair<object, object>(
                    Convert.ToSingle(left, CultureInfo.InvariantCulture), Convert.ToSingle(right, CultureInfo.InvariantCulture));
            }

            // If either type is a decimal, the result is both cast to decimal.
            if (leftType == typeof(decimal) || rightType == typeof(decimal))
            {
                return new Pair<object, object>(
                    Convert.ToDecimal(left, CultureInfo.InvariantCulture), Convert.ToDecimal(right, CultureInfo.InvariantCulture));
            }

            // Otherwise, they must both be integers because this function is only called
            // if they were both numeric types to begin with.  Thus, let the integer equation
            // logic handle the problem.
            return EquateIntegerObjects(left, right);
        }

        private static bool LongCompare(long left, long right, GeneralComparisonOperator comparisonOperator)
        {
            // switch on the comparison operator and perform the comparison.
            switch (comparisonOperator)
            {
                case GeneralComparisonOperator.Equal:
                    return left == right;
                case GeneralComparisonOperator.GreaterThan:
                    return left > right;
                case GeneralComparisonOperator.GreaterThanOrEqual:
                    return left >= right;
                case GeneralComparisonOperator.LessThan:
                    return left < right;
                case GeneralComparisonOperator.LessThanOrEqual:
                    return left <= right;
                default:
                    return left != right;
            }
        }

        private static Exception NewException(string message, params object[] parameters)
        {
            if (parameters.Any())
            {
                return
                    new AssertFailedException(
                        string.Format(CultureInfo.InvariantCulture, message, parameters.Select(x => x.ToUsefulString()).Cast<object>().ToArray()));
            }

            return new AssertFailedException(message);
        }

        private static bool NumericCompare(object left, object right, GeneralComparisonOperator comparisonOperator)
        {
            Type leftType = left.GetType();
            Type rightType = right.GetType();

            // There is no Reference Equality as that would have been handle by the caller, so we only have to 
            // deal with mathematical comparisons.

            // First let's deal with decimals (which is a special case floating point).
            if (leftType == typeof(decimal) && rightType == typeof(decimal))
            {
                return DecimalCompare(left.CastTo<decimal>(), right.CastTo<decimal>(), comparisonOperator);
            }

            // Next let's deal with any FloatingPoint (other than decimals) as they require no additional special rules.
            if (leftType.IsFloatingPoint() || rightType.IsFloatingPoint())
            {
                // Safely cast both as decimal and do a Floating Point comparison.
                return DoubleCompare(
                    Convert.ToDouble(left, CultureInfo.InvariantCulture), Convert.ToDouble(right, CultureInfo.InvariantCulture), comparisonOperator);
            }

            // Next let's deal with integer types when neither are a ulong.
            if (leftType != typeof(ulong) && rightType != typeof(ulong))
            {
                return LongCompare(
                    Convert.ToInt64(left, CultureInfo.InvariantCulture), Convert.ToInt64(right, CultureInfo.InvariantCulture), comparisonOperator);
            }

            // If both types are ulong, we can simply compare the values
            if (leftType == typeof(ulong) && rightType == typeof(ulong))
            {
                return UlongCompare(
                    Convert.ToUInt64(left, CultureInfo.InvariantCulture), Convert.ToUInt64(right, CultureInfo.InvariantCulture), comparisonOperator);
            }

            // We only need to test one for ulong because the other can't be (we've handled the case where they both are).
            bool leftIsUlong = left.Is<ulong>();

            // Okay now the special case rules for ulong types.  We already know that both are not 
            // ulong types (as that has already been handled).  The casting rules will only result with 
            // a ulong value if the value of the ulong was greater than long.MaxValue (otherwise it would
            // have simply been turned into a long).  Thus most of the comparisons are fairly simple)
            switch (comparisonOperator)
            {
                    // They can not be equal because one is greater than long.MaxValue and the other is not.
                case GeneralComparisonOperator.Equal:
                    return false;

                    // left is greater than (also greater than or equal) right if left was the ulong, otherwise it is not.
                case GeneralComparisonOperator.GreaterThan:
                case GeneralComparisonOperator.GreaterThanOrEqual:
                    return leftIsUlong;

                case GeneralComparisonOperator.LessThan:
                case GeneralComparisonOperator.LessThanOrEqual:
                    return !leftIsUlong;

                    // Only not equal remains, they must be not equal because one is greater than 
                    // long.MaxValue and the other is not.
                default:
                    return true;
            }
        }

        private static bool SafeEquals<T>(this T left, T right)
        {
            var comparer = new AssertEqualityComparer<T>();

            return comparer.Equals(left, right);
        }

        private static bool StringCompare(string left, string right, GeneralComparisonOperator comparisonOperator)
        {
            int comp = left.CompareOs(right);

            // switch on the comparison operator and perform the comparison.)
            switch (comparisonOperator)
            {
                case GeneralComparisonOperator.Equal:
                    return comp == 0;
                case GeneralComparisonOperator.GreaterThan:
                    return comp > 0;
                case GeneralComparisonOperator.GreaterThanOrEqual:
                    return comp >= 0;
                case GeneralComparisonOperator.LessThan:
                    return comp < 0;
                case GeneralComparisonOperator.LessThanOrEqual:
                    return comp <= 0;
                default:
                    return comp != 0;
            }
        }

        private static string Tab(this string str)
        {
            if (string.IsNullOrEmpty(str))
            {
                return string.Empty;
            }

            string[] split = str.Split(new[] { "\r\n", "\n" }, StringSplitOptions.None);
            var sb = new StringBuilder();

            sb.Append("  " + split[0]);
            foreach (string part in split.Skip(1))
            {
                sb.AppendLine();
                sb.Append("  " + part);
            }

            return sb.ToString();
        }

        private static bool TimespanCompare(TimeSpan left, TimeSpan right, GeneralComparisonOperator comparisonOperator)
        {
            return LongCompare(left.Ticks, right.Ticks, comparisonOperator);
        }

        private static string ToUsefulString(this object obj)
        {
            string str;
            if (obj == null)
            {
                return "[null]";
            }

            var s = obj as string;
            if (s != null)
            {
                str = s;
                return "\"" + str.Replace("\n", "\\n") + "\"";
            }

            if (obj.GetType().IsValueType)
            {
                return "[" + obj + "]";
            }

            var objAsEnumerable = obj as IEnumerable;
            if (objAsEnumerable != null)
            {
                IEnumerable<object> enumerable = objAsEnumerable.Cast<object>();

                return obj.GetType() + ":\n" + enumerable.EachToUsefulString();
            }

            str = obj.ToString();

            if (string.IsNullOrEmpty(str))
            {
                return string.Format(CultureInfo.InvariantCulture, "{0}:[]", obj.GetType());
            }

            str = str.Trim();

            if (str.Contains("\n"))
            {
                return string.Format(CultureInfo.InvariantCulture, "{1}:\r\n[\r\n{0}\r\n]", str.Tab(), obj.GetType());
            }

            if (obj.GetType().ToString() == str)
            {
                return obj.GetType().ToString();
            }

            return string.Format(CultureInfo.InvariantCulture, "{0}:[{1}]", obj.GetType(), str);
        }

        private static bool UlongCompare(ulong left, ulong right, GeneralComparisonOperator comparisonOperator)
        {
            // switch on the comparison operator and perform the comparison.
            switch (comparisonOperator)
            {
                case GeneralComparisonOperator.Equal:
                    return left == right;
                case GeneralComparisonOperator.GreaterThan:
                    return left > right;
                case GeneralComparisonOperator.GreaterThanOrEqual:
                    return left >= right;
                case GeneralComparisonOperator.LessThan:
                    return left < right;
                case GeneralComparisonOperator.LessThanOrEqual:
                    return left <= right;
                default:
                    return left != right;
            }
        }
    }
}
