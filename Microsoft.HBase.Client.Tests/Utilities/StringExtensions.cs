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
    using System.Globalization;
    using System.Text.RegularExpressions;

    internal static class StringExtensions
    {
        internal static int CompareOi(this string thisValue, string with)
        {
            return string.Compare(thisValue, with, StringComparison.OrdinalIgnoreCase);
        }

        internal static int CompareOs(this string thisValue, string with)
        {
            return string.Compare(thisValue, with, StringComparison.Ordinal);
        }

        internal static bool EqualsOi(this string thisValue, string with)
        {
            return thisValue.CompareOi(with) == 0;
        }

        internal static bool EqualsOs(this string thisValue, string with)
        {
            return thisValue.CompareOs(with) == 0;
        }

        internal static string FormatIc(this string thisValue, params object[] args)
        {
            return string.Format(CultureInfo.InvariantCulture, thisValue, args);
        }

        internal static int IndexOfOi(this string thisValue, string search)
        {
            if (ReferenceEquals(thisValue, null))
            {
                throw new ArgumentNullException("thisValue");
            }
            else
            {
                return thisValue.IndexOf(search, StringComparison.OrdinalIgnoreCase);
            }
        }

        internal static int IndexOfOs(this string thisValue, string search)
        {
            if (ReferenceEquals(thisValue, null))
            {
                throw new ArgumentNullException("thisValue");
            }
            else
            {
                return thisValue.IndexOf(search, StringComparison.Ordinal);
            }
        }

        internal static bool IsOrdinalEqual(this string thisValue, string with)
        {
            return thisValue.CompareOs(with) == 0;
        }

        internal static string StripWhiteSpace(this string value)
        {
            return Regex.Replace(value, "\\s", string.Empty);
        }

        internal static bool ToBooleanIc(this string value)
        {
            if (ReferenceEquals(value, null))
            {
                throw new ArgumentNullException("value");
            }
            bool result;
            if (bool.TryParse(value, out result))
            {
                return result;
            }
            string thisValue = value.Trim();
            if (thisValue.EqualsOi("1") || thisValue.EqualsOi("-1") || thisValue.EqualsOi("yes"))
            {
                return true;
            }
            if (thisValue.EqualsOi("0") || thisValue.EqualsOi("no"))
            {
                return false;
            }
            else
            {
                throw new InvalidOperationException("Unable to convert value to a Boolean.");
            }
        }

        internal static byte ToByteIc(this string thisValue)
        {
            return byte.Parse(thisValue, CultureInfo.InvariantCulture);
        }

        internal static DateTime ToDateTimeLocalEnu(this string value)
        {
            CultureInfo specificCulture = CultureInfo.CreateSpecificCulture("en-US");
            DateTime result;
            if (DateTime.TryParse(value, specificCulture, DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.AssumeLocal, out result))
            {
                return result;
            }
            throw new InvalidOperationException("Unable to convert {0} to DateTime.".FormatIc(new object[] { value }));
        }

        internal static Decimal ToDecimalIc(this string thisValue)
        {
            return Decimal.Parse(thisValue, CultureInfo.InvariantCulture);
        }

        internal static double ToDoubleIc(this string thisValue)
        {
            return double.Parse(thisValue, CultureInfo.InvariantCulture);
        }

        internal static short ToInt16Ic(this string thisValue)
        {
            return short.Parse(thisValue, CultureInfo.InvariantCulture);
        }

        internal static int ToInt32Ic(this string thisValue)
        {
            return int.Parse(thisValue, CultureInfo.InvariantCulture);
        }

        internal static long ToInt64Ic(this string thisValue)
        {
            return long.Parse(thisValue, CultureInfo.InvariantCulture);
        }

        internal static sbyte ToSByteIc(this string thisValue)
        {
            return sbyte.Parse(thisValue, CultureInfo.InvariantCulture);
        }

        internal static float ToSingleIc(this string thisValue)
        {
            return float.Parse(thisValue, CultureInfo.InvariantCulture);
        }

        internal static object ToType(this string value, Type type)
        {
            if (type == typeof(string))
            {
                return value;
            }
            if (type == typeof(bool))
            {
                return value.ToBooleanIc();
            }
            if (type == typeof(long))
            {
                return value.ToInt64Ic();
            }
            if (type == typeof(double))
            {
                return value.ToDoubleIc();
            }
            else
            {
                throw new InvalidOperationException("Unable to convert type.");
            }
        }
        
        internal static ushort ToUInt16Ic(this string thisValue)
        {
            return ushort.Parse(thisValue, CultureInfo.InvariantCulture);
        }
        
        internal static uint ToUInt32Ic(this string thisValue)
        {
            return uint.Parse(thisValue, CultureInfo.InvariantCulture);
        }
        
        internal static ulong ToUInt64Ic(this string thisValue)
        {
            return ulong.Parse(thisValue, CultureInfo.InvariantCulture);
        }

        internal static string TrimNullOrEmptyToDefault(this string value, string defaultValue)
        {
            string str = (value ?? string.Empty).Trim();
            if (str.Length == 0)
            {
                str = defaultValue;
            }
            return str;
        }

        internal static string TrimNullToEmpty(this string value)
        {
            return (value ?? string.Empty).Trim();
        }
    }
}
