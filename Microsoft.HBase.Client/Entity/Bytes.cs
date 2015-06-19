

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.HBase.Client.Entity
{
    public enum HBaseDataTypes
    {
        TypeUNKNOWN = 0x0,
        TypeString,
        TypeBoolean,
        TypeLong,
        TypeInt,
        TypeFloat,
        TypeDouble,
        TypeDateTime,
        TypeBinary
    }
    /// <summary>
    /// http://grepcode.com/file/repository.cloudera.com/content/repositories/releases/com.cloudera.hbase/hbase/0.89.20100924-28/org/apache/hadoop/hbase/util/Bytes.java
    /// </summary>
    public static class Bytes
    {

        public const int SIZEOF_BOOLEAN = sizeof(bool);
        public const int SIZEOF_BYTE = sizeof(byte);
        public const int SIZEOF_CHAR = sizeof(char);
        public const int SIZEOF_DOUBLE = sizeof(double);
        public const int SIZEOF_FLOAT = sizeof(float);
        public const int SIZEOF_INT = sizeof(int);
        public const int SIZEOF_LONG = sizeof(long);

        public static string ToString(byte[] bytes)
        {
            if (bytes == null || bytes.Length == 0)
                return string.Empty;

            return Encoding.UTF8.GetString(bytes);
        }

        public static bool ToBoolean(byte[] bytes)
        {
            if (bytes.Length != SIZEOF_BOOLEAN)
                throw new ArgumentOutOfRangeException();

            return bytes[0] != (byte)0x0;
        }

        public static long ToLong(byte[] bytes)
        {
            if (bytes.Length != SIZEOF_LONG)
            {
                throw new ArgumentOutOfRangeException();
            }

            long l = 0;
            for (var i = 0; i < SIZEOF_LONG; i++)
            {
                l <<= 8;
                l ^= bytes[i] & 0xFF;
            }

            return l;
        }

        public static int ToInt(byte[] bytes)
        {
            if (bytes.Length != SIZEOF_INT)
            {
                throw new ArgumentOutOfRangeException();
            }

            int n = 0;
            for (var i = 0; i < SIZEOF_INT; i++)
            {
                n <<= 8;
                n ^= bytes[i] & 0xFF;
            }

            return n;
        }

        public static float ToFloat(byte[] bytes)
        {
            return IntBitsToFloat(ToInt(bytes));
        }

        public static double ToDouble(byte[] bytes)
        {
            return LongBitsToDouble(ToLong(bytes));
        }

        public static DateTime ToDateTime(byte[] bytes)
        {
            return ToDateTime(ToLong(bytes));
        }

        public static DateTime ToDateTime(long timestamp)
        {
            return new DateTime(1970, 1, 1).AddMilliseconds(timestamp);
        }

        public static bool IsSupportedType(Type t)
        {
            if (t == null)
                throw new ArgumentNullException();

            return supportedTypesMapping.ContainsKey(t);
        }

        public static object ToObject(byte[] bytes, Type t)
        {
            var ht = HBaseDataTypes.TypeUNKNOWN;
            if (!supportedTypesMapping.TryGetValue(t, out ht))
                throw new ArgumentOutOfRangeException();

            switch (ht)
            {
                case HBaseDataTypes.TypeBinary:
                    return bytes;
                case HBaseDataTypes.TypeBoolean:
                    return ToBoolean(bytes);
                case HBaseDataTypes.TypeDateTime:
                    return ToDateTime(bytes);
                case HBaseDataTypes.TypeDouble:
                    return ToDouble(bytes);
                case HBaseDataTypes.TypeFloat:
                    return ToFloat(bytes);
                case HBaseDataTypes.TypeInt:
                    return ToInt(bytes);
                case HBaseDataTypes.TypeLong:
                    return ToLong(bytes);
                case HBaseDataTypes.TypeString:
                    return ToString(bytes);
                default:
                    throw new NotImplementedException();
            }

        }
        public static byte[] ToBytes(object o)
        {
            if (o == null)
                return null;

            var t = o.GetType();
            var ht = HBaseDataTypes.TypeUNKNOWN;
            if (!supportedTypesMapping.TryGetValue(t, out ht))
                throw new ArgumentOutOfRangeException();

            switch (ht)
            {
                case HBaseDataTypes.TypeBinary:
                    return (byte[])o;
                case HBaseDataTypes.TypeBoolean:
                    return ToBytes((bool)o);
                case HBaseDataTypes.TypeDateTime:
                    return ToBytes((DateTime)o);
                case HBaseDataTypes.TypeDouble:
                    return ToBytes((double)o);
                case HBaseDataTypes.TypeFloat:
                    return ToBytes((float)o);
                case HBaseDataTypes.TypeInt:
                    return ToBytes((int)o);
                case HBaseDataTypes.TypeLong:
                    return ToBytes((long)o);
                case HBaseDataTypes.TypeString:
                    return ToBytes((string)o);
                default:
                    throw new NotImplementedException();
            }

        }

        public static byte[] ToBytes(string s)
        {
            if (string.IsNullOrEmpty(s))
                return null;

            return Encoding.UTF8.GetBytes(s);
        }

        public static byte[] ToBytes(bool b)
        {
            return new byte[] { b ? (byte)0xFF : (byte)0x0 };
        }

        public static byte[] ToBytes(long l)
        {
            var bs = new byte[SIZEOF_LONG];
            for (var i = SIZEOF_LONG - 1; i >= 0; i--)
            {
                bs[i] = (byte)(l & 0xFFL);
                l >>= 8;
            }

            return bs;
        }

        public static byte[] ToBytes(int n)
        {
            var bs = new byte[SIZEOF_INT];
            for (var i = SIZEOF_INT - 1; i >= 0; i--)
            {
                bs[i] = (byte)(n & 0xFF);
                n >>= 8;
            }

            return bs;
        }

        public static byte[] ToBytes(float f)
        {
            return ToBytes(FloatToIntBits(f));
        }

        public static byte[] ToBytes(double d)
        {
            return ToBytes(DoubleToLongBits(d));
        }

        public static byte[] ToBytes(DateTime dt)
        {
            return ToBytes(DateTimeToLong(dt));
        }

        public static long DateTimeToLong(DateTime dt)
        {
            return (long)(dt - new DateTime(1970, 1, 1)).TotalMilliseconds;
        }

        public static string BytesToBase64(byte[] bytes)
        {
            return Convert.ToBase64String(bytes);
        }

        public static byte[] Base64ToBytes(string base64)
        {
            return Convert.FromBase64String(base64);
        }

        public static string BytesToHexString(byte[] bytes)
        {
            StringBuilder sb = new StringBuilder(bytes.Length * 2 + 1);
            foreach (var b in bytes)
            {
                sb.AppendFormat("X2", b);
            }

            return sb.ToString();
        }

        public static byte[] HexStringToBytes(string hex)
        {
            if (string.IsNullOrEmpty(hex))
            {
                return null;
            }

            var len = hex.Length;
            if (len % 2 != 0)
            {
                throw new ArgumentOutOfRangeException();
            }

            var bytes = new byte[len >> 1];
            for (var i = 0; i < hex.Length; i += 2)
            {
                bytes[i >> 1] = (byte)((GetByteFromChar(hex[i]) << 4) | GetByteFromChar(hex[i + 1]));
            }

            return bytes;
        }

        #region Private
        private static readonly Dictionary<Type, HBaseDataTypes> supportedTypesMapping = new Dictionary<Type, HBaseDataTypes>
        {
            {typeof(string), HBaseDataTypes.TypeString},
            {typeof(bool), HBaseDataTypes.TypeBoolean},
            {typeof(long), HBaseDataTypes.TypeLong},
            {typeof(int), HBaseDataTypes.TypeInt},
            {typeof(double), HBaseDataTypes.TypeDouble},
            {typeof(float), HBaseDataTypes.TypeFloat},
            {typeof(DateTime), HBaseDataTypes.TypeDateTime},
            {typeof(byte[]), HBaseDataTypes.TypeBinary}
        };

        private const float FLOAT_POSITIVE_INFINITY = 1.0f / 0.0f;
        private const float FLOAT_NEGATIVE_INFINITY = -1.0f / 0.0f;
        private const float FLOAT_NAN = 0.0f / 0.0f;
        private const int FLOAT_POSITIVE_INFINITY_INTBITS = 0x7F800000;
        private const int FLOAT_NEGATIVE_INFINITY_INTBITS = (0xFF << 24) | 0x800000;
        private const int FLOAT_NAN_INTBITS = 0x7FC00000;

        private const double DOUBLE_POSITIVE_INFINITY = 1.0d / 0.0d;
        private const double DOUBLE_NEGATIVE_INFINITY = -1.0d / 0.0d;
        private const double DOUBLE_NAN = 0.0d / 0.0d;
        private const long DOUBLE_POSITIVE_INFINITY_LONGBITS = 0x7FF0000000000000L;
        private const long DOUBLE_NEGATIVE_INFINITY_LONGBITS = (0xFFL << 56) | 0xF0000000000000L;
        private const long DOUBLE_NAN_LONGBITS = 0x7FF8000000000000L;

        private static float IntBitsToFloat(int n)
        {
            if (n == FLOAT_POSITIVE_INFINITY_INTBITS) return FLOAT_POSITIVE_INFINITY;
            if (n == FLOAT_NEGATIVE_INFINITY_INTBITS) return FLOAT_NEGATIVE_INFINITY;
            if (n == FLOAT_NAN_INTBITS) return FLOAT_NAN;

            unsafe
            {
                return *((float*)&n);
            }
        }
        private static int FloatToIntBits(float f)
        {
            if (float.IsPositiveInfinity(f)) return FLOAT_POSITIVE_INFINITY_INTBITS;
            if (float.IsNegativeInfinity(f)) return FLOAT_NEGATIVE_INFINITY_INTBITS;
            if (float.IsNaN(f)) return FLOAT_NAN_INTBITS;

            unsafe
            {
                return *((int*)&f);
            }
        }
        private static double LongBitsToDouble(long l)
        {
            if (l == DOUBLE_POSITIVE_INFINITY_LONGBITS) return DOUBLE_POSITIVE_INFINITY;
            if (l == DOUBLE_NEGATIVE_INFINITY_LONGBITS) return DOUBLE_NEGATIVE_INFINITY;
            if (l == DOUBLE_NAN_LONGBITS) return DOUBLE_NAN;

            unsafe
            {
                return *((double*)&l);
            }
        }
        private static long DoubleToLongBits(double d)
        {
            if (double.IsPositiveInfinity(d)) return DOUBLE_POSITIVE_INFINITY_LONGBITS;
            if (double.IsNegativeInfinity(d)) return DOUBLE_NEGATIVE_INFINITY_LONGBITS;
            if (double.IsNaN(d)) return DOUBLE_NAN_LONGBITS;

            unsafe
            {
                return *((long*)&d);
            }
        }

        private static byte GetByteFromChar(char c)
        {
            var val = (int)c;
            val = (val - (val < 0x3A ? 0x30 : 0x37));
            if (val < 0x0 || val > 0xF)
            {
                throw new ArgumentOutOfRangeException();
            }

            return (byte)val;
        }

        #endregion
    }
}
