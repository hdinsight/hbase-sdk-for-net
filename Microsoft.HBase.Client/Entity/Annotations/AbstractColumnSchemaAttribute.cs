using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Newtonsoft.Json;


namespace Microsoft.HBase.Client.Entity.Annotations
{
    public enum ColumnEncodingOption
    {
        /// <summary>
        /// this option, row key will only call Bytes.ToBytes to get the byte array
        /// sortable, easy access raw data. but: not friendly with rest api, eg, int value 1 will be stored as 0x00000001, no way you can use a string to access the data
        /// </summary>
        RawBytes = 0,
        /// <summary>
        /// this option, row key will get the hex format string of the Bytes.ToBytes, then store the string as Bytes again
        /// sortable, soso access raw data, rest api friendly, but longer
        /// </summary>
        HexString,
        /// <summary>
        /// this option, raw key will get the Bytes.ToBytes and then Base64 get the string, then store the string as Bytes again
        /// not sortable, hard access raw data, rest api friendly, shorter than HexString
        /// </summary>
        RawBase64String,
        /// <summary>
        /// RawBase64String, then escape / to _ to make rest api friendly
        /// </summary>
        SlashEscapedBase64String,

        /// <summary>
        /// UDT, convert to Json string then to bytes
        /// </summary>
        UDTConvertToJson
    }

    public abstract class AbstractColumnSchemaAttribute : Attribute
    {
        public AbstractColumnSchemaAttribute(string columnFamily)
            : this(columnFamily, string.Empty)
        {

        }
        public AbstractColumnSchemaAttribute(string columnFamily, string columnQualifier)
        {
            ValidateName(columnFamily);
            ColumnFamily = columnFamily;

            if (!string.IsNullOrEmpty(columnQualifier))
            {
                ValidateName(columnQualifier);
                ColumnQualifier = columnQualifier;
            }
        }

        private static readonly Regex nameRule = new Regex("[a-z][0-9a-z]*", RegexOptions.IgnoreCase);

        public ColumnEncodingOption ColumnEncoding { get; set; }
        public string ColumnFamily { get; private set; }
        public string ColumnQualifier { get; private set; }
        protected void ValidateName(string name)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException("name");

            if (!nameRule.IsMatch(name))
                throw new ArgumentOutOfRangeException("name");
        }

        public byte[] GetColumnBytes(object value)
        {
            return GetColumnBytes(value, ColumnEncoding);
        }

        public object GetColumnObject(byte[] bytes, Type t)
        {
            return GetColumnObject(bytes, t, ColumnEncoding);
        }

        public static byte[] GetColumnBytes(object value, ColumnEncodingOption encodingOption)
        {
            var bytes = Bytes.ToBytes(value);

            switch (encodingOption)
            {
                case ColumnEncodingOption.RawBytes:
                    return bytes;
                case ColumnEncodingOption.HexString:
                    return Bytes.ToBytes(Bytes.BytesToHexString(bytes));
                case ColumnEncodingOption.RawBase64String:
                    return Bytes.ToBytes(Bytes.BytesToBase64(bytes));
                case ColumnEncodingOption.SlashEscapedBase64String:
                    return Bytes.ToBytes(Bytes.BytesToBase64(bytes).Replace('/', '_'));
                case ColumnEncodingOption.UDTConvertToJson:
                    return Bytes.ToBytes(JsonConvert.SerializeObject(value));
                default:
                    throw new NotImplementedException();
            }

        }

        public static object GetColumnObject(byte[] bytes, Type t, ColumnEncodingOption encodingOption)
        {
            var rawBytes = default(byte[]);

            switch (encodingOption)
            {
                case ColumnEncodingOption.RawBytes:
                    rawBytes = bytes;
                    break;
                case ColumnEncodingOption.HexString:
                    rawBytes = Bytes.HexStringToBytes(Bytes.ToString(bytes));
                    break;
                case ColumnEncodingOption.RawBase64String:
                    rawBytes = Bytes.Base64ToBytes(Bytes.ToString(bytes));
                    break;
                case ColumnEncodingOption.SlashEscapedBase64String:
                    rawBytes = Bytes.Base64ToBytes(Bytes.ToString(bytes).Replace('_', '/'));
                    break;
                case ColumnEncodingOption.UDTConvertToJson:
                    return JsonConvert.DeserializeObject(Bytes.ToString(bytes), t);
                default:
                    throw new NotImplementedException();
            }

            return Bytes.ToObject(rawBytes, t);
        }

    }
}
