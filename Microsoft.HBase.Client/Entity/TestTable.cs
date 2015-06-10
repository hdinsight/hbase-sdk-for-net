using Microsoft.HBase.Client.Entity.Annotations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.HBase.Client.Entity
{
    public class UdtInRow
    {
        public string StringProp { get; set; }
    }

    public class TestRow : HBaseRow
    {
        [RowKey(ColumnEncoding = ColumnEncodingOption.SlashEscapedBase64String)]
        public string Key { get; set; }

        #region RawBytes
        [SingleColumnQualifierSchema("cfrawbytes", "cqstring", ColumnEncoding = ColumnEncodingOption.RawBytes)]
        public string StringPropRawBytes { get; set; }
        [SingleColumnQualifierSchema("cfrawbytes", "cqint", ColumnEncoding = ColumnEncodingOption.RawBytes)]
        public int IntPropRawBytes { get; set; }
        [SingleColumnQualifierSchema("cfrawbytes", "cqlong", ColumnEncoding = ColumnEncodingOption.RawBytes)]
        public long LongPropRawBytes { get; set; }
        [SingleColumnQualifierSchema("cfrawbytes", "cqfloat", ColumnEncoding = ColumnEncodingOption.RawBytes)]
        public float FloatPropRawBytes { get; set; }
        [SingleColumnQualifierSchema("cfrawbytes", "cqdouble", ColumnEncoding = ColumnEncodingOption.RawBytes)]
        public double DoublePropRawBytes { get; set; }
        [SingleColumnQualifierSchema("cfrawbytes", "cqbool", ColumnEncoding = ColumnEncodingOption.RawBytes)]
        public bool BoolPropRawBytes { get; set; }
        [SingleColumnQualifierSchema("cfrawbytes", "cqdatetime", ColumnEncoding = ColumnEncodingOption.RawBytes)]
        public DateTime DateTimePropRawBytes { get; set; }
        [SingleColumnQualifierSchema("cfrawbytes", "cqbinary", ColumnEncoding = ColumnEncodingOption.RawBytes)]
        public byte[] BinaryPropRawBytes { get; set; }
        #endregion

        #region RawBase64String
        [SingleColumnQualifierSchema("cfrawbase64", "cqstring", ColumnEncoding = ColumnEncodingOption.RawBase64String)]
        public string StringPropBase64Bytes { get; set; }
        [SingleColumnQualifierSchema("cfrawbase64", "cqint", ColumnEncoding = ColumnEncodingOption.RawBase64String)]
        public int IntPropBase64Bytes { get; set; }
        [SingleColumnQualifierSchema("cfrawbase64", "cqlong", ColumnEncoding = ColumnEncodingOption.RawBase64String)]
        public long LongPropBase64Bytes { get; set; }
        [SingleColumnQualifierSchema("cfrawbase64", "cqfloat", ColumnEncoding = ColumnEncodingOption.RawBase64String)]
        public float FloatPropBase64Bytes { get; set; }
        [SingleColumnQualifierSchema("cfrawbase64", "cqdouble", ColumnEncoding = ColumnEncodingOption.RawBase64String)]
        public double DoublePropBase64Bytes { get; set; }
        [SingleColumnQualifierSchema("cfrawbase64", "cqbool", ColumnEncoding = ColumnEncodingOption.RawBase64String)]
        public bool BoolPropBase64Bytes { get; set; }
        [SingleColumnQualifierSchema("cfrawbase64", "cqdatetime", ColumnEncoding = ColumnEncodingOption.RawBase64String)]
        public DateTime DateTimePropBase64Bytes { get; set; }
        [SingleColumnQualifierSchema("cfrawbase64", "cqbinary", ColumnEncoding = ColumnEncodingOption.RawBase64String)]
        public byte[] BinaryPropBase64Bytes { get; set; }
        #endregion

        #region HexString
        [SingleColumnQualifierSchema("cfhex", "cqstring", ColumnEncoding = ColumnEncodingOption.HexString)]
        public string StringPropHexString { get; set; }
        [SingleColumnQualifierSchema("cfhex", "cqint", ColumnEncoding = ColumnEncodingOption.HexString)]
        public int IntPropHexString { get; set; }
        [SingleColumnQualifierSchema("cfhex", "cqlong", ColumnEncoding = ColumnEncodingOption.HexString)]
        public long LongPropHexString { get; set; }
        [SingleColumnQualifierSchema("cfhex", "cqfloat", ColumnEncoding = ColumnEncodingOption.HexString)]
        public float FloatPropHexString { get; set; }
        [SingleColumnQualifierSchema("cfhex", "cqdouble", ColumnEncoding = ColumnEncodingOption.HexString)]
        public double DoublePropHexString { get; set; }
        [SingleColumnQualifierSchema("cfhex", "cqbool", ColumnEncoding = ColumnEncodingOption.HexString)]
        public bool BoolPropHexString { get; set; }
        [SingleColumnQualifierSchema("cfhex", "cqdatetime", ColumnEncoding = ColumnEncodingOption.HexString)]
        public DateTime DateTimePropHexString { get; set; }
        [SingleColumnQualifierSchema("cfhex", "cqbinary", ColumnEncoding = ColumnEncodingOption.HexString)]
        public byte[] BinaryPropHexString { get; set; }
        #endregion


        #region UDT
        [SingleColumnQualifierSchema("cfudt", "cqudtsingle", ColumnEncoding = ColumnEncodingOption.UDTConvertToJson)]
        public UdtInRow UdtProp { get; set; }
        [SingleColumnQualifierSchema("cfudt", "cqudtlist", ColumnEncoding = ColumnEncodingOption.UDTConvertToJson)]
        public List<UdtInRow> UdtListProp { get; set; }
        [SingleColumnQualifierSchema("cfudt", "cqudtdict", ColumnEncoding = ColumnEncodingOption.UDTConvertToJson)]
        public Dictionary<int, UdtInRow> UdtDictProp { get; set; }

        #endregion

        #region GroupedCF
        [DictionaryKeyAsColumnQualifierColumnSchema("cfdictwithvc", ColumnEncoding = ColumnEncodingOption.RawBytes, VersionControl = true)]
        public Dictionary<string, string> DictPropWithVersionControl { get; set; }
        [DictionaryKeyAsColumnQualifierColumnSchema("cfdictwithoutvc", ColumnEncoding = ColumnEncodingOption.UDTConvertToJson, VersionControl = false)]
        public Dictionary<string, UdtInRow> DictPropWithoutVersionControl { get; set; }

        #endregion
    }

}
