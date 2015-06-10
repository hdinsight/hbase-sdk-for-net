using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.HBase.Client.Entity.Annotations
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class RowKeyAttribute : AbstractColumnSchemaAttribute
    {
        public RowKeyAttribute()
            : base("rowkey")
        {
        }
    }

    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class SingleColumnQualifierSchemaAttribute : AbstractColumnSchemaAttribute
    {
        public SingleColumnQualifierSchemaAttribute(string columnFamily, string columnQualifier)
            : base(columnFamily, columnQualifier)
        {
        }
    }

    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class DictionaryKeyAsColumnQualifierColumnSchemaAttribute : AbstractColumnSchemaAttribute
    {
        public DictionaryKeyAsColumnQualifierColumnSchemaAttribute(string columnFamily)
            : base(columnFamily)
        {
        }

        public const string TIMESTAMPPREFIX = "__ts__";
        public const string SYSTEMRESERVEDTIMESTAMPQUALIFIER = "__SyStTmReSeRvEd__DiCtIoNaRy__TiMeStAmP__cOlUmNqUaLiFiEr__";
        public bool VersionControl { get; set; }
    }
}
