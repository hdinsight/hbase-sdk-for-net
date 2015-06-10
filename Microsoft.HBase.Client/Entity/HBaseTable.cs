using Microsoft.HBase.Client.Entity.Annotations;
using org.apache.hadoop.hbase.rest.protobuf.generated;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.HBase.Client.Entity
{
    public class HBaseRow
    {
    }


    public class HBaseTable<T> : IQueryable<T>, IEnumerable<T>, IHBaseTableOperations<T>
        where T : HBaseRow, new()
    {
        public string TableName { get; set; }
        private HBaseClient client;
        public HBaseTable(string tableName, ClusterCredentials credentials)
        {
            if (string.IsNullOrEmpty(tableName))
                throw new ArgumentNullException("tableName");

            if (credentials == null)
                throw new ArgumentNullException("credentials");

            TableName = tableName;
            client = new HBaseClient(credentials);
            UpdateRowInfo();
        }

        #region Private

        private Tuple<PropertyInfo, RowKeyAttribute> rowKeyInfo = null;
        private Dictionary<string, Dictionary<string, Tuple<PropertyInfo, SingleColumnQualifierSchemaAttribute>>> singleColumnSchemaInfo = new Dictionary<string, Dictionary<string, Tuple<PropertyInfo, SingleColumnQualifierSchemaAttribute>>>();
        private Dictionary<string, Tuple<PropertyInfo, DictionaryKeyAsColumnQualifierColumnSchemaAttribute>> groupedColumnSchemaInfo = new Dictionary<string, Tuple<PropertyInfo, DictionaryKeyAsColumnQualifierColumnSchemaAttribute>>();

        private void UpdateRowInfo()
        {
            // get key
            // get column family and column
            // prepare get/set property

            var properties = typeof(T).GetProperties();

            foreach (var p in properties)
            {
                var rowKey = p.GetCustomAttribute<RowKeyAttribute>();
                var singleSchema = p.GetCustomAttribute<SingleColumnQualifierSchemaAttribute>();
                var dictSchema = p.GetCustomAttribute<DictionaryKeyAsColumnQualifierColumnSchemaAttribute>();

                if (rowKey != null)
                {
                    if (rowKeyInfo != null)
                        throw new ArgumentException("Only one property with RowKey attribute is allowed");

                    if (singleSchema != null || dictSchema != null)
                        throw new ArgumentException("One property can only be either RowKey or ColumnQualifier");

                    if (!Bytes.IsSupportedType(p.PropertyType))
                        throw new ArgumentOutOfRangeException("RowKey type not supported");

                    rowKeyInfo = new Tuple<PropertyInfo, RowKeyAttribute>(p, rowKey);
                }
                else if (singleSchema != null)
                {
                    if (dictSchema != null)
                        throw new ArgumentException("One property can only be either SingleColumnQualifier or DictionaryKeyAsColumnQualifier");

                    if (groupedColumnSchemaInfo.ContainsKey(singleSchema.ColumnFamily))
                        throw new ArgumentException("A ColumnFamily can only be SingleColumnQualifier or DictionaryKeyAsColumnQualifier");

                    Dictionary<string, Tuple<PropertyInfo, SingleColumnQualifierSchemaAttribute>> cfs = null;

                    if (!singleColumnSchemaInfo.TryGetValue(singleSchema.ColumnFamily, out cfs))
                    {
                        cfs = new Dictionary<string, Tuple<PropertyInfo, SingleColumnQualifierSchemaAttribute>>();
                        singleColumnSchemaInfo.Add(singleSchema.ColumnFamily, cfs);
                    }

                    if (cfs.ContainsKey(singleSchema.ColumnQualifier))
                        throw new ArgumentException("Duplicate column");

                    if (singleSchema.ColumnEncoding != ColumnEncodingOption.UDTConvertToJson && !Bytes.IsSupportedType(p.PropertyType))
                        throw new ArgumentOutOfRangeException("Column type not supported");

                    cfs.Add(singleSchema.ColumnQualifier, new Tuple<PropertyInfo, SingleColumnQualifierSchemaAttribute>(p, singleSchema));

                }
                else if (dictSchema != null)
                {
                    if (singleColumnSchemaInfo.ContainsKey(dictSchema.ColumnFamily))
                        throw new ArgumentException("A ColumnFamily can only be fixed or grouped");
                    if (groupedColumnSchemaInfo.ContainsKey(dictSchema.ColumnFamily))
                        throw new ArgumentException("Duplicate ColumnFamily");
                    var pType = p.PropertyType;
                    if (!pType.IsGenericType || !(pType.GetInterface(typeof(IDictionary<,>).Name) != null || pType.Name == "IDictionary`2"))
                        throw new ArgumentException("Not a IDictionary generic type");
                    var args = pType.GetGenericArguments();
                    if (args.Length != 2)
                        throw new ArgumentException("Not a IDcitionary generic type");

                    if (!Bytes.IsSupportedType(args[0]))
                        throw new ArgumentException("Key type is not supported");
                    if (args[0] != typeof(string))
                        throw new ArgumentException("Key type should always be string");
                    if (dictSchema.ColumnEncoding != ColumnEncodingOption.UDTConvertToJson && !Bytes.IsSupportedType(args[1]))
                        throw new ArgumentException("Value type is not supported");

                    groupedColumnSchemaInfo.Add(dictSchema.ColumnFamily, new Tuple<PropertyInfo, DictionaryKeyAsColumnQualifierColumnSchemaAttribute>(p, dictSchema));
                }
                else
                { }
            }
        }

        public CellSet.Row SerializeToRow(T obj)
        {
            var row = (HBaseRow)obj;

            var r = new CellSet.Row();
            r.key = rowKeyInfo.Item2.GetColumnBytes(rowKeyInfo.Item1.GetValue(row));
            foreach (var kv in singleColumnSchemaInfo)
            {
                foreach (var vn in kv.Value)
                {
                    r.values.Add(new Cell { column = Bytes.ToBytes(kv.Key + ":" + vn.Key), data = vn.Value.Item2.GetColumnBytes(vn.Value.Item1.GetValue(row)) });
                }
            }

            foreach (var kv in groupedColumnSchemaInfo)
            {
                var dict = kv.Value.Item1.GetValue(row) as IDictionary;
                if (dict == null)
                    continue;

                var timeStamp = DictionaryKeyAsColumnQualifierColumnSchemaAttribute.TIMESTAMPPREFIX + Bytes.ToLong(kv.Value.Item2.VersionControl ? Bytes.ToBytes(DateTime.UtcNow) : Bytes.ToBytes(0L)).ToString("X16");
                foreach (string key in dict.Keys)
                {
                    var k = kv.Value.Item2.VersionControl ? kv.Key + ":" + timeStamp + ":" + key : kv.Key + ":" + key;
                    r.values.Add(new Cell { column = Bytes.ToBytes(k), data = kv.Value.Item2.GetColumnBytes(dict[key]) });
                }

                if (kv.Value.Item2.VersionControl)
                {
                    r.values.Add(new Cell { column = Bytes.ToBytes(kv.Key + ":" + DictionaryKeyAsColumnQualifierColumnSchemaAttribute.SYSTEMRESERVEDTIMESTAMPQUALIFIER), data = Bytes.ToBytes(timeStamp) });
                }
            }

            return r;
        }

        public T DeserializeToObject(CellSet.Row row)
        {
            var t = new T();

            rowKeyInfo.Item1.SetValue(t, rowKeyInfo.Item2.GetColumnObject(row.key, rowKeyInfo.Item1.PropertyType));

            row.values.Select(p =>
            {
                var key = Bytes.ToString(p.column);
                var kIndex = key.IndexOf(':');
                var cf = key.Substring(0, kIndex);
                var c = key.Substring(kIndex + 1);

                return new { cf = cf, c = c, v = p.data };
            }).GroupBy(p => p.cf).ToList().ForEach(p =>
            {
                var cf = p.Key;
                Dictionary<string, Tuple<PropertyInfo, SingleColumnQualifierSchemaAttribute>> cfs = null;
                Tuple<PropertyInfo, DictionaryKeyAsColumnQualifierColumnSchemaAttribute> groupedPi = null;
                if (singleColumnSchemaInfo.TryGetValue(cf, out cfs))
                {
                    foreach (var c in p)
                    {
                        Tuple<PropertyInfo, SingleColumnQualifierSchemaAttribute> pi = null;
                        if (!cfs.TryGetValue(c.c, out pi))
                        {
                            // cannot find the property
                        }

                        pi.Item1.SetValue(t, pi.Item2.GetColumnObject(c.v, pi.Item1.PropertyType));
                    }
                }
                else if (groupedColumnSchemaInfo.TryGetValue(cf, out groupedPi))
                {
                    var valuetype = groupedPi.Item1.PropertyType.GetGenericArguments()[1];
                    var dict = Activator.CreateInstance(groupedPi.Item1.PropertyType) as IDictionary;


                    if (groupedPi.Item2.VersionControl)
                    {
                        var ts = p.Where(q => q.c == DictionaryKeyAsColumnQualifierColumnSchemaAttribute.SYSTEMRESERVEDTIMESTAMPQUALIFIER).FirstOrDefault();
                        var tsString = ts != null ? Bytes.ToString(ts.v) : string.Empty;
                        if (string.IsNullOrEmpty(tsString))
                            throw new ArgumentException();

                        foreach (var kv in p.Where(q => q.c.StartsWith(tsString)).ToDictionary(q => q.c.Substring(tsString.Length + 1), q => groupedPi.Item2.GetColumnObject(q.v, valuetype)))
                        {
                            dict.Add(kv.Key, kv.Value);
                        }
                    }
                    else
                    {
                        foreach (var kv in p.ToDictionary(q => q.c, q => groupedPi.Item2.GetColumnObject(q.v, valuetype)))
                        {
                            dict.Add(kv.Key, kv.Value);
                        }
                    }

                    groupedPi.Item1.SetValue(t, dict);
                }
                else
                {
                    // cannot find the column family
                }
            });

            return t;
        }
        #endregion


        public IEnumerator<T> GetEnumerator()
        {
            throw new NotImplementedException();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public Type ElementType
        {
            get { return typeof(T); }
        }

        public System.Linq.Expressions.Expression Expression
        {
            get { throw new NotImplementedException(); }
        }

        public IQueryProvider Provider
        {
            get { throw new NotImplementedException(); }
        }

        public async Task<bool> CreateTableAsync(bool failIfExist, bool updateSchemaIfExist)
        {

            var tableSchema = new TableSchema();
            tableSchema.name = TableName;
            tableSchema.columns.AddRange(singleColumnSchemaInfo.Select(p => p.Key).Select(p => new ColumnSchema { name = p }));
            tableSchema.columns.AddRange(groupedColumnSchemaInfo.Select(p => p.Key).Select(p => new ColumnSchema { name = p }));
            var tables = await client.ListTablesAsync();
            if (tables.name.Contains(TableName))
            {
                if (failIfExist)
                    return false;

                var existSchema = await client.GetTableSchemaAsync(TableName);
                var missingCF = existSchema.columns.Where(p => tableSchema.columns.SingleOrDefault(q => q.name == p.name) == null).ToArray();
                if (updateSchemaIfExist)
                {
                    if (missingCF.Length != 0)
                    {
                        tableSchema.columns.AddRange(missingCF);
                        await client.ModifyTableSchemaAsync(TableName, tableSchema);
                    }

                    return true;
                }
                else
                {
                    return missingCF.Length == 0;
                }
            }
            else
            {
                return await client.CreateTableAsync(tableSchema);
            }
        }

        public async Task DeleteTableAsync()
        {
            await client.DeleteTableAsync(TableName);
        }

        public async Task InsertOrUpdateAsync(T entity)
        {
            var cellset = new CellSet();
            cellset.rows.Add(SerializeToRow(entity));

            await client.StoreCellsAsync(TableName, cellset);
        }

        public async Task DeleteAsync(T entity)
        {
            throw new NotImplementedException();
        }

        public async Task InsertOrUpdateRangeAsync(IEnumerable<T> entities)
        {
            var cellset = new CellSet();
            cellset.rows.AddRange(entities.Select(p => SerializeToRow(p)));

            await client.StoreCellsAsync(TableName, cellset);
        }

        public async Task DeleteRangeAsync(IEnumerable<T> entities)
        {
            throw new NotImplementedException();
        }

        public async Task<IEnumerable<T>> RetriveAllData()
        {
            List<T> data = new List<T>();
            var scanner = await client.CreateScannerAsync(TableName, new Scanner { batch = 1000 });
            var cellset = default(CellSet);
            while((cellset = await client.ScannerGetNextAsync(scanner)) != null)
            {
                foreach(var r in cellset.rows)
                {
                    data.Add(DeserializeToObject(r));
                }
            }

            await client.DeleteScannerAsync(TableName, scanner);

            return data;
        }
    }
}
