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

namespace Microsoft.HBase.Client.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using Microsoft.HBase.Client.Filters;
    using Microsoft.HBase.Client.Tests.Utilities;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using org.apache.hadoop.hbase.rest.protobuf.generated;
    using System.Net;
    using Practices.EnterpriseLibrary.TransientFaultHandling;    // ReSharper disable InconsistentNaming
    using System.Threading.Tasks;
    using LoadBalancing;
    [TestClass]
    public class CellOperationsTests : DisposableContextSpecification
    {
        private const string TableNamePrefix = "celltest";

        private const string ColumnFamilyName1 = "first";
        private const string ColumnFamilyName2 = "second";
        private const string ColumnNameA = "a";
        private const string ColumnNameB = "b";

        private static bool _arrangementCompleted;
        private static readonly List<FilterTestRecord> _allExpectedRecords = new List<FilterTestRecord>();
        private static ClusterCredentials _credentials;
        private static readonly Encoding _encoding = Encoding.UTF8;
        private static string _tableName;
        private static TableSchema _tableSchema;

        protected override void Context()
        {
            if (!_arrangementCompleted)
            {
                var client = GetClient();
                // ensure tables from previous tests are cleaned up
                TableList tables = client.ListTables();
                foreach (string name in tables.name)
                {
                    string pinnedName = name;
                    if (name.StartsWith(TableNamePrefix, StringComparison.Ordinal))
                    {
                        client.DeleteTable(pinnedName);
                    }
                }

                AddTable();
                _arrangementCompleted = true;
            }
        }

        private HBaseClient GetClient()
        {
            _credentials = ClusterCredentialsFactory.CreateFromFile(@".\credentials.txt");
            var options = RequestOptions.GetDefaultOptions();
            options.RetryPolicy = RetryPolicy.NoRetry;

            var client = new HBaseClient(_credentials, options);
            #region VNet
            //options.TimeoutMillis = 30000;
            //options.KeepAlive = false;
            //options.Port = 8090;
            //options.AlternativeEndpoint = "/";
            //var client = new HBaseClient(null, options, new LoadBalancerRoundRobin(new List<string> { "ip address" }));
            #endregion

            return client;
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_DeleteCells_With_TimeStamp_I_can_add_with_higher_timestamp()
        {
            var client = GetClient();

            client.StoreCells(_tableName, CreateCellSet(GetCellSet("1", "c1", "1A", 10)));
            client.StoreCells(_tableName, CreateCellSet(GetCellSet("1", "c2", "1A", 10)));

            client.DeleteCells(_tableName, "1", ColumnFamilyName1, 10);

            try
            {
                client.GetCells(_tableName, "1");
                Assert.Fail("Expected to throw an exception as the row is deleted");
            }
            catch(Exception ex)
            {
                if (ex is AggregateException)
                {
                    ((ex.InnerException as WebException).Response as HttpWebResponse).StatusCode.ShouldEqual(HttpStatusCode.NotFound);
                }
            }

            client.StoreCells(_tableName, CreateCellSet(GetCellSet("1", "c1", "1A", 11)));

            var retrievedCells = client.GetCells(_tableName, "1");

            retrievedCells.rows.Count.ShouldEqual(1);
            retrievedCells.rows[0].values[0].timestamp.ShouldEqual(11);
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_DeleteCells_With_TimeStamp_I_cannot_add_with_lower_timestamp()
        {
            var client = GetClient();

            client.StoreCells(_tableName, CreateCellSet(GetCellSet("2", "c1", "1A", 10)));
            client.StoreCells(_tableName, CreateCellSet(GetCellSet("2", "c2", "1A", 10)));

            client.DeleteCells(_tableName, "2", ColumnFamilyName1, 10);

            try
            {
                client.GetCells(_tableName, "2");
                Assert.Fail("Expected to throw an exception as the row is deleted");
            }
            catch (Exception ex)
            {
                if (ex is AggregateException)
                {
                    ((ex.InnerException as WebException).Response as HttpWebResponse).StatusCode.ShouldEqual(HttpStatusCode.NotFound);
                }
            }

            client.StoreCells(_tableName, CreateCellSet(GetCellSet("2", "c1", "1A", 9)));

            try
            {
                client.GetCells(_tableName, "2");
                Assert.Fail("Expected to throw an exception as the row cannot be added with lower timestamp");
            }
            catch (Exception ex)
            {
                if (ex is AggregateException)
                {
                    ((ex.InnerException as WebException).Response as HttpWebResponse).StatusCode.ShouldEqual(HttpStatusCode.NotFound);
                }
            }
        }


        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public async Task When_I_CheckAndDeleteCells_With_TimeStamp_I_cannot_add_with_lower_timestamp_than_hbaseserver()
        {
            var client = GetClient();

            client.StoreCells(_tableName, CreateCellSet(GetCellSet("3", "c1", "1A", 10)));
            client.StoreCells(_tableName, CreateCellSet(GetCellSet("3", "c2", "1A", 10)));

            bool deleted = await client.CheckAndDeleteAsync(_tableName, GetCell("3","c1","1A",10));

            deleted.ShouldEqual(true);

            var retrievedCells = client.GetCells(_tableName, "3");
            // Deletes in the Cell c1 so c2 should be present.
            retrievedCells.rows[0].values.Count.ShouldEqual(1);

            deleted = await client.CheckAndDeleteAsync(_tableName, GetCell("3", "c2", "1A", 10));
            deleted.ShouldEqual(true);

            try
            {
                // All  cells are deleted so this should fail
                retrievedCells = client.GetCells(_tableName, "3");
                throw new AssertFailedException("expecting Get '3' to fail as all cells are removed");
            }
            catch (Exception ex)
            {
                if (ex is AggregateException)
                {
                    ((ex.InnerException as WebException).Response as HttpWebResponse).StatusCode.ShouldEqual(HttpStatusCode.NotFound);
                }
            }

            client.StoreCells(_tableName, CreateCellSet(GetCellSet("3", "c1", "1B", 11)));

            try
            {
                retrievedCells = client.GetCells(_tableName, "3");
                throw new AssertFailedException("Expected to throw an exception as the row cannot be added with lower timestamp than servers timestamp");
            }
            catch (Exception ex)
            {
                if (ex is AggregateException)
                {
                    ((ex.InnerException as WebException).Response as HttpWebResponse).StatusCode.ShouldEqual(HttpStatusCode.NotFound);
                }
            }
        }


        // These need fixes from https://issues.apache.org/jira/browse/HBASE-15323
        [TestMethod]
        public async Task When_I_CheckAndDeleteCells_With_TimeStamp_And_Cells_To_Delete_I_Can_add_with_higher_timestamp()
        {
            var client = GetClient();

            client.StoreCells(_tableName, CreateCellSet(GetCellSet("3", "c1", "1A", 10)));
            client.StoreCells(_tableName, CreateCellSet(GetCellSet("3", "c2", "1A", 10)));

            // Deletes all the ColumnFamily with timestamp less than 10
            CellSet.Row rowToDelete = new CellSet.Row() { key = Encoding.UTF8.GetBytes("3") };
            //rowToDelete.values.Add(GetCell(rowToDelete.key, column = BuildCellColumn(ColumnFamilyName1, "c1"), data= "1A", timestamp = 10 });
            rowToDelete.values.Add(GetCell("3", "c1", "1A", 10));
            rowToDelete.values.Add(GetCell("3", "c2", "1A", 10));
            bool deleted = await client.CheckAndDeleteAsync(_tableName, GetCell("3", "c1", "1A", 10), rowToDelete);

            deleted.ShouldEqual(true);

            CellSet retrievedCells;
            try
            {
                // All  cells are deleted so this should fail
                retrievedCells = client.GetCells(_tableName, "3");
                throw new AssertFailedException("expecting Get '3' to fail as all cells are removed");
            }
            catch (Exception ex)
            {
                if (ex is AggregateException)
                {
                    ((ex.InnerException as WebException).Response as HttpWebResponse).StatusCode.ShouldEqual(HttpStatusCode.NotFound);
                }
                else
                {
                    throw ex;
                }
            }

            client.StoreCells(_tableName, CreateCellSet(GetCellSet("3", "c1", "1B", 11)));

            try
            {
                retrievedCells = client.GetCells(_tableName, "3");
                retrievedCells.rows[0].values.Count.ShouldEqual(1);
                Encoding.UTF8.GetString(retrievedCells.rows[0].values[0].column).ShouldBeEqualOrdinalIgnoreCase("c1");
            }
            catch (Exception ex)
            {
                if (ex is AggregateException)
                {
                    ((ex.InnerException as WebException).Response as HttpWebResponse).StatusCode.ShouldEqual(HttpStatusCode.NotFound);
                }
            }
        }

        // These need fixes from https://issues.apache.org/jira/browse/HBASE-15323
        [TestMethod]
        public async Task When_I_CheckAndDeleteCells_With_ColumnFamily_Deletes_All_cells()
        {
            var client = GetClient();

            client.StoreCells(_tableName, CreateCellSet(GetCellSet("3", "c1", "1A", 10)));
            client.StoreCells(_tableName, CreateCellSet(GetCellSet("3", "c2", "1A", 10)));

            // Deletes all the ColumnFamily with timestamp less than 10
            CellSet.Row rowToDelete = new CellSet.Row() { key = Encoding.UTF8.GetBytes("3") };
            rowToDelete.values.Add(new Cell() { row = rowToDelete.key, column = Encoding.UTF8.GetBytes(ColumnFamilyName1), timestamp = 10 });
            bool deleted = await client.CheckAndDeleteAsync(_tableName, GetCell("3", "c1", "1A", 10), rowToDelete);

            deleted.ShouldEqual(true);

            CellSet retrievedCells;
            try
            {
                // All  cells are deleted so this should fail
                retrievedCells = client.GetCells(_tableName, "3");
                throw new AssertFailedException("expecting Get '3' to fail as all cells are removed");
            }
            catch (Exception ex)
            {
                if (ex is AggregateException)
                {
                    ((ex.InnerException as WebException).Response as HttpWebResponse).StatusCode.ShouldEqual(HttpStatusCode.NotFound);
                }
            }

            client.StoreCells(_tableName, CreateCellSet(GetCellSet("3", "c1", "1B", 11)));

            try
            {
                retrievedCells = client.GetCells(_tableName, "3");
                retrievedCells.rows[0].values.Count.ShouldEqual(1);
            }
            catch (Exception ex)
            {
                if (ex is AggregateException)
                {
                    ((ex.InnerException as WebException).Response as HttpWebResponse).StatusCode.ShouldEqual(HttpStatusCode.NotFound);
                }
            }
        }

        private CellSet CreateCellSet(params CellSet.Row[] rows)
        {
            CellSet cellSet = new CellSet();
            cellSet.rows.AddRange(rows);
            return cellSet;
        }

        private Cell GetCell(string key, string columnName, string value = null, long timestamp = 0)
        {
            Cell cell = new Cell() { column = BuildCellColumn(ColumnFamilyName1, columnName) , row = Encoding.UTF8.GetBytes(key) };
            if (value != null)
            {
                cell.data = Encoding.UTF8.GetBytes(value);
            }
            if (timestamp > 0)
            {
                cell.timestamp = timestamp;
            }
            return cell;
        }
        private CellSet.Row GetCellSet(string key, string columnName, string value, long timestamp)
        {
            CellSet.Row row = new CellSet.Row() { key = Encoding.UTF8.GetBytes(key) };
            Cell c1 = new Cell() { column = BuildCellColumn(ColumnFamilyName1, columnName ), row = row.key };
            if (value != null)
            {
                c1.data = Encoding.UTF8.GetBytes(value);
            }

            if (timestamp > 0)
            {
                c1.timestamp = timestamp;
            }
            row.values.Add(c1);
            return row;
        }

        private Byte[] BuildCellColumn(string columnFamilyName, string columnName)
        {
            return _encoding.GetBytes(string.Format(CultureInfo.InvariantCulture, "{0}:{1}", columnFamilyName, columnName));
        }

        private string ExtractColumnName(Byte[] cellColumn)
        {
            string qualifiedColumnName = _encoding.GetString(cellColumn);
            string[] parts = qualifiedColumnName.Split(new[] { ':' }, 2);
            return parts[1];
        }

        private void AddTable()
        {
            // add a table specific to this test
            var client = GetClient();
            _tableName = TableNamePrefix + Guid.NewGuid().ToString("N");
            _tableSchema = new TableSchema { name = _tableName };
            _tableSchema.columns.Add(new ColumnSchema { name = ColumnFamilyName1 });
            _tableSchema.columns.Add(new ColumnSchema { name = ColumnFamilyName2 });

            client.CreateTable(_tableSchema);
        }
    }
}
