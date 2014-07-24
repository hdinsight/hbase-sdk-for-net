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
    using System.Linq;
    using System.Text;
    using Microsoft.HBase.Client.Tests.Utilities;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using org.apache.hadoop.hbase.rest.protobuf.generated;

    [TestClass]
    public class HBaseClientTests : DisposableContextSpecification
    {
        private const string TestTablePrefix = "marlintest";
        private ClusterCredentials _credentials;
        private readonly Random _random = new Random();

        private string _testTableName;
        private TableSchema _testTableSchema;

        protected override void Context()
        {
            _credentials = ClusterCredentialsFactory.CreateFromFile(@".\credentials.txt");
            var client = new HBaseClient(_credentials);

            // ensure tables from previous tests are cleaned up
            TableList tables = client.ListTables();
            foreach (string name in tables.name)
            {
                if (name.StartsWith(TestTablePrefix, StringComparison.Ordinal))
                {
                    client.DeleteTable(name);
                }
            }

            // add a table specific to this test
            _testTableName = TestTablePrefix + _random.Next(10000);
            _testTableSchema = new TableSchema();
            _testTableSchema.name = _testTableName;
            _testTableSchema.columns.Add(new ColumnSchema { name = "d" });

            client.CreateTable(_testTableSchema);
        }

        [TestCleanup]
        public override void TestCleanup()
        {
            // moved table cleanup to Context
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void TestFullScan()
        {
            var client = new HBaseClient(_credentials);

            StoreTestData(client);

            // full range scan
            var scanSettings = new Scanner { batch = 10 };
            ScannerInformation scannerInfo = client.CreateScanner(_testTableName, scanSettings);

            CellSet next;
            var expectedSet = new HashSet<int>(Enumerable.Range(0, 100));
            while ((next = client.ScannerGetNext(scannerInfo)) != null)
            {
                Assert.AreEqual(10, next.rows.Count);
                foreach (CellSet.Row row in next.rows)
                {
                    int k = BitConverter.ToInt32(row.key, 0);
                    expectedSet.Remove(k);
                }
            }
            Assert.AreEqual(0, expectedSet.Count, "The expected set wasn't empty! Items left {0}!", string.Join(",", expectedSet));
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void TestGetStorageClusterStatus()
        {
            var client = new HBaseClient(_credentials);
            StorageClusterStatus status = client.GetStorageClusterStatus();
            // TODO not really a good test
            Assert.IsTrue(status.requests >= 0, "number of requests is negative");
            Assert.IsTrue(status.liveNodes.Count >= 1, "number of live nodes is zero or negative");
            Assert.IsTrue(status.liveNodes[0].requests >= 0, "number of requests to the first node is negative");
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void TestGetVersion()
        {
            var client = new HBaseClient(_credentials);
            org.apache.hadoop.hbase.rest.protobuf.generated.Version version = client.GetVersion();
            version.jvmVersion.ShouldNotBeNullOrEmpty();
            version.jerseyVersion.ShouldNotBeNullOrEmpty();
            version.osVersion.ShouldNotBeNullOrEmpty();
            version.restVersion.ShouldNotBeNullOrEmpty();
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void TestListTables()
        {
            var client = new HBaseClient(_credentials);
            TableList tables = client.ListTables();
            List<string> testtables = tables.name.Where(item => item.StartsWith("marlintest", StringComparison.Ordinal)).ToList();
            Assert.AreEqual(1, testtables.Count);
            Assert.AreEqual(_testTableName, testtables[0]);
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void TestScannerCreation()
        {
            var client = new HBaseClient(_credentials);
            var batchSetting = new Scanner { batch = 2 };
            ScannerInformation scannerInfo = client.CreateScanner(_testTableName, batchSetting);
            Assert.AreEqual(_testTableName, scannerInfo.TableName);
            Assert.IsTrue(
                scannerInfo.Location.Authority.StartsWith("headnode", StringComparison.Ordinal),
                "returned location didn't start with \"headnode\", it was: {0}",
                scannerInfo.Location);
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void TestStoreSingleCell()
        {
            const string testKey = "content";
            const string testValue = "the force is strong in this column";
            var client = new HBaseClient(_credentials);
            var set = new CellSet();
            var row = new CellSet.Row { key = Encoding.UTF8.GetBytes(testKey) };
            set.rows.Add(row);

            var value = new Cell { column = Encoding.UTF8.GetBytes("d:starwars"), data = Encoding.UTF8.GetBytes(testValue) };
            row.values.Add(value);
            client.StoreCells(_testTableName, set);

            CellSet cells = client.GetCells(_testTableName, testKey);
            Assert.AreEqual(1, cells.rows.Count);
            Assert.AreEqual(1, cells.rows[0].values.Count);
            Assert.AreEqual(testValue, Encoding.UTF8.GetString(cells.rows[0].values[0].data));
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void TestSubsetScan()
        {
            var client = new HBaseClient(_credentials);
            const int startRow = 15;
            const int endRow = 15 + 13;
            StoreTestData(client);

            // subset range scan
            var scanSettings = new Scanner { batch = 10, startRow = BitConverter.GetBytes(startRow), endRow = BitConverter.GetBytes(endRow) };
            ScannerInformation scannerInfo = client.CreateScanner(_testTableName, scanSettings);

            CellSet next;
            var expectedSet = new HashSet<int>(Enumerable.Range(startRow, endRow - startRow));
            while ((next = client.ScannerGetNext(scannerInfo)) != null)
            {
                foreach (CellSet.Row row in next.rows)
                {
                    int k = BitConverter.ToInt32(row.key, 0);
                    expectedSet.Remove(k);
                }
            }
            Assert.AreEqual(0, expectedSet.Count, "The expected set wasn't empty! Items left {0}!", string.Join(",", expectedSet));
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void TestTableSchema()
        {
            var client = new HBaseClient(_credentials);
            TableSchema schema = client.GetTableSchema(_testTableName);
            Assert.AreEqual(_testTableName, schema.name);
            Assert.AreEqual(_testTableSchema.columns.Count, schema.columns.Count);
            Assert.AreEqual(_testTableSchema.columns[0].name, schema.columns[0].name);
        }

        private void StoreTestData(HBaseClient hBaseClient)
        {
            // we are going to insert the keys 0 to 100 and then do some range queries on that
            const string testValue = "the force is strong in this column";
            var set = new CellSet();
            for (int i = 0; i < 100; i++)
            {
                var row = new CellSet.Row { key = BitConverter.GetBytes(i) };
                var value = new Cell { column = Encoding.UTF8.GetBytes("d:starwars"), data = Encoding.UTF8.GetBytes(testValue) };
                row.values.Add(value);
                set.rows.Add(row);
            }

            hBaseClient.StoreCells(_testTableName, set);
        }
    }
}
