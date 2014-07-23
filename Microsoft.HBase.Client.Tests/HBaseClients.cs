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
    using Microsoft.HBase.Client;
    using Microsoft.HBase.Client.Tests.Utilities;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using org.apache.hadoop.hbase.rest.protobuf.generated;

    [TestClass]
    public class HBaseClients : DisposableContextSpecification
    {
        private readonly Random _random = new Random();
        private ClusterCredentials _credentials;

        private TableSchema _testTableSchema;
        private string _testTableName;

        protected override void Context()
        {
            _credentials = ClusterCredentialsFactory.CreateFromFile(@"..\..\credentials.txt");
            _testTableName = "marlintest" + _random.Next(10000);
            _testTableSchema = new TableSchema();
            _testTableSchema.name = _testTableName;
            _testTableSchema.columns.Add(new ColumnSchema() { name = "d" });

            var marlin = new HBaseClient(_credentials);
            marlin.CreateTable(_testTableSchema);
        }

        [TestCleanup]
        public override void TestCleanup()
        {
            var marlin = new HBaseClient(_credentials);
            var tables = marlin.ListTables();
            foreach (var name in tables.name)
            {
                if (name.StartsWith("marlintest"))
                {
                    marlin.DeleteTable(name);
                }
            }
        }

        [TestMethod]
        public void TestScannerCreation()
        {
            var marlin = new HBaseClient(_credentials);
            var batchSetting = new Scanner() { batch = 2 };
            var scannerInfo = marlin.CreateScanner(_testTableName, batchSetting);
            Assert.AreEqual(_testTableName, scannerInfo.TableName);
            Assert.IsTrue(scannerInfo.Location.Authority.StartsWith("headnode"),
                "returned location didn't start with \"headnode\", it was: {0}", scannerInfo.Location);
        }

        [TestMethod]
        public void TestFullScan()
        {
            var marlin = new HBaseClient(_credentials);

            StoreTestData(marlin);

            // full range scan
            var scanSettings = new Scanner() { batch = 10 };
            var scannerInfo = marlin.CreateScanner(_testTableName, scanSettings);

            CellSet next = null;
            var expectedSet = new HashSet<int>(Enumerable.Range(0, 100));
            while ((next = marlin.ScannerGetNext(scannerInfo)) != null)
            {
                Assert.AreEqual(10, next.rows.Count);
                foreach (var row in next.rows)
                {
                    int k = BitConverter.ToInt32(row.key, 0);
                    expectedSet.Remove(k);
                }
            }
            Assert.AreEqual(0, expectedSet.Count, "The expected set wasn't empty! Items left {0}!", string.Join(",", expectedSet));
        }

        [TestMethod]
        public void TestSubsetScan()
        {
            var marlin = new HBaseClient(_credentials);
            int startRow = 15;
            int endRow = 15 + 13;
            StoreTestData(marlin);

            // subset range scan
            var scanSettings = new Scanner()
            {
                batch = 10,
                startRow = BitConverter.GetBytes(startRow),
                endRow = BitConverter.GetBytes(endRow)
            };
            var scannerInfo = marlin.CreateScanner(_testTableName, scanSettings);

            CellSet next = null;
            var expectedSet = new HashSet<int>(Enumerable.Range(startRow, endRow - startRow));
            while ((next = marlin.ScannerGetNext(scannerInfo)) != null)
            {
                foreach (var row in next.rows)
                {
                    int k = BitConverter.ToInt32(row.key, 0);
                    expectedSet.Remove(k);
                }
            }
            Assert.AreEqual(0, expectedSet.Count, "The expected set wasn't empty! Items left {0}!", string.Join(",", expectedSet));
        }

        [TestMethod]
        public void TestGetVersion()
        {
            var marlin = new HBaseClient(_credentials);
            var version = marlin.GetVersion();
            version.jvmVersion.ShouldNotBeNullOrEmpty();
            version.jerseyVersion.ShouldNotBeNullOrEmpty();
            version.osVersion.ShouldNotBeNullOrEmpty();
            version.restVersion.ShouldNotBeNullOrEmpty();
        }

        [TestMethod]
        public void TestListTables()
        {
            var marlin = new HBaseClient(_credentials);
            var tables = marlin.ListTables();
            var testtables = tables.name.Where(item => item.StartsWith("marlintest")).ToList();
            Assert.AreEqual(1, testtables.Count);
            Assert.AreEqual(_testTableName, testtables[0]);
        }

        [TestMethod]
        public void TestTableSchema()
        {
            var marlin = new HBaseClient(_credentials);
            var schema = marlin.GetTableSchema(_testTableName);
            Assert.AreEqual(_testTableName, schema.name);
            Assert.AreEqual(_testTableSchema.columns.Count, schema.columns.Count);
            Assert.AreEqual(_testTableSchema.columns[0].name, schema.columns[0].name);
        }

        [TestMethod]
        public void TestGetStorageClusterStatus()
        {
            var marlin = new HBaseClient(_credentials);
            var status = marlin.GetStorageClusterStatus();
            // TODO not really a good test
            Assert.IsTrue( status.requests >= 0, "number of requests is negative");
            Assert.IsTrue(status.liveNodes.Count >= 1, "number of live nodes is zero or negative");
            Assert.IsTrue(status.liveNodes[0].requests >= 0, "number of requests to the first node is negative");
        }

        [TestMethod]
        public void TestStoreSingleCell()
        {
            var testKey = "content";
            var testValue = "the force is strong in this column";
            var marlin = new HBaseClient(_credentials);
            CellSet set = new CellSet();
            CellSet.Row row = new CellSet.Row() { key = Encoding.UTF8.GetBytes(testKey) };
            set.rows.Add(row);

            var value = new Cell() { column = Encoding.UTF8.GetBytes("d:starwars"), data = Encoding.UTF8.GetBytes(testValue) };
            row.values.Add(value);
            marlin.StoreCells(_testTableName, set);

            var cells = marlin.GetCells(_testTableName, testKey);
            Assert.AreEqual(1, cells.rows.Count);
            Assert.AreEqual(1, cells.rows[0].values.Count);
            Assert.AreEqual(testValue, Encoding.UTF8.GetString(cells.rows[0].values[0].data));
        }

        private void StoreTestData(HBaseClient hBaseClient)
        {
            // we are going to insert the keys 0 to 100 and then do some range queries on that
            var testValue = "the force is strong in this column";
            CellSet set = new CellSet();
            for (int i = 0; i < 100; i++)
            {
                CellSet.Row row = new CellSet.Row() { key = BitConverter.GetBytes(i) };
                var value = new Cell()
                {
                    column = Encoding.UTF8.GetBytes("d:starwars"),
                    data = Encoding.UTF8.GetBytes(testValue)
                };
                row.values.Add(value);
                set.rows.Add(row);
            }

            hBaseClient.StoreCells(_testTableName, set);
        }
    }
}
