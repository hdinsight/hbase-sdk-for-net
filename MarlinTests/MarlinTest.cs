namespace MarlinTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using Marlin;
    using NUnit.Framework;
    using org.apache.hadoop.hbase.rest.protobuf.generated;

    [TestFixture]
    public class MarlinTest
    {
        private readonly Random _random = new Random();
        private ClusterCredentials _credentials;

        private TableSchema _testTableSchema;
        private string _testTableName;

        [SetUp]
        public void SetUp()
        {
            _credentials = ClusterCredentials.FromFile(@"..\..\credentials.txt");
            _testTableName = "marlintest" + _random.Next(10000);
            _testTableSchema = new TableSchema();
            _testTableSchema.name = _testTableName;
            _testTableSchema.columns.Add(new ColumnSchema() { name = "d" });

            var marlin = new Marlin(_credentials);
            marlin.CreateTable(_testTableSchema);
        }

        [TearDown]
        public void TearDown()
        {
            var marlin = new Marlin(_credentials);
            var tables = marlin.ListTables();
            foreach (var name in tables.name)
            {
                marlin.DeleteTable(name);
            }
        }

        [Test]
        public void TestScannerCreation()
        {
            var marlin = new Marlin(_credentials);
            var batchSetting = new Scanner() { batch = 2 };
            var scannerInfo = marlin.CreateScanner(_testTableName, batchSetting);
            Assert.AreEqual(_testTableName, scannerInfo.TableName);
            Assert.IsTrue(scannerInfo.Location.Authority.StartsWith("headnode"),
                "returned location didn't start with \"headnode\", it was: {0}", scannerInfo.Location);
        }

        [Test]
        public void TestFullScan()
        {
            var marlin = new Marlin(_credentials);

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

        [Test]
        public void TestSubsetScan()
        {
            var marlin = new Marlin(_credentials);
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

        [Test]
        public void TestGetVersion()
        {
            var marlin = new Marlin(_credentials);
            var version = marlin.GetVersion();
            Assert.IsNotNullOrEmpty(version.jvmVersion);
            Assert.IsNotNullOrEmpty(version.jerseyVersion);
            Assert.IsNotNullOrEmpty(version.osVersion);
            Assert.IsNotNullOrEmpty(version.restVersion);
        }

        [Test]
        public void TestListTables()
        {
            var marlin = new Marlin(_credentials);
            var tables = marlin.ListTables();
            Assert.AreEqual(1, tables.name.Count);
            Assert.AreEqual(_testTableName, tables.name[0]);
        }

        [Test]
        public void TestTableSchema()
        {
            var marlin = new Marlin(_credentials);
            var schema = marlin.GetTableSchema(_testTableName);
            Assert.AreEqual(_testTableName, schema.name);
            Assert.AreEqual(_testTableSchema.columns.Count, schema.columns.Count);
            Assert.AreEqual(_testTableSchema.columns[0].name, schema.columns[0].name);
        }

        [Test]
        public void TestGetStorageClusterStatus()
        {
            var marlin = new Marlin(_credentials);
            var status = marlin.GetStorageClusterStatus();
            // TODO not really a good test
            Assert.GreaterOrEqual(status.requests, 0, "number of requests is negative");
            Assert.GreaterOrEqual(status.liveNodes.Count, 1, "number of live nodes is zero or negative");
            Assert.GreaterOrEqual(status.liveNodes[0].requests, 0, "number of requests to the first node is negative");
        }

        [Test]
        public void TestStoreSingleCell()
        {
            var testKey = "content";
            var testValue = "the force is strong in this column";
            var marlin = new Marlin(_credentials);
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

        private void StoreTestData(Marlin marlin)
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

            marlin.StoreCells(_testTableName, set);
        }
    }
}
