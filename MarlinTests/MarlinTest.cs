namespace MarlinTests
{
    using System;
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

    }
}
