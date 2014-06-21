namespace MarlinTests
{
    using System;
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
            _testTableSchema = new TableSchema();
            _testTableSchema.columns.Add(new ColumnSchema() { name = "d" });

            var marlin = new Marlin(_credentials);
            _testTableName = "marlintest-" + _random.Next(10000);
            marlin.CreateTable(_testTableName, _testTableSchema);
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
    }
}
