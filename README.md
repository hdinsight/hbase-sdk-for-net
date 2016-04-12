Microsoft HBase REST Client Library for .NET
======

This is C# client library for HBase on Azure HDInsight.

It has been compatible with all HBase versions since 0.96.2 (HDI 3.0).

The communication works through HBase REST (StarGate) which uses ProtoBuf as a serialization format.

Non-HDInsight HBase cluster can use VNET mode which does not require OAuth credentials.

Getting Started
===============

* [Getting Started with HBase](http://azure.microsoft.com/en-us/documentation/articles/hdinsight-hbase-get-started/) - docuemntation article that walks you through the steps to create HBase cluster on Azure and then use this library to create a simple app.
* [Analyze real-time Twitter sentiment](http://azure.microsoft.com/en-us/documentation/articles/hdinsight-hbase-analyze-twitter-sentiment/) - more advanced tutorial of creating real-time Twitter sentiment analysis app using this library.

Build
=====

Import the solution file into VS2013 and compile. Retrieve the resulting *.dll files.

We have published the signed binary on nuget.org (https://www.nuget.org/packages/Microsoft.HBase.Client/).

Usage
=====

After compilation, you can easily use the library to get the version of the HBase/HDInsight cluster you're running on:
```csharp
var creds = new ClusterCredentials(new Uri("https://myclustername.azurehdinsight.net"), "myusername", "mypassword");
var client = new HBaseClient(creds);

var version = client.GetVersionAsync().Result;
Console.WriteLine(version);

// yields: RestVersion: 0.0.2, JvmVersion: Azul Systems, Inc. 1.7.0_55-24.55-b03, OsVersion: Windows Server 2012 R2 6.3 amd64, ServerVersion: jetty/6.1.26, JerseyVersion: 1.8, ExtensionObject:
```

Table creation works like this:
```csharp
var creds = new ClusterCredentials(new Uri("https://myclustername.azurehdinsight.net"), "myusername", "mypassword");
var client = new HBaseClient(creds);

var testTableSchema = new TableSchema();
testTableSchema.name = "mytablename";
testTableSchema.columns.Add(new ColumnSchema() { name = "d" });
testTableSchema.columns.Add(new ColumnSchema() { name = "f" });
client.CreateTableAsync(testTableSchema).Wait();
```

Inserting data can be done like this:
```csharp
var creds = new ClusterCredentials(new Uri("https://myclustername.azurehdinsight.net"), "myusername", "mypassword");
var client = new HBaseClient(creds);

var tableName = "mytablename";
var testKey = "content";
var testValue = "the force is strong in this column";
var set = new CellSet();
var row = new CellSet.Row { key = Encoding.UTF8.GetBytes(testKey) };
set.rows.Add(row);

var value = new Cell { column = Encoding.UTF8.GetBytes("d:starwars"), data = Encoding.UTF8.GetBytes(testValue) };
row.values.Add(value);
client.StoreCellsAsync(tableName, set).Wait();
```

Retrieving all cells for a key looks like this:
```csharp
var creds = new ClusterCredentials(new Uri("https://myclustername.azurehdinsight.net"), "myusername", "mypassword");
var client = new HBaseClient(creds);

var testKey = "content";
var tableName = "mytablename";

var cells = client.GetCells(tableName, testKey).Result;
// get the first value from the row.
Console.WriteLine(Encoding.UTF8.GetString(cells.rows[0].values[0].data));
// with the previous insert, it should yield: "the force is strong in this column"
```

Scanning over rows looks like this:
```csharp
var creds = new ClusterCredentials(new Uri("https://myclustername.azurehdinsight.net"), "myusername", "mypassword");
var client = new HBaseClient(creds);

var tableName = "mytablename";

// assume the table has integer keys and we want data between keys 25 and 35
var scanSettings = new Scanner()
{
	batch = 10,
	startRow = BitConverter.GetBytes(25),
	endRow = BitConverter.GetBytes(35)
};
RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
scanOptions.AlternativeEndpoint = "hbaserest0/";
ScannerInformation scannerInfo = null;
try
{
    scannerInfo = client.CreateScannerAsync(tableName, scanSettings, scanOptions);
    CellSet next = null;
    while ((next = client.ScannerGetNextAsync(scannerInfo, scanOptions).Result) != null)
    {
	foreach (var row in next.rows)
        {
    	    // ... read the rows
        }
    }
}
finally
{
    if (scannerInfo != null)
    {
        client.DeleteScannerAsync(tableName, scannerInfo, scanOptions).Wait();
    }
}
```

There is also a VNET mode which can be used if your application is in the VNET with your HDI HBase cluster. NOTE: VNET mode also works for non-HDI clusters or on-premises HBase clusters.
```csharp
var scanOptions = RequestOptions.GetDefaultOptions();
scanOptions.Port = 8090;
scanOptions.AlternativeEndpoint = "/";
var nodeIPs = new List<string>();
nodeIPs.Add("10.0.0.15");
nodeIPs.Add("10.0.0.16");
var client = new HBaseClient(null, options, new LoadBalancerRoundRobin(nodeIPs));
var scanSettings = new Scanner { batch = 10 };
ScannerInformation scannerInfo = client.CreateScanner(testTableName, scanSettings, scanOptions);
var options = RequestOptions.GetDefaultOptions();
options.Port = 8090;
options.AlternativeEndpoint = "/";
options.AlternativeHost = scannerInfo.Location.Host;
client.DeleteScanner(testTableName, scannerInfo, options);
```
