Marlin - Microsoft HBase REST Client Library for .NET
======

Marlin is a C# client for HBase on Azure HDInsight.
It currently targets HBase 0.96.2 and HDInsight 3.0 on Microsoft Azure.
The communication works through HBase REST (StarGate) which uses ProtoBuf as a serialization format.

Missing features
================

There are some core features missing that are documented in the [Stargate wiki](http://wiki.apache.org/hadoop/Hbase/Stargate "stargate docs"):
- Enhancements to existing methods e.g. multi-cell stores with timestamps, versions or specific columns

_Besides that..._

Marlin currently only provides the C# <-> ProtoBuf <-> Stargate communication layer.
As you can imagine, this API is neither very C#'y nor is it very convenient to use.

*If you want to layer a much nicer/fluent client API on top of that, please feel free to fork and open a pull request.*

Build
=====

Import the solution file into VS2013 and compile. Retrieve the resulting *.dll files.

A NuGet publish will be announced as soon as a first feature complete version is done.

Usage
=====

After compilation, you can easily use the library to get the version of the HBase/HDInsight cluster you're running on:
```csharp
var credentials = ClusterCredentials.FromFile("credentials.txt");
var marlin = new Marlin(credentials);
var version = marlin.GetVersion();
Console.WriteLine(version);

// yields:
RestVersion: 0.0.2, JvmVersion: Azul Systems, Inc. 1.7.0_55-24.55-b03, OsVersion: Windows Server 2012 R2 6.3 amd64, ServerVersion: jetty/6.1.26, JerseyVersion: 1.8, ExtensionObject: 
```

The credentials text file contains exactly three lines:
- Azure HDInsight URL
- Azure HDInsight Username
- Azure HDInsight Password
 
An example looks like this:
```
https://azurehbase.azurehdinsight.net
admin
_mySup3rS4f3P4ssW0rd.
```

Table creation works like this:

```csharp
var marlin = new Marlin(ClusterCredentials.FromFile("credentials.txt"));
var tableName = "table";
var testTableSchema = new TableSchema();
testTableSchema.name = tableName;
testTableSchema.columns.Add(new ColumnSchema() { name = "d" });
testTableSchema.columns.Add(new ColumnSchema() { name = "f" });
marlin.CreateTable(testTableSchema);
```

Inserting stuff can be done like this:

```csharp
var testKey = "content";
var testValue = "the force is strong in this column";

var marlin = new Marlin(ClusterCredentials.FromFile("credentials.txt"));
CellSet set = new CellSet();
CellSet.Row row = new CellSet.Row() { key = Encoding.UTF8.GetBytes(testKey) };
set.rows.Add(row);

var value = new Cell() { column = Encoding.UTF8.GetBytes("d:starwars"), data = Encoding.UTF8.GetBytes(testValue) };
row.values.Add(value);
marlin.StoreCells("table", set);
```

Retrieving all cells for a key looks like that:

```csharp
var cells = marlin.GetCells("table", testKey);
// get the first value from the row.
Console.WriteLine(Encoding.UTF8.GetString(cells.rows[0].values[0].data));
// with the previous insert, it should yield: "the force is strong in this column"
```

Scanning over rows looks like this:

```csharp

var marlin = new Marlin(_credentials);
// assume the table has integer keys and we want data between keys 25 and 35
var scanSettings = new Scanner()
{
	batch = 10,
	startRow = BitConverter.GetBytes(25),
	endRow = BitConverter.GetBytes(35)
};
var scannerInfo = marlin.CreateScanner(_testTableName, scanSettings);
CellSet next = null;
while ((next = marlin.ScannerGetNext(scannerInfo)) != null)
{
	foreach (var row in next.rows)
		// read the rows
}            

```
