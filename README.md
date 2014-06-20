Marlin
======

Marlin is a C# client for HBase on Azure HDInsight.
It currently targets HBase 0.96.2 and HDInsight 3.0 on Microsoft Azure.
The communication works through HBase REST (StarGate) which uses ProtoBuf as a serialization format.

Usage
=====

After compilation, you can easily use the library to get the version of the HBase/HDInsight cluster you're running on:
```csharp
var credentials = ClusterCredentials.FromFile("credentials.txt");
var marlin = new Marlin(credentials);
var version = marlin.GetVersion();
Console.WriteLine(version);
```

The credentials text file contains exactly three lines:
- Azure HDInsight REST URL
- Azure HDInsight Username
- Azure HDInsight Password
 
An example looks like this:
```
https://azurehbase.azurehdinsight.net/hbaserest
admin
_mySup3rS4f3P4ssW0rd.
```

Build
=====

Import the solution file into VS2013 and compile. Retrieve the resulting *.dll files.
A NuGet publish will soon be announced.

Naming
======

The name "Marlin" follows the convention of Skype's data team.
We name parts of our real-time pipeline (even the pipeline itself!) after cool fishes. 
Famous Microsoft internal names are Ray, Whaleshark or BlobFish.

Since the Marlin counts as one of the fastest fishes on earth, we aim to be the fastest C# library when querying HBase on HDInsight.

