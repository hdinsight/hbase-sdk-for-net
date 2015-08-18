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

namespace Microsoft.HBase.Client
{
    using System.Threading.Tasks;
    using org.apache.hadoop.hbase.rest.protobuf.generated;

    /// <summary>
    /// A C# connector to HBase. 
    /// </summary>
    /// <remarks>
    /// It currently targets HBase 0.98 and HDInsight 3.1 on Microsoft Azure.
    /// The communication works through HBase REST (StarGate) which uses ProtoBuf as a serialization format.
    /// 
    /// The usage is quite simple:
    /// 
    /// <code>
    /// var credentials = ClusterCredentials.FromFile("credentials.txt");
    /// var client = new HBaseClient(credentials);
    /// var version = await client.GetVersionAsync();
    /// 
    /// Console.WriteLine(version);
    /// </code>
    /// </remarks>
    public interface IHBaseClient
    {
        /// <summary>
        /// Creates a scanner on the server side.
        /// The resulting ScannerInformation can be used to read query the CellSets returned by this scanner in the #ScannerGetNext/Async method.
        /// </summary>
        /// <param name="tableName">the table to scan</param>
        /// <param name="scannerSettings">the settings to e.g. set the batch size of this scan</param>
        /// <returns>A ScannerInformation which contains the continuation url/token and the table name</returns>
        ScannerInformation CreateScanner(string tableName, Scanner scannerSettings);

        /// <summary>
        /// Creates a scanner on the server side.
        /// The resulting ScannerInformation can be used to read query the CellSets returned by this scanner in the #ScannerGetNext/Async method.
        /// </summary>
        /// <param name="tableName">the table to scan</param>
        /// <param name="scannerSettings">the settings to e.g. set the batch size of this scan</param>
        /// <returns>A ScannerInformation which contains the continuation url/token and the table name</returns>
        Task<ScannerInformation> CreateScannerAsync(string tableName, Scanner scannerSettings);

        /// <summary>
        /// Deletes scanner.        
        /// </summary>
        /// <param name="tableName">the table the scanner is associated with.</param>
        /// <param name="scannerId">the id of the scanner to delete.</param>
        Task DeleteScannerAsync(string tableName, string scannerId);

        /// <summary>
        /// Creates a table and/or fully replaces its schema.
        /// </summary>
        /// <param name="schema">the schema</param>
        /// <returns>returns true if the table was created, false if the table already exists. In case of any other error it throws a WebException.</returns>
        bool CreateTable(TableSchema schema);

        /// <summary>
        /// Creates a table and/or fully replaces its schema.
        /// </summary>
        /// <param name="schema">the schema</param>
        /// <returns>returns true if the table was created, false if the table already exists. In case of any other error it throws a WebException.</returns>
        Task<bool> CreateTableAsync(TableSchema schema);

        /// <summary>
        /// Deletes a table.
        /// If something went wrong, a WebException is thrown.
        /// </summary>
        /// <param name="tableName">the table name</param>
        void DeleteTable(string tableName);

        /// <summary>
        /// Deletes a table.
        /// If something went wrong, a WebException is thrown.
        /// </summary>
        /// <param name="table">the table name</param>
        Task DeleteTableAsync(string table);

        /// <summary>
        /// Gets the cells.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="rowKey">The row key.</param>
        /// <returns></returns>
        CellSet GetCells(string tableName, string rowKey);

        /// <summary>
        /// Gets the cells asynchronously.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="rowKey">The row key.</param>
        /// <returns></returns>
        Task<CellSet> GetCellsAsync(string tableName, string rowKey);

        /// <summary>
        /// Gets the storage cluster status.
        /// </summary>
        /// <returns>
        /// </returns>
        StorageClusterStatus GetStorageClusterStatus();

        /// <summary>
        /// Gets the storage cluster status asynchronous.
        /// </summary>
        /// <returns>
        /// </returns>
        Task<StorageClusterStatus> GetStorageClusterStatusAsync();

        /// <summary>
        /// Gets the table information.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns>
        /// </returns>
        TableInfo GetTableInfo(string table);

        /// <summary>
        /// Gets the table information asynchronously.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns></returns>
        Task<TableInfo> GetTableInfoAsync(string table);

        /// <summary>
        /// Gets the table schema.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns>
        /// </returns>
        TableSchema GetTableSchema(string table);

        /// <summary>
        /// Gets the table schema asynchronously.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns>
        /// </returns>
        Task<TableSchema> GetTableSchemaAsync(string table);

        /// <summary>
        /// Gets the version.
        /// </summary>
        /// <returns>
        /// </returns>
        org.apache.hadoop.hbase.rest.protobuf.generated.Version GetVersion();

        /// <summary>
        /// Gets the version asynchronously.
        /// </summary>
        /// <returns>
        /// </returns>
        Task<org.apache.hadoop.hbase.rest.protobuf.generated.Version> GetVersionAsync();

        /// <summary>
        /// Lists the tables.
        /// </summary>
        /// <returns></returns>
        TableList ListTables();

        /// <summary>
        /// Lists the tables asynchronously.
        /// </summary>
        /// <returns>
        /// </returns>
        Task<TableList> ListTablesAsync();

        /// <summary>
        /// Modifies a table schema. 
        /// If necessary it creates a new table with the given schema. 
        /// If something went wrong, a WebException is thrown.
        /// </summary>
        /// <param name="tableName">the table name</param>
        /// <param name="schema">the schema</param>
        void ModifyTableSchema(string tableName, TableSchema schema);

        /// <summary>
        /// Modifies a table schema. 
        /// If necessary it creates a new table with the given schema. 
        /// If something went wrong, a WebException is thrown.
        /// </summary>
        /// <param name="table">the table name</param>
        /// <param name="schema">the schema</param>
        Task ModifyTableSchemaAsync(string table, TableSchema schema);

        /// <summary>
        /// Scans the next set of messages.
        /// </summary>
        /// <param name="scannerInfo">the scanner information retrieved by #CreateScanner()</param>
        /// <returns>a cellset, or null if the scanner is exhausted</returns>
        CellSet ScannerGetNext(ScannerInformation scannerInfo);

        /// <summary>
        /// Scans the next set of messages.
        /// </summary>
        /// <param name="scannerInfo">the scanner information retrieved by #CreateScanner()</param>
        /// <returns>a cellset, or null if the scanner is exhausted</returns>
        Task<CellSet> ScannerGetNextAsync(ScannerInformation scannerInfo);

        /// <summary>
        /// Stores the given cells in the supplied table.
        /// </summary>
        /// <param name="table">the table</param>
        /// <param name="cells">the cells to insert</param>
        void StoreCells(string table, CellSet cells);

        /// <summary>
        /// Stores the given cells in the supplied table.
        /// </summary>
        /// <param name="table">the table</param>
        /// <param name="cells">the cells to insert</param>
        /// <returns>a task that is awaitable, signifying the end of this operation</returns>
        Task StoreCellsAsync(string table, CellSet cells);
    }
}
