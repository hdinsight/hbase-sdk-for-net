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
    using System.Collections.Generic;
    /// <summary>
    /// A C# connector to HBase. 
    /// </summary>
    /// <remarks>
    /// It currently targets HBase 0.98.4 and HDInsight 3.2 on Microsoft Azure.
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
    /// 
    /// Scan Requests are stateful. Please provide RequestOption for every scan requests to specify the request is sent to which REST server.
    /// In Gateway mode, RequestOption.AlternativeEndpoint need to be set to "hbaserest0/","hbaserest1/","hbaserest2/"...etc.
    /// In VNET mode,  RequestOption.AlternativeEndpoint need to be set to "/" and RequestOption.AlternativeHost need to be set or use loadbalancer.
    /// </remarks>
    public interface IHBaseClient
    {
        /// <summary>
        /// Creates a scanner on the server side.
        /// The resulting ScannerInformation can be used to read query the CellSets returned by this scanner in the #ScannerGetNext/Async method.
        /// </summary>
        /// <param name="tableName">the table to scan</param>
        /// <param name="scannerSettings">the settings to e.g. set the batch size of this scan</param>
        /// <param name="options">the request options, scan requests must set endpoint(Gateway mode) or host(VNET mode) to receive the scan request</param>
        /// <returns>A ScannerInformation which contains the continuation url/token and the table name</returns>
        Task<ScannerInformation> CreateScannerAsync(string tableName, Scanner scannerSettings, RequestOptions options);

        /// <summary>
        /// Deletes scanner.        
        /// </summary>
        /// <param name="tableName">the table the scanner is associated with.</param>
        /// <param name="scannerInfo">the scanner information retrieved by #CreateScanner()</param>
        /// <param name="options">the request options, scan requests must set endpoint(Gateway mode) or host(VNET mode) to receive the scan request</param>
        Task DeleteScannerAsync(string tableName, ScannerInformation scannerInfo, RequestOptions options);

        /// <summary>
        /// Deletes row with specific row key.        
        /// </summary>
        /// <param name="tableName">the table name</param>
        /// <param name="rowKey">the row to delete</param>
        Task DeleteCellsAsync(string tableName, string rowKey, RequestOptions options = null);

        /// <summary>
        /// Deletes row with specific row key and specific columnFamily and versions less than the mentioned one
        /// </summary>
        /// <param name="tableName">the table name</param>
        /// <param name="rowKey">the row to delete</param>
        /// <param name="columnFamily">the column family to delete</param>
        /// <param name="timestamp">timestamp's lower than this will be deleted for the row</param>
        Task DeleteCellsAsync(string tableName, string rowKey, string columnFamily, long timestamp, RequestOptions options = null);

        /// <summary>
        /// Creates a table and/or fully replaces its schema.
        /// </summary>
        /// <param name="schema">the schema</param>
        /// <returns>returns true if the table was created, false if the table already exists. In case of any other error it throws a WebException.</returns>
        Task<bool> CreateTableAsync(TableSchema schema, RequestOptions options = null);

        /// <summary>
        /// Deletes a table.
        /// If something went wrong, a WebException is thrown.
        /// </summary>
        /// <param name="table">the table name</param>
        Task DeleteTableAsync(string table, RequestOptions options = null);

        /// <summary>
        /// Gets the cells asynchronously. Getting column value by columnName and getting multi-versions only work in VNET mode currently.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="rowKey">The row key.</param>
        /// <param name="columnName">familyname:columnname</param>
        /// <param name="numOfVersions">Number of versions to fetch</param>
        /// <returns></returns>
        Task<CellSet> GetCellsAsync(string tableName, string rowKey, string columnName=null, string numOfVersions=null, RequestOptions options = null);

        /// <summary>
        /// Gets the storage cluster status asynchronous.
        /// </summary>
        /// <returns>
        /// </returns>
        Task<StorageClusterStatus> GetStorageClusterStatusAsync(RequestOptions options = null);

        /// <summary>
        /// Gets the table information asynchronously.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns></returns>
        Task<TableInfo> GetTableInfoAsync(string table, RequestOptions options = null);

        /// <summary>
        /// Gets the table schema asynchronously.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns>
        /// </returns>
        Task<TableSchema> GetTableSchemaAsync(string table, RequestOptions options = null);

        /// <summary>
        /// Gets the version asynchronously.
        /// </summary>
        /// <returns>
        /// </returns>
        Task<org.apache.hadoop.hbase.rest.protobuf.generated.Version> GetVersionAsync(RequestOptions options = null);

        /// <summary>
        /// Lists the tables asynchronously.
        /// </summary>
        /// <returns>
        /// </returns>
        Task<TableList> ListTablesAsync(RequestOptions options = null);

        /// <summary>
        /// Modifies a table schema. 
        /// If necessary it creates a new table with the given schema. 
        /// If something went wrong, a WebException is thrown.
        /// </summary>
        /// <param name="table">the table name</param>
        /// <param name="schema">the schema</param>
        Task ModifyTableSchemaAsync(string table, TableSchema schema, RequestOptions options = null);

        /// <summary>
        /// Scans the next set of messages.
        /// </summary>
        /// <param name="scannerInfo">the scanner information retrieved by #CreateScanner()</param>
        /// <param name="options">the request options, scan requests must set endpoint(Gateway mode) or host(VNET mode) to receive the scan request</param>
        /// <returns>a cellset, or null if the scanner is exhausted</returns>
        Task<CellSet> ScannerGetNextAsync(ScannerInformation scannerInfo, RequestOptions options);

        /// <summary>
        /// Stores the given cells in the supplied table.
        /// </summary>
        /// <param name="table">the table</param>
        /// <param name="cells">the cells to insert</param>
        /// <returns>a task that is awaitable, signifying the end of this operation</returns>
        Task StoreCellsAsync(string table, CellSet cells, RequestOptions options = null);

        /// <summary>
        /// Automically checks if a row/family/qualifier value matches the expected value and updates
        /// </summary>
        /// <param name="table">the table</param>
        /// <param name="row">row to update</param>
        /// <param name="cellToCheck">cell to check</param>
        /// <returns>true if the record was updated; false if condition failed at check</returns>
        Task<bool> CheckAndPutAsync(string table, CellSet.Row row, Cell cellToCheck, RequestOptions options = null);

        /// <summary>
        /// Automically checks if a row/family/qualifier value matches the expected value and deletes
        /// </summary>
        /// <param name="table">the table</param>
        /// <param name="cellToCheck">cell to check for deleting the row</param>
        /// <param name="row">row cells to delete</param>
        /// <returns>true if the record was deleted; false if condition failed at check</returns>
        Task<bool> CheckAndDeleteAsync(string table, Cell cellToCheck, CellSet.Row row = null, RequestOptions options = null);


        Task<IEnumerable<CellSet>> StatelessScannerAsync(string tableName, string optionalRowPrefix = null, string scanParameters = null, RequestOptions options = null);
    }
}
