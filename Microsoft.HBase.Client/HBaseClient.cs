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
    using System;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;
    using ProtoBuf;
    using org.apache.hadoop.hbase.rest.protobuf.generated;

    /// <summary>
    /// A C# connector to HBase. 
    /// 
    /// It currently targets HBase 0.96.2 and HDInsight 3.0 on Microsoft Azure.
    /// The communication works through HBase REST (StarGate) which uses ProtoBuf as a serialization format.
    /// 
    /// The usage is quite simple:
    /// 
    /// <code>
    /// var credentials = ClusterCredentials.FromFile("credentials.txt");
    /// var marlin = new Marlin(credentials);
    /// 
    /// var version = await marlin.GetVersionAsync();
    /// 
    /// Console.WriteLine(version);
    /// 
    /// </code>
    /// </summary>
    public class HBaseClient
    {
        private readonly WebRequester _requester;

        public HBaseClient(ClusterCredentials credentials)
        {
            credentials.ArgumentNotNull("credentials");
            _requester = new WebRequester(credentials);
        }

        /// <summary>
        /// Creates a scanner on the server side.
        /// The resulting ScannerInformation can be used to read query the CellSets returned by this scanner in the #ScannerGetNext/Async method.
        /// </summary>
        /// <param name="tableName">the table to scan</param>
        /// <param name="scannerSettings">the settings to e.g. set the batch size of this scan</param>
        /// <returns>A ScannerInformation which contains the continuation url/token and the table name</returns>
        public ScannerInformation CreateScanner(string tableName, Scanner scannerSettings)
        {
            return CreateScannerAsync(tableName, scannerSettings).Result;
        }

        /// <summary>
        /// Creates a scanner on the server side.
        /// The resulting ScannerInformation can be used to read query the CellSets returned by this scanner in the #ScannerGetNext/Async method.
        /// </summary>
        /// <param name="tableName">the table to scan</param>
        /// <param name="scannerSettings">the settings to e.g. set the batch size of this scan</param>
        /// <returns>A ScannerInformation which contains the continuation url/token and the table name</returns>
        public async Task<ScannerInformation> CreateScannerAsync(string tableName, Scanner scannerSettings)
        {
            if (tableName == null || !tableName.Any())
            {
                throw new ArgumentException("TableName was either null or empty!");
            }
            if (scannerSettings == null)
            {
                throw new ArgumentException("ScannerSettings was null!");
            }

            using (HttpWebResponse response = await PostRequest(tableName + "/scanner", scannerSettings, WebRequester.RestEndpointBaseZero))
            {
                if (response.StatusCode != HttpStatusCode.Created)
                {
                    using (var output = new StreamReader(response.GetResponseStream()))
                    {
                        string message = output.ReadToEnd();
                        throw new WebException(
                            string.Format(
                                "Couldn't create a scanner for table {0}! Response code was: {1}, expected 201! Response body was: {2}",
                                tableName,
                                response.StatusCode,
                                message));
                    }
                }
                string location = response.Headers.Get("Location");
                if (location == null)
                {
                    throw new ArgumentException("Couldn't find header 'Location' in the response!");
                }
                return new ScannerInformation { TableName = tableName, Location = new Uri(location) };
            }
        }

        /// <summary>
        /// Creates a table and/or fully replaces its schema.
        /// </summary>
        /// <param name="schema">the schema</param>
        /// <returns>returns true if the table was created, false if the table already exists. In case of any other error it throws a WebException.</returns>
        public bool CreateTable(TableSchema schema)
        {
            return CreateTableAsync(schema).Result;
        }

        /// <summary>
        /// Creates a table and/or fully replaces its schema.
        /// </summary>
        /// <param name="schema">the schema</param>
        /// <returns>returns true if the table was created, false if the table already exists. In case of any other error it throws a WebException.</returns>
        public async Task<bool> CreateTableAsync(TableSchema schema)
        {
            if (schema == null)
            {
                throw new ArgumentException("Schema was null!");
            }
            if (schema.name == null || !schema.name.Any())
            {
                throw new ArgumentException("TableName was either null or empty!");
            }
            using (HttpWebResponse webResponse = await PutRequest(schema.name + "/schema", schema))
            {
                if (webResponse.StatusCode == HttpStatusCode.Created)
                {
                    return true;
                }
                // table already exits
                if (webResponse.StatusCode == HttpStatusCode.OK)
                {
                    return false;
                }

                // throw the exception otherwise
                using (var output = new StreamReader(webResponse.GetResponseStream()))
                {
                    string message = output.ReadToEnd();
                    throw new WebException(
                        string.Format(
                            "Couldn't create table {0}! Response code was: {1}, expected either 200 or 201! Response body was: {2}",
                            schema.name,
                            webResponse.StatusCode,
                            message));
                }
            }
        }

        /// <summary>
        /// Deletes a table.
        /// If something went wrong, a WebException is thrown.
        /// </summary>
        /// <param name="tableName">the table name</param>
        public void DeleteTable(string tableName)
        {
            DeleteTableAsync(tableName).Wait();
        }

        /// <summary>
        /// Deletes a table.
        /// If something went wrong, a WebException is thrown.
        /// </summary>
        /// <param name="table">the table name</param>
        public async Task DeleteTableAsync(string table)
        {
            if (table == null || !table.Any())
            {
                throw new ArgumentException("TableName was either null or empty!");
            }
            using (HttpWebResponse webResponse = await DeleteRequest<TableSchema>(table + "/schema", null))
            {
                if (webResponse.StatusCode != HttpStatusCode.OK)
                {
                    using (var output = new StreamReader(webResponse.GetResponseStream()))
                    {
                        string message = output.ReadToEnd();
                        throw new WebException(
                            string.Format(
                                "Couldn't delete table {0}! Response code was: {1}, expected 200! Response body was: {2}",
                                table,
                                webResponse.StatusCode,
                                message));
                    }
                }
            }
        }

        public CellSet GetCells(string tableName, string rowKey)
        {
            return GetCellsAsync(tableName, rowKey).Result;
        }

        // TODO add timestamp, versions and column queries
        public async Task<CellSet> GetCellsAsync(string tableName, string rowKey)
        {
            if (tableName == null || !tableName.Any())
            {
                throw new ArgumentException("TableName was either null or empty!");
            }
            if (rowKey == null)
            {
                throw new ArgumentException("RowKey was null!");
            }
            return await GetRequestAndDeserialize<CellSet>(tableName + "/" + rowKey);
        }

        public StorageClusterStatus GetStorageClusterStatus()
        {
            return GetStorageClusterStatusAsync().Result;
        }

        public async Task<StorageClusterStatus> GetStorageClusterStatusAsync()
        {
            return await GetRequestAndDeserialize<StorageClusterStatus>("/status/cluster");
        }

        public TableInfo GetTableInfo(string table)
        {
            return GetTableInfoAsync(table).Result;
        }

        public async Task<TableInfo> GetTableInfoAsync(string table)
        {
            if (table == null || !table.Any())
            {
                throw new ArgumentException("Table was either null or empty!");
            }
            return await GetRequestAndDeserialize<TableInfo>(table + "/regions");
        }

        public TableSchema GetTableSchema(string table)
        {
            return GetTableSchemaAsync(table).Result;
        }

        public async Task<TableSchema> GetTableSchemaAsync(string table)
        {
            if (table == null || !table.Any())
            {
                throw new ArgumentException("Table was either null or empty!");
            }
            return await GetRequestAndDeserialize<TableSchema>(table + "/schema");
        }

        public org.apache.hadoop.hbase.rest.protobuf.generated.Version GetVersion()
        {
            return GetVersionAsync().Result;
        }

        public async Task<org.apache.hadoop.hbase.rest.protobuf.generated.Version> GetVersionAsync()
        {
            return await GetRequestAndDeserialize<org.apache.hadoop.hbase.rest.protobuf.generated.Version>("version");
        }

        public TableList ListTables()
        {
            return ListTablesAsync().Result;
        }

        public async Task<TableList> ListTablesAsync()
        {
            return await GetRequestAndDeserialize<TableList>("");
        }

        /// <summary>
        /// Modifies a table schema. 
        /// If necessary it creates a new table with the given schema. 
        /// If something went wrong, a WebException is thrown.
        /// </summary>
        /// <param name="tableName">the table name</param>
        /// <param name="schema">the schema</param>
        public void ModifyTableSchema(string tableName, TableSchema schema)
        {
            ModifyTableSchemaAsync(tableName, schema).Wait();
        }

        /// <summary>
        /// Modifies a table schema. 
        /// If necessary it creates a new table with the given schema. 
        /// If something went wrong, a WebException is thrown.
        /// </summary>
        /// <param name="table">the table name</param>
        /// <param name="schema">the schema</param>
        public async Task ModifyTableSchemaAsync(string table, TableSchema schema)
        {
            if (table == null || !table.Any())
            {
                throw new ArgumentException("TableName was either null or empty!");
            }
            if (schema == null)
            {
                throw new ArgumentException("Schema was null!");
            }
            using (HttpWebResponse webResponse = await PostRequest(table + "/schema", schema))
            {
                if (webResponse.StatusCode != HttpStatusCode.OK || webResponse.StatusCode != HttpStatusCode.Created)
                {
                    using (var output = new StreamReader(webResponse.GetResponseStream()))
                    {
                        string message = output.ReadToEnd();
                        throw new WebException(
                            string.Format(
                                "Couldn't modify table {0}! Response code was: {1}, expected either 200 or 201! Response body was: {2}",
                                table,
                                webResponse.StatusCode,
                                message));
                    }
                }
            }
        }

        /// <summary>
        /// Scans the next set of messages.
        /// </summary>
        /// <param name="scannerInfo">the scanner information retrieved by #CreateScanner()</param>
        /// <returns>a cellset, or null if the scanner is exhausted</returns>
        public CellSet ScannerGetNext(ScannerInformation scannerInfo)
        {
            return ScannerGetNextAsync(scannerInfo).Result;
        }

        /// <summary>
        /// Scans the next set of messages.
        /// </summary>
        /// <param name="scannerInfo">the scanner information retrieved by #CreateScanner()</param>
        /// <returns>a cellset, or null if the scanner is exhausted</returns>
        public async Task<CellSet> ScannerGetNextAsync(ScannerInformation scannerInfo)
        {
            if (scannerInfo == null)
            {
                throw new ArgumentException("ScannerInformation was null!");
            }

            using (
                HttpWebResponse webResponse =
                    await GetRequest(scannerInfo.TableName + "/scanner/" + scannerInfo.ScannerId, WebRequester.RestEndpointBaseZero))
            {
                if (webResponse.StatusCode == HttpStatusCode.OK)
                {
                    return Serializer.Deserialize<CellSet>(webResponse.GetResponseStream());
                }

                return null;
            }
        }

        /// <summary>
        /// Stores the given cells in the supplied table.
        /// </summary>
        /// <param name="table">the table</param>
        /// <param name="cells">the cells to insert</param>
        public void StoreCells(string table, CellSet cells)
        {
            StoreCellsAsync(table, cells).Wait();
        }

        /// <summary>
        /// Stores the given cells in the supplied table.
        /// </summary>
        /// <param name="table">the table</param>
        /// <param name="cells">the cells to insert</param>
        /// <returns>a task that is awaitable, signifying the end of this operation</returns>
        public async Task StoreCellsAsync(string table, CellSet cells)
        {
            if (table == null || !table.Any())
            {
                throw new ArgumentException("TableName was either null or empty!");
            }
            if (cells == null)
            {
                throw new ArgumentException("CellSet was null!");
            }

            // note the fake row key to insert a set of cells
            using (HttpWebResponse webResponse = await PutRequest(table + "/somefalsekey", cells))
            {
                if (webResponse.StatusCode != HttpStatusCode.OK)
                {
                    using (var output = new StreamReader(webResponse.GetResponseStream()))
                    {
                        string message = output.ReadToEnd();
                        throw new WebException(
                            string.Format(
                                "Couldn't insert into table {0}! Response code was: {1}, expected 200! Response body was: {2}",
                                table,
                                webResponse.StatusCode,
                                message));
                    }
                }
            }
        }

        internal async Task<HttpWebResponse> DeleteRequest<TReq>(string endpoint, TReq request, string alternativeEndpointBase = null)
            where TReq : class
        {
            return await ExecuteMethod("DELETE", endpoint, request, alternativeEndpointBase);
        }

        internal async Task<HttpWebResponse> ExecuteMethod<TReq>(string method, string endpoint, TReq request, string alternativeEndpointBase = null)
            where TReq : class
        {
            // TODO make the buffer size configurable 
            using (var input = new MemoryStream())
            {
                if (request != null)
                {
                    Serializer.Serialize(input, request);
                }
                return await _requester.IssueWebRequestAsync(endpoint, method: method, input: input, alternativeEndpointBase: alternativeEndpointBase);
            }
        }

        internal async Task<HttpWebResponse> GetRequest(string endpoint, string alternativeEndpointBase = null)
        {
            return await _requester.IssueWebRequestAsync(endpoint, "GET", null, alternativeEndpointBase: alternativeEndpointBase);
        }

        internal async Task<T> GetRequestAndDeserialize<T>(string endpoint, string alternativeEndpointBase = null)
        {
            using (HttpWebResponse response = await _requester.IssueWebRequestAsync(endpoint, "GET", alternativeEndpointBase: alternativeEndpointBase)
                )
            {
                using (Stream responseStream = response.GetResponseStream())
                {
                    return Serializer.Deserialize<T>(responseStream);
                }
            }
        }

        internal async Task<HttpWebResponse> PostRequest<TReq>(string endpoint, TReq request, string alternativeEndpointBase = null)
            where TReq : class
        {
            return await ExecuteMethod("POST", endpoint, request, alternativeEndpointBase);
        }

        internal async Task<HttpWebResponse> PutRequest<TReq>(string endpoint, TReq request, string alternativeEndpointBase = null) where TReq : class
        {
            return await ExecuteMethod("PUT", endpoint, request, alternativeEndpointBase);
        }
    }
}
