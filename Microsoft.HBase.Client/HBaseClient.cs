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
    using System.Net;
    using System.Threading.Tasks;
    using Microsoft.HBase.Client.Internal;
    using org.apache.hadoop.hbase.rest.protobuf.generated;
    using ProtoBuf;

    /// <summary>
    /// A C# connector to HBase. 
    /// </summary>
    /// <remarks>
    /// It currently targets HBase 0.96.2 and HDInsight 3.0 on Microsoft Azure.
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
    public sealed class HBaseClient : IHBaseClient
    {
        private readonly WebRequester _requester;
        private readonly IRetryPolicyFactory _retryPolicyFactory;

        /// <summary>
        /// Initializes a new instance of the <see cref="HBaseClient"/> class.
        /// </summary>
        /// <param name="credentials">The credentials.</param>
        public HBaseClient(ClusterCredentials credentials)
            : this(credentials, new DefaultRetryPolicyFactory())
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="HBaseClient"/> class.
        /// </summary>
        /// <param name="credentials">The credentials.</param>
        /// <param name="retryPolicyFactory">The retry policy factory.</param>
        public HBaseClient(ClusterCredentials credentials, IRetryPolicyFactory retryPolicyFactory)
        {
            credentials.ArgumentNotNull("credentials");
            retryPolicyFactory.ArgumentNotNull("retryPolicyFactory");

            _requester = new WebRequester(credentials);
            _retryPolicyFactory = retryPolicyFactory;
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
            tableName.ArgumentNotNullNorEmpty("tableName");
            scannerSettings.ArgumentNotNull("scannerSettings");

            while (true)
            {
                IRetryPolicy retryPolicy = _retryPolicyFactory.Create();
                try
                {
                    using (HttpWebResponse response = await PostRequestAsync(tableName + "/scanner", scannerSettings, WebRequester.RestEndpointBaseZero))
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
                        return new ScannerInformation(new Uri(location), tableName);
                    }
                }
                catch (Exception e)
                {
                    if (!retryPolicy.ShouldRetryAttempt(e))
                    {
                        throw;
                    }
                }
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
            schema.ArgumentNotNull("schema");

            if (string.IsNullOrEmpty(schema.name))
            {
                throw new ArgumentException("schema.name was either null or empty!", "schema");
            }

            while (true)
            {
                IRetryPolicy retryPolicy = _retryPolicyFactory.Create();
                try
                {
                    using (HttpWebResponse webResponse = await PutRequestAsync(schema.name + "/schema", schema))
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
                catch (Exception e)
                {
                    if (!retryPolicy.ShouldRetryAttempt(e))
                    {
                        throw;
                    }
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
            table.ArgumentNotNullNorEmpty("table");

            while (true)
            {
                IRetryPolicy retryPolicy = _retryPolicyFactory.Create();
                try
                {
                    using (HttpWebResponse webResponse = await DeleteRequestAsync<TableSchema>(table + "/schema", null))
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
                        else
                        {
                            return;
                        }
                    }
                }
                catch (Exception e)
                {
                    if (!retryPolicy.ShouldRetryAttempt(e))
                    {
                        throw;
                    }
                }
            }
        }

        /// <summary>
        /// Gets the cells.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="rowKey">The row key.</param>
        /// <returns></returns>
        public CellSet GetCells(string tableName, string rowKey)
        {
            return GetCellsAsync(tableName, rowKey).Result;
        }

        /// <summary>
        /// Gets the cells asynchronously.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="rowKey">The row key.</param>
        /// <returns></returns>
        public async Task<CellSet> GetCellsAsync(string tableName, string rowKey)
        {
            // TODO add timestamp, versions and column queries
            tableName.ArgumentNotNullNorEmpty("tableName");
            rowKey.ArgumentNotNull("rowKey");

            while (true)
            {
                IRetryPolicy retryPolicy = _retryPolicyFactory.Create();
                try
                {
                    return await GetRequestAndDeserializeAsync<CellSet>(tableName + "/" + rowKey);
                }
                catch (Exception e)
                {
                    if (!retryPolicy.ShouldRetryAttempt(e))
                    {
                        throw;
                    }
                }
            }
        }

        /// <summary>
        /// Gets the storage cluster status.
        /// </summary>
        /// <returns>
        /// </returns>
        public StorageClusterStatus GetStorageClusterStatus()
        {
            return GetStorageClusterStatusAsync().Result;
        }

        /// <summary>
        /// Gets the storage cluster status asynchronous.
        /// </summary>
        /// <returns>
        /// </returns>
        public async Task<StorageClusterStatus> GetStorageClusterStatusAsync()
        {
            while (true)
            {
                IRetryPolicy retryPolicy = _retryPolicyFactory.Create();
                try
                {
                    return await GetRequestAndDeserializeAsync<StorageClusterStatus>("/status/cluster");
                }
                catch (Exception e)
                {
                    if (!retryPolicy.ShouldRetryAttempt(e))
                    {
                        throw;
                    }
                }
            }
        }

        /// <summary>
        /// Gets the table information.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns>
        /// </returns>
        public TableInfo GetTableInfo(string table)
        {
            return GetTableInfoAsync(table).Result;
        }

        /// <summary>
        /// Gets the table information asynchronously.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns></returns>
        public async Task<TableInfo> GetTableInfoAsync(string table)
        {
            table.ArgumentNotNullNorEmpty("table");

            while (true)
            {
                IRetryPolicy retryPolicy = _retryPolicyFactory.Create();
                try
                {
                    return await GetRequestAndDeserializeAsync<TableInfo>(table + "/regions");
                }
                catch (Exception e)
                {
                    if (!retryPolicy.ShouldRetryAttempt(e))
                    {
                        throw;
                    }
                }
            }
        }

        /// <summary>
        /// Gets the table schema.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns>
        /// </returns>
        public TableSchema GetTableSchema(string table)
        {
            return GetTableSchemaAsync(table).Result;
        }

        /// <summary>
        /// Gets the table schema asynchronously.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns>
        /// </returns>
        public async Task<TableSchema> GetTableSchemaAsync(string table)
        {
            table.ArgumentNotNullNorEmpty("table");

            while (true)
            {
                IRetryPolicy retryPolicy = _retryPolicyFactory.Create();
                try
                {
                    return await GetRequestAndDeserializeAsync<TableSchema>(table + "/schema");
                }
                catch (Exception e)
                {
                    if (!retryPolicy.ShouldRetryAttempt(e))
                    {
                        throw;
                    }
                }
            }
        }

        /// <summary>
        /// Gets the version.
        /// </summary>
        /// <returns>
        /// </returns>
        public org.apache.hadoop.hbase.rest.protobuf.generated.Version GetVersion()
        {
            return GetVersionAsync().Result;
        }

        /// <summary>
        /// Gets the version asynchronously.
        /// </summary>
        /// <returns>
        /// </returns>
        public async Task<org.apache.hadoop.hbase.rest.protobuf.generated.Version> GetVersionAsync()
        {
            while (true)
            {
                IRetryPolicy retryPolicy = _retryPolicyFactory.Create();
                try
                {
                    return await GetRequestAndDeserializeAsync<org.apache.hadoop.hbase.rest.protobuf.generated.Version>("version");
                }
                catch (Exception e)
                {
                    if (!retryPolicy.ShouldRetryAttempt(e))
                    {
                        throw;
                    }
                }
            }
        }

        /// <summary>
        /// Lists the tables.
        /// </summary>
        /// <returns></returns>
        public TableList ListTables()
        {
            return ListTablesAsync().Result;
        }

        /// <summary>
        /// Lists the tables asynchronously.
        /// </summary>
        /// <returns>
        /// </returns>
        public async Task<TableList> ListTablesAsync()
        {
            while (true)
            {
                IRetryPolicy retryPolicy = _retryPolicyFactory.Create();
                try
                {
                    return await GetRequestAndDeserializeAsync<TableList>("");
                }

                catch (Exception e)
                {
                    if (!retryPolicy.ShouldRetryAttempt(e))
                    {
                        throw;
                    }
                }
            }
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
            table.ArgumentNotNullNorEmpty("table");
            schema.ArgumentNotNull("schema");

            while (true)
            {
                IRetryPolicy retryPolicy = _retryPolicyFactory.Create();
                try
                {
                    using (HttpWebResponse webResponse = await PostRequestAsync(table + "/schema", schema))
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
                        else
                        {
                            return;
                        }
                    }
                }
                catch (Exception e)
                {
                    if (!retryPolicy.ShouldRetryAttempt(e))
                    {
                        throw;
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
            scannerInfo.ArgumentNotNull("scannerInfo");

            while (true)
            {
                IRetryPolicy retryPolicy = _retryPolicyFactory.Create();
                try
                {
                    using (
                       HttpWebResponse webResponse =
                          await GetRequestAsync(scannerInfo.TableName + "/scanner/" + scannerInfo.ScannerId, WebRequester.RestEndpointBaseZero))
                    {
                        if (webResponse.StatusCode == HttpStatusCode.OK)
                        {
                            return Serializer.Deserialize<CellSet>(webResponse.GetResponseStream());
                        }

                        return null;
                    }
                }
                catch (Exception e)
                {
                    if (!retryPolicy.ShouldRetryAttempt(e))
                    {
                        throw;
                    }
                }
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
            table.ArgumentNotNullNorEmpty("table");
            cells.ArgumentNotNull("cells");

            while (true)
            {
                IRetryPolicy retryPolicy = _retryPolicyFactory.Create();
                try
                {
                    // note the fake row key to insert a set of cells
                    using (HttpWebResponse webResponse = await PutRequestAsync(table + "/somefalsekey", cells))
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
                        else
                        {
                            return;
                        }
                    }
                }
                catch (Exception e)
                {
                    if (!retryPolicy.ShouldRetryAttempt(e))
                    {
                        throw;
                    }
                }
            }
        }

        private async Task<HttpWebResponse> DeleteRequestAsync<TReq>(string endpoint, TReq request, string alternativeEndpointBase = null)
           where TReq : class
        {
            return await ExecuteMethodAsync("DELETE", endpoint, request, alternativeEndpointBase);
        }

        private async Task<HttpWebResponse> ExecuteMethodAsync<TReq>(
           string method,
           string endpoint,
           TReq request,
           string alternativeEndpointBase = null) where TReq : class
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

        private async Task<T> GetRequestAndDeserializeAsync<T>(string endpoint, string alternativeEndpointBase = null)
        {
            using (HttpWebResponse response = await _requester.IssueWebRequestAsync(endpoint, "GET", alternativeEndpointBase: alternativeEndpointBase))
            {
                using (Stream responseStream = response.GetResponseStream())
                {
                    return Serializer.Deserialize<T>(responseStream);
                }
            }
        }

        private async Task<HttpWebResponse> GetRequestAsync(string endpoint, string alternativeEndpointBase = null)
        {
            return await _requester.IssueWebRequestAsync(endpoint, "GET", null, alternativeEndpointBase: alternativeEndpointBase);
        }

        private async Task<HttpWebResponse> PostRequestAsync<TReq>(string endpoint, TReq request, string alternativeEndpointBase = null)
           where TReq : class
        {
            return await ExecuteMethodAsync("POST", endpoint, request, alternativeEndpointBase);
        }

        private async Task<HttpWebResponse> PutRequestAsync<TReq>(string endpoint, TReq request, string alternativeEndpointBase = null)
           where TReq : class
        {
            return await ExecuteMethodAsync("PUT", endpoint, request, alternativeEndpointBase);
        }
    }
}