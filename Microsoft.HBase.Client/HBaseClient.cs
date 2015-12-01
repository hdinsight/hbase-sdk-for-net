﻿// Copyright (c) Microsoft Corporation
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
    using System.Diagnostics;
    using System.Diagnostics.Contracts;
    using System.IO;
    using System.Net;
    using System.Threading.Tasks;
    using Microsoft.HBase.Client.Internal;
    using Microsoft.HBase.Client.LoadBalancing;
    using Microsoft.HBase.Client.Requester;
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
        private readonly IWebRequester _requester;
        private readonly RequestOptions _globalRequestOptions;

        /// <summary>
        /// Initializes a new instance of the <see cref="HBaseClient"/> class.
        /// </summary>
        /// <param name="credentials">The credentials.</param>
        public HBaseClient(ClusterCredentials credentials)
            : this(credentials, RequestOptions.GetDefaultOptions())
        { }

        /// <summary>
        /// Initializes a new instance of the <see cref="HBaseClient"/> class.
        /// </summary>
        /// <remarks>
        /// To find the cluster vnet domain visit:
        /// https://azure.microsoft.com/en-us/documentation/articles/hdinsight-hbase-provision-vnet/
        /// </remarks>
        /// <param name="numRegionServers">The number of region servers in the cluster.</param>
        /// <param name="clusterDomain">The fully-qualified domain name of the cluster.</param>
        public HBaseClient(int numRegionServers, string clusterDomain = null)
            : this(null, RequestOptions.GetDefaultOptions(), new LoadBalancerRoundRobin(numRegionServers: numRegionServers, clusterDomain: clusterDomain))
        { }

        /// <summary>
        /// Initializes a new instance of the <see cref="HBaseClient"/> class.
        /// </summary>
        /// <param name="credentials">The credentials.</param>
        /// <param name="globalRequestOptions">The global request options.</param>
        /// <param name="loadBalancer">load balancer for vnet modes</param>
        public HBaseClient(ClusterCredentials credentials, RequestOptions globalRequestOptions = null, ILoadBalancer loadBalancer = null)
        {
            _globalRequestOptions = globalRequestOptions ?? RequestOptions.GetDefaultOptions();

            if (credentials != null) // gateway mode
            {
                _requester = new GatewayWebRequester(credentials);
            }
            else // vnet mode
            {
                _requester = new VNetWebRequester(loadBalancer);
            }
        }

        /// <summary>
        /// Creates a scanner on the server side.
        /// The resulting ScannerInformation can be used to read query the CellSets returned by this scanner in the #ScannerGetNext/Async method.
        /// </summary>
        /// <param name="tableName">the table to scan</param>
        /// <param name="scannerSettings">the settings to e.g. set the batch size of this scan</param>
        /// <returns>A ScannerInformation which contains the continuation url/token and the table name</returns>
        public ScannerInformation CreateScanner(string tableName, Scanner scannerSettings, RequestOptions options = null)
        {
            return CreateScannerAsync(tableName, scannerSettings, options).Result;
        }

        /// <summary>
        /// Creates a scanner on the server side.
        /// The resulting ScannerInformation can be used to read query the CellSets returned by this scanner in the #ScannerGetNext/Async method.
        /// </summary>
        /// <param name="tableName">the table to scan</param>
        /// <param name="scannerSettings">the settings to e.g. set the batch size of this scan</param>
        /// <returns>A ScannerInformation which contains the continuation url/token and the table name</returns>
        public async Task<ScannerInformation> CreateScannerAsync(string tableName, Scanner scannerSettings, RequestOptions options = null)
        {
            tableName.ArgumentNotNullNorEmpty("tableName");
            scannerSettings.ArgumentNotNull("scannerSettings");
            var optionToUse = options ?? _globalRequestOptions;
            return await optionToUse.RetryPolicy.ExecuteAsync(() => CreateScannerAsyncInternal(tableName, scannerSettings, optionToUse));
        }

        private async Task<ScannerInformation> CreateScannerAsyncInternal(string tableName, Scanner scannerSettings, RequestOptions options)
        {
            using (HttpWebResponse response = await PostRequestAsync(tableName + "/scanner", scannerSettings, options))
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

        public void DeleteScanner(string tableName, string scannerId, RequestOptions options = null)
        {
            DeleteScannerAsync(tableName, scannerId, options).Wait();
        }

        /// <summary>
        /// Deletes scanner.        
        /// </summary>
        /// <param name="tableName">the table the scanner is associated with.</param>
        /// <param name="scannerId">the id of the scanner to delete.</param>
        public Task DeleteScannerAsync(string tableName, string scannerId, RequestOptions options = null)
        {
            tableName.ArgumentNotNullNorEmpty("tableName");
            scannerId.ArgumentNotNullNorEmpty("scannerId");

            var optionToUse = options ?? _globalRequestOptions;
            return optionToUse.RetryPolicy.ExecuteAsync(() => DeleteScannerAsyncInternal(tableName, scannerId, optionToUse));
        }

        private async Task DeleteScannerAsyncInternal(string tableName, string scannerId, RequestOptions options)
        {
            using (HttpWebResponse webResponse = await DeleteRequestAsync<Scanner>(tableName + "/scanner" + scannerId, null, options))
            {
                if (webResponse.StatusCode != HttpStatusCode.OK)
                {
                    using (var output = new StreamReader(webResponse.GetResponseStream()))
                    {
                        string message = output.ReadToEnd();
                        throw new WebException(
                           string.Format(
                              "Couldn't delete scanner {0} associated with {1} table.! Response code was: {2}, expected 200! Response body was: {3}",
                              scannerId,
                              tableName,
                              webResponse.StatusCode,
                              message));
                    }
                }
            }
        }

        public void DeleteCells(string tableName, string rowKey, RequestOptions options = null)
        {
            DeleteCellsAsync(tableName, rowKey, options).Wait();
        }

        public Task DeleteCellsAsync(string tableName, string rowKey, RequestOptions options = null)
        {
            tableName.ArgumentNotNullNorEmpty("tableName");
            rowKey.ArgumentNotNullNorEmpty("rowKey");
            var optionToUse = options ?? _globalRequestOptions;
            return optionToUse.RetryPolicy.ExecuteAsync(() => DeleteCellsAsyncInternal(tableName, rowKey, optionToUse));
        }

        private async Task DeleteCellsAsyncInternal(string tableName, string rowKey, RequestOptions options)
        {
            using (HttpWebResponse webResponse = await DeleteRequestAsync<Scanner>(tableName + "/" + rowKey, null, options))
            {
                if (webResponse.StatusCode != HttpStatusCode.OK)
                {
                    using (var output = new StreamReader(webResponse.GetResponseStream()))
                    {
                        string message = output.ReadToEnd();
                        throw new WebException(
                            string.Format(
                                "Couldn't delete row {0} associated with {1} table.! Response code was: {2}, expected 200! Response body was: {3}",
                                rowKey,
                                tableName,
                                webResponse.StatusCode,
                                message));
                    }
                }
            }
        }

        /// <summary>
        /// Creates a table and/or fully replaces its schema.
        /// </summary>
        /// <param name="schema">the schema</param>
        /// <returns>returns true if the table was created, false if the table already exists. In case of any other error it throws a WebException.</returns>
        public bool CreateTable(TableSchema schema, RequestOptions options = null)
        {
            return CreateTableAsync(schema, options).Result;
        }

        /// <summary>
        /// Creates a table and/or fully replaces its schema.
        /// </summary>
        /// <param name="schema">the schema</param>
        /// <returns>returns true if the table was created, false if the table already exists. In case of any other error it throws a WebException.</returns>
        public async Task<bool> CreateTableAsync(TableSchema schema, RequestOptions options = null)
        {
            schema.ArgumentNotNull("schema");
            var optionToUse = options ?? _globalRequestOptions;
            return await optionToUse.RetryPolicy.ExecuteAsync(() => CreateTableAsyncInternal(schema, optionToUse));
        }

        private async Task<bool> CreateTableAsyncInternal(TableSchema schema, RequestOptions options)
        {
            if (string.IsNullOrEmpty(schema.name))
            {
                throw new ArgumentException("schema.name was either null or empty!", "schema");
            }

            using (HttpWebResponse webResponse = await PutRequestAsync(schema.name + "/schema", schema, options))
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
        public void DeleteTable(string tableName, RequestOptions options = null)
        {
            DeleteTableAsync(tableName, options).Wait();
        }

        /// <summary>
        /// Deletes a table.
        /// If something went wrong, a WebException is thrown.
        /// </summary>
        /// <param name="table">the table name</param>
        public async Task DeleteTableAsync(string table, RequestOptions options = null)
        {
            table.ArgumentNotNullNorEmpty("table");
            var optionToUse = options ?? _globalRequestOptions;
            await optionToUse.RetryPolicy.ExecuteAsync(() => DeleteTableAsyncInternal(table, optionToUse));
        }

        public async Task DeleteTableAsyncInternal(string table, RequestOptions options)
        {
            using (HttpWebResponse webResponse = await DeleteRequestAsync<TableSchema>(table + "/schema", null, options))
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

        /// <summary>
        /// Gets the cells.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="rowKey">The row key.</param>
        /// <returns></returns>
        public CellSet GetCells(string tableName, string rowKey, RequestOptions options = null)
        {
            return GetCellsAsync(tableName, rowKey, options).Result;
        }

        /// <summary>
        /// Gets the cells asynchronously.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="rowKey">The row key.</param>
        /// <returns></returns>
        public async Task<CellSet> GetCellsAsync(string tableName, string rowKey, RequestOptions options = null)
        {
            tableName.ArgumentNotNullNorEmpty("tableName");
            rowKey.ArgumentNotNull("rowKey");

            var optionToUse = options ?? _globalRequestOptions;
            return await optionToUse.RetryPolicy.ExecuteAsync(() => GetRequestAndDeserializeAsync<CellSet>(tableName + "/" + rowKey, optionToUse));
        }

        /// <summary>
        /// Gets the storage cluster status.
        /// </summary>
        /// <returns>
        /// </returns>
        public StorageClusterStatus GetStorageClusterStatus(RequestOptions options = null)
        {
            return GetStorageClusterStatusAsync(options).Result;
        }

        /// <summary>
        /// Gets the storage cluster status asynchronous.
        /// </summary>
        /// <returns>
        /// </returns>
        public async Task<StorageClusterStatus> GetStorageClusterStatusAsync(RequestOptions options = null)
        {
            var optionToUse = options ?? _globalRequestOptions;
            return await optionToUse.RetryPolicy.ExecuteAsync(() => GetRequestAndDeserializeAsync<StorageClusterStatus>("/status/cluster", options));
        }

        /// <summary>
        /// Gets the table information.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns>
        /// </returns>
        public TableInfo GetTableInfo(string table, RequestOptions options = null)
        {
            return GetTableInfoAsync(table, options).Result;
        }

        /// <summary>
        /// Gets the table information asynchronously.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns></returns>
        public async Task<TableInfo> GetTableInfoAsync(string table, RequestOptions options = null)
        {
            table.ArgumentNotNullNorEmpty("table");
            var optionToUse = options ?? _globalRequestOptions;
            return await optionToUse.RetryPolicy.ExecuteAsync(() => GetRequestAndDeserializeAsync<TableInfo>(table + "/regions", optionToUse));
        }

        /// <summary>
        /// Gets the table schema.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns>
        /// </returns>
        public TableSchema GetTableSchema(string table, RequestOptions options = null)
        {
            return GetTableSchemaAsync(table, options).Result;
        }

        /// <summary>
        /// Gets the table schema asynchronously.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns>
        /// </returns>
        public async Task<TableSchema> GetTableSchemaAsync(string table, RequestOptions options = null)
        {
            table.ArgumentNotNullNorEmpty("table");
            var optionToUse = options ?? _globalRequestOptions;
            return await optionToUse.RetryPolicy.ExecuteAsync(() => GetRequestAndDeserializeAsync<TableSchema>(table + "/schema", optionToUse));
        }

        /// <summary>
        /// Gets the version.
        /// </summary>
        /// <returns>
        /// </returns>
        public org.apache.hadoop.hbase.rest.protobuf.generated.Version GetVersion(RequestOptions options = null)
        {
            return GetVersionAsync(options).Result;
        }

        /// <summary>
        /// Gets the version asynchronously.
        /// </summary>
        /// <returns>
        /// </returns>
        public async Task<org.apache.hadoop.hbase.rest.protobuf.generated.Version> GetVersionAsync(RequestOptions options = null)
        {
            var optionToUse = options ?? _globalRequestOptions;
            return await optionToUse.RetryPolicy.ExecuteAsync(() => GetRequestAndDeserializeAsync<org.apache.hadoop.hbase.rest.protobuf.generated.Version>("version", optionToUse));
        }

        /// <summary>
        /// Lists the tables.
        /// </summary>
        /// <returns></returns>
        public TableList ListTables(RequestOptions options = null)
        {
            return ListTablesAsync().Result;
        }

        /// <summary>
        /// Lists the tables asynchronously.
        /// </summary>
        /// <returns>
        /// </returns>
        public async Task<TableList> ListTablesAsync(RequestOptions options = null)
        {
            var optionToUse = options ?? _globalRequestOptions;
            return await optionToUse.RetryPolicy.ExecuteAsync(() => GetRequestAndDeserializeAsync<TableList>("", optionToUse));
        }

        /// <summary>
        /// Modifies a table schema. 
        /// If necessary it creates a new table with the given schema. 
        /// If something went wrong, a WebException is thrown.
        /// </summary>
        /// <param name="tableName">the table name</param>
        /// <param name="schema">the schema</param>
        public void ModifyTableSchema(string tableName, TableSchema schema, RequestOptions options = null)
        {
            ModifyTableSchemaAsync(tableName, schema, options).Wait();
        }

        /// <summary>
        /// Modifies a table schema. 
        /// If necessary it creates a new table with the given schema. 
        /// If something went wrong, a WebException is thrown.
        /// </summary>
        /// <param name="table">the table name</param>
        /// <param name="schema">the schema</param>
        public async Task ModifyTableSchemaAsync(string table, TableSchema schema, RequestOptions options = null)
        {
            table.ArgumentNotNullNorEmpty("table");
            schema.ArgumentNotNull("schema");

            var optionToUse = options ?? _globalRequestOptions;
            await optionToUse.RetryPolicy.ExecuteAsync(() => ModifyTableSchemaAsyncInternal(table, schema, optionToUse));
        }

        private async Task ModifyTableSchemaAsyncInternal(string table, TableSchema schema, RequestOptions options)
        {
            using (HttpWebResponse webResponse = await PostRequestAsync(table + "/schema", schema, options))
            {
                if (webResponse.StatusCode != HttpStatusCode.OK || webResponse.StatusCode != HttpStatusCode.Created)
                {
                    using (var output = new StreamReader(webResponse.GetResponseStream()))
                    {
                        string message = output.ReadToEnd();
                        throw new WebException(
                            string.Format(
                                "Couldn't modify table schema {0}! Response code was: {1}, expected either 200 or 201! Response body was: {2}",
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
        public CellSet ScannerGetNext(ScannerInformation scannerInfo, RequestOptions options = null)
        {
            return ScannerGetNextAsync(scannerInfo, options).Result;
        }

        /// <summary>
        /// Scans the next set of messages.
        /// </summary>
        /// <param name="scannerInfo">the scanner information retrieved by #CreateScanner()</param>
        /// <returns>a cellset, or null if the scanner is exhausted</returns>
        public async Task<CellSet> ScannerGetNextAsync(ScannerInformation scannerInfo, RequestOptions options = null)
        {
            scannerInfo.ArgumentNotNull("scannerInfo");
            var optionToUse = options ?? _globalRequestOptions;
            return await optionToUse.RetryPolicy.ExecuteAsync(() => ScannerGetNextAsyncInternal(scannerInfo, optionToUse));
        }

        private async Task<CellSet> ScannerGetNextAsyncInternal(ScannerInformation scannerInfo, RequestOptions options)
        {
            using (HttpWebResponse webResponse = await GetRequestAsync(scannerInfo.TableName + "/scanner/" + scannerInfo.ScannerId, options))
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
        public void StoreCells(string table, CellSet cells, RequestOptions options = null)
        {
            StoreCellsAsync(table, cells, options).Wait();
        }

        /// <summary>
        /// Stores the given cells in the supplied table.
        /// </summary>
        /// <param name="table">the table</param>
        /// <param name="cells">the cells to insert</param>
        /// <returns>a task that is awaitable, signifying the end of this operation</returns>
        public async Task StoreCellsAsync(string table, CellSet cells, RequestOptions options = null)
        {
            table.ArgumentNotNullNorEmpty("table");
            cells.ArgumentNotNull("cells");

            var optionToUse = options ?? _globalRequestOptions;
            await optionToUse.RetryPolicy.ExecuteAsync(() => StoreCellsAsyncInternal(table, cells, optionToUse));
        }

        private async Task StoreCellsAsyncInternal(string table, CellSet cells, RequestOptions options)
        {
            // note the fake row key to insert a set of cells
            using (HttpWebResponse webResponse = await PutRequestAsync(table + "/somefalsekey", cells, options))
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

        private async Task<HttpWebResponse> DeleteRequestAsync<TReq>(string endpoint, TReq request, RequestOptions options)
           where TReq : class
        {
            return await ExecuteMethodAsync("DELETE", endpoint, request, options);
        }

        private async Task<HttpWebResponse> ExecuteMethodAsync<TReq>(
           string method,
           string endpoint,
           TReq request,
           RequestOptions options) where TReq : class
        {
            using (var input = new MemoryStream(options.SerializationBufferSize))
            {
                if (request != null)
                {
                    Serializer.Serialize(input, request);
                }
                return await _requester.IssueWebRequestAsync(endpoint, method, input, options);
            }
        }

        private async Task<T> GetRequestAndDeserializeAsync<T>(string endpoint, RequestOptions options)
        {
            using (HttpWebResponse response = await _requester.IssueWebRequestAsync(endpoint, "GET", null, options))
            {
                using (Stream responseStream = response.GetResponseStream())
                {
                    return Serializer.Deserialize<T>(responseStream);
                }
            }
        }

        private async Task<HttpWebResponse> GetRequestAsync(string endpoint, RequestOptions options)
        {
            return await _requester.IssueWebRequestAsync(endpoint, "GET", null, options);
        }

        private async Task<HttpWebResponse> PostRequestAsync<TReq>(string endpoint, TReq request, RequestOptions options)
           where TReq : class
        {
            return await ExecuteMethodAsync("POST", endpoint, request, options);
        }

        private async Task<HttpWebResponse> PutRequestAsync<TReq>(string endpoint, TReq request, RequestOptions options)
           where TReq : class
        {
            return await ExecuteMethodAsync("PUT", endpoint, request, options);
        }
    }
}