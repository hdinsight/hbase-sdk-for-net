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
    using System.Diagnostics;
    using System.Diagnostics.Contracts;
    using System.IO;
    using System.Net;
    using System.Threading.Tasks;
    using Microsoft.HBase.Client.Internal;
    using Microsoft.HBase.Client.LoadBalancing;
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
        private readonly IRetryPolicyFactory _retryPolicyFactory;
        private ILoadBalancer _loadBalancer;
        
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
        public HBaseClient(int numRegionServers)
            : this(null, new DefaultRetryPolicyFactory(), new LoadBalancerRoundRobin(numRegionServers: numRegionServers))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="HBaseClient"/> class.
        /// </summary>
        /// <param name="credentials">The credentials.</param>
        /// <param name="retryPolicyFactory">The retry policy factory.</param>
        public HBaseClient(ClusterCredentials credentials, IRetryPolicyFactory retryPolicyFactory, ILoadBalancer loadBalancer = null)
        {
            retryPolicyFactory.ArgumentNotNull("retryPolicyFactory");

            if (credentials != null)
            {
                _requester = new WebRequesterSecure(credentials);
            }
            else
            {
                _requester = new WebRequesterBasic();
                _loadBalancer = loadBalancer;
            }
            
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
            return ExecuteAndGetWithVirtualNetworkLoadBalancing<string, Scanner, ScannerInformation>(CreateScannerAsyncInternal, tableName, scannerSettings);
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
            return await CreateScannerAsyncInternal(tableName, scannerSettings);   
        }

        private async Task<ScannerInformation> CreateScannerAsyncInternal(string tableName, Scanner scannerSettings, string alternativeEndpointBase = null)
        {
            tableName.ArgumentNotNullNorEmpty("tableName");
            scannerSettings.ArgumentNotNull("scannerSettings");

            while (true)
            {
                IRetryPolicy retryPolicy = _retryPolicyFactory.Create();
                try
                {
                    using (HttpWebResponse response = await PostRequestAsync(tableName + "/scanner", scannerSettings, alternativeEndpointBase ?? Constants.RestEndpointBaseZero))
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
            return ExecuteAndGetWithVirtualNetworkLoadBalancing<TableSchema, bool>(CreateTableAsyncInternal, schema);
        }

        /// <summary>
        /// Creates a table and/or fully replaces its schema.
        /// </summary>
        /// <param name="schema">the schema</param>
        /// <returns>returns true if the table was created, false if the table already exists. In case of any other error it throws a WebException.</returns>
        public async Task<bool> CreateTableAsync(TableSchema schema)
        {
            return await CreateTableAsyncInternal(schema);
        }

        private async Task<bool> CreateTableAsyncInternal(TableSchema schema, string alternativeEndpointBase = null)
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
                    using (HttpWebResponse webResponse = await PutRequestAsync(schema.name + "/schema", schema, alternativeEndpointBase))
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
            ExecuteWithVirtualNetworkLoadBalancing<string>(DeleteTableAsyncInternal, tableName);
        }

        /// <summary>
        /// Deletes a table.
        /// If something went wrong, a WebException is thrown.
        /// </summary>
        /// <param name="table">the table name</param>

        public async Task DeleteTableAsync(string table)
        {
            await DeleteTableAsyncInternal(table);
        }

        public async Task DeleteTableAsyncInternal(string table, string alternativeEndpointBase = null)
        {
            table.ArgumentNotNullNorEmpty("table");

            while (true)
            {
                IRetryPolicy retryPolicy = _retryPolicyFactory.Create();
                try
                {
                    using (HttpWebResponse webResponse = await DeleteRequestAsync<TableSchema>(table + "/schema", null, alternativeEndpointBase))
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
            return ExecuteAndGetWithVirtualNetworkLoadBalancing<string, string, CellSet>(GetCellsAsyncInternal, tableName, rowKey);
        }

        /// <summary>
        /// Gets the cells asynchronously.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="rowKey">The row key.</param>
        /// <returns></returns>
        public async Task<CellSet> GetCellsAsync(string tableName, string rowKey)
        {
            return await GetCellsAsyncInternal(tableName, rowKey);
        }

        private async Task<CellSet> GetCellsAsyncInternal(string tableName, string rowKey, string alternativeEndpointBase = null)
        {
            // TODO add timestamp, versions and column queries
            tableName.ArgumentNotNullNorEmpty("tableName");
            rowKey.ArgumentNotNull("rowKey");

            while (true)
            {
                IRetryPolicy retryPolicy = _retryPolicyFactory.Create();
                try
                {
                    return await GetRequestAndDeserializeAsync<CellSet>(tableName + "/" + rowKey, alternativeEndpointBase);
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
            return ExecuteAndGetWithVirtualNetworkLoadBalancing<StorageClusterStatus>(GetStorageClusterStatusAsyncInternal);
        }

        /// <summary>
        /// Gets the storage cluster status asynchronous.
        /// </summary>
        /// <returns>
        /// </returns>
        public async Task<StorageClusterStatus> GetStorageClusterStatusAsync()
        {
            return await GetStorageClusterStatusAsync();   
        }

        private async Task<StorageClusterStatus> GetStorageClusterStatusAsyncInternal(string alternativeEndpointBase = null)
        {
            while (true)
            {
                IRetryPolicy retryPolicy = _retryPolicyFactory.Create();
                try
                {
                    return await GetRequestAndDeserializeAsync<StorageClusterStatus>("/status/cluster", alternativeEndpointBase);
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
            return ExecuteAndGetWithVirtualNetworkLoadBalancing<string, TableInfo>(GetTableInfoAsyncInternal, table);
        }

        /// <summary>
        /// Gets the table information asynchronously.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns></returns>
        public async Task<TableInfo> GetTableInfoAsync(string table)
        {
            return await GetTableInfoAsyncInternal(table);
        }

        private async Task<TableInfo> GetTableInfoAsyncInternal(string table, string alternativeEndpointBase = null)
        {
            table.ArgumentNotNullNorEmpty("table");

            while (true)
            {
                IRetryPolicy retryPolicy = _retryPolicyFactory.Create();
                try
                {
                    return await GetRequestAndDeserializeAsync<TableInfo>(table + "/regions", alternativeEndpointBase);
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
            return ExecuteAndGetWithVirtualNetworkLoadBalancing<string, TableSchema>(GetTableSchemaAsyncInternal, table);
        }

        /// <summary>
        /// Gets the table schema asynchronously.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns>
        /// </returns>
        public async Task<TableSchema> GetTableSchemaAsync(string table)
        {
            return await GetTableSchemaAsyncInternal(table);
        }
        
        private async Task<TableSchema> GetTableSchemaAsyncInternal(string table, string alternativeEndpointBase = null)
        {
            table.ArgumentNotNullNorEmpty("table");

            while (true)
            {
                IRetryPolicy retryPolicy = _retryPolicyFactory.Create();
                try
                {
                    return await GetRequestAndDeserializeAsync<TableSchema>(table + "/schema", alternativeEndpointBase);
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
            return ExecuteAndGetWithVirtualNetworkLoadBalancing<org.apache.hadoop.hbase.rest.protobuf.generated.Version>(GetVersionAsyncInternal);
        }

        /// <summary>
        /// Gets the version asynchronously.
        /// </summary>
        /// <returns>
        /// </returns>
        public async Task<org.apache.hadoop.hbase.rest.protobuf.generated.Version> GetVersionAsync()
        {
            return await GetVersionAsyncInternal();
        }

        private async Task<org.apache.hadoop.hbase.rest.protobuf.generated.Version> GetVersionAsyncInternal(string alternativeEndpointBase = null)
        {
            while (true)
            {
                IRetryPolicy retryPolicy = _retryPolicyFactory.Create();
                try
                {
                    return await GetRequestAndDeserializeAsync<org.apache.hadoop.hbase.rest.protobuf.generated.Version>("version", alternativeEndpointBase);
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
            return ExecuteAndGetWithVirtualNetworkLoadBalancing<TableList>(ListTablesAsyncInternal);
        }

        /// <summary>
        /// Lists the tables asynchronously.
        /// </summary>
        /// <returns>
        /// </returns>
        public async Task<TableList> ListTablesAsync()
        {
            return await ListTablesAsyncInternal();
        }

        private async Task<TableList> ListTablesAsyncInternal(string alternativeEndpointBase = null)
        {
            while (true)
            {
                IRetryPolicy retryPolicy = _retryPolicyFactory.Create();
                try
                {
                    return await GetRequestAndDeserializeAsync<TableList>("", alternativeEndpointBase: alternativeEndpointBase);
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
            ExecuteWithVirtualNetworkLoadBalancing<string, TableSchema>(ModifyTableSchemaAsyncInternal, tableName, schema);
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
            await ModifyTableSchemaAsyncInternal(table, schema);
        }

        private async Task ModifyTableSchemaAsyncInternal(string table, TableSchema schema, string alternativeEndpointBase = null)
        {
            table.ArgumentNotNullNorEmpty("table");
            schema.ArgumentNotNull("schema");

            while (true)
            {
                IRetryPolicy retryPolicy = _retryPolicyFactory.Create();
                try
                {
                    using (HttpWebResponse webResponse = await PostRequestAsync(table + "/schema", schema, alternativeEndpointBase))
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
            return ExecuteAndGetWithVirtualNetworkLoadBalancing<ScannerInformation, CellSet>(ScannerGetNextAsyncInternal, scannerInfo);
        }

        /// <summary>
        /// Scans the next set of messages.
        /// </summary>
        /// <param name="scannerInfo">the scanner information retrieved by #CreateScanner()</param>
        /// <returns>a cellset, or null if the scanner is exhausted</returns>
        public async Task<CellSet> ScannerGetNextAsync(ScannerInformation scannerInfo)
        {
            return await ScannerGetNextAsyncInternal(scannerInfo);
        }

        private async Task<CellSet> ScannerGetNextAsyncInternal(ScannerInformation scannerInfo, string alternativeEndpointBase = null)
        {
            scannerInfo.ArgumentNotNull("scannerInfo");
            
            while (true)
            {
                IRetryPolicy retryPolicy = _retryPolicyFactory.Create();
                try
                {
                    using (
                       HttpWebResponse webResponse =
                          await GetRequestAsync(scannerInfo.TableName + "/scanner/" + scannerInfo.ScannerId, alternativeEndpointBase ?? Constants.RestEndpointBaseZero))
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
            ExecuteWithVirtualNetworkLoadBalancing<string, CellSet>(StoreCellsAsyncInternal, table, cells);
        }

        /// <summary>
        /// Stores the given cells in the supplied table.
        /// </summary>
        /// <param name="table">the table</param>
        /// <param name="cells">the cells to insert</param>
        /// <returns>a task that is awaitable, signifying the end of this operation</returns>
        public async Task StoreCellsAsync(string table, CellSet cells)
        {
            await StoreCellsAsyncInternal(table, cells);
        }

        private async Task StoreCellsAsyncInternal(string table, CellSet cells, string alternativeEndpointBase = null)
        {
            table.ArgumentNotNullNorEmpty("table");
            cells.ArgumentNotNull("cells");

            while (true)
            {
                IRetryPolicy retryPolicy = _retryPolicyFactory.Create();
                try
                {
                    // note the fake row key to insert a set of cells
                    using (HttpWebResponse webResponse = await PutRequestAsync(table + "/somefalsekey", cells, alternativeEndpointBase))
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
            using (HttpWebResponse response = await _requester.IssueWebRequestAsync(endpoint, "GET", input: null, alternativeEndpointBase: alternativeEndpointBase))
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


        // Executes the asynchronous method with load balancing in case of virtual network
        internal TResult ExecuteAndGetWithVirtualNetworkLoadBalancing<TResult>(Func<string, Task<TResult>> method)
        {
            if (_loadBalancer == null)
            {
                return method.Invoke(null).Result;
            }
            else
            {
                TResult result = default(TResult);

                int numRetries = _loadBalancer.GetWorkersCount();
                LoadBalancingHelper.Execute(() =>
                {
                    var endpoint = _loadBalancer.GetWorkerNodeEndPointBaseNext().ToString();
                    Contract.Assert(endpoint != null, "Load balancer failed to return a worker node endpoint!");

                    Trace.TraceInformation("\tIssuing request to endpoint " + endpoint);

                    result = method.Invoke(endpoint).Result;
                },
                new RetryOnAllExceptionsPolicy(),
                new NoOpBackOffScheme(), numRetries: numRetries);

                return result;
            }
        }

        internal TResult ExecuteAndGetWithVirtualNetworkLoadBalancing<TArg, TResult>(Func<TArg, string, Task<TResult>> method, TArg arg1)
        {
            if (_loadBalancer == null)
            {
                return method.Invoke(arg1, null).Result;
            }
            else
            {
                TResult result = default(TResult);

                int numRetries = _loadBalancer.GetWorkersCount();
                LoadBalancingHelper.Execute(() =>
                {
                    var endpoint = _loadBalancer.GetWorkerNodeEndPointBaseNext().ToString();
                    Contract.Assert(endpoint != null, "Load balancer failed to return a worker node endpoint!");

                    Trace.TraceInformation("\tIssuing request to endpoint " + endpoint);

                    result = method.Invoke(arg1, endpoint).Result;
                },
                new RetryOnAllExceptionsPolicy(),
                new NoOpBackOffScheme(), numRetries: numRetries);

                return result;
            }
        }

        internal TResult ExecuteAndGetWithVirtualNetworkLoadBalancing<TArgA, TArgB, TResult>(Func<TArgA, TArgB, string, Task<TResult>> method, TArgA arg1, TArgB arg2)
        {
            if (_loadBalancer == null)
            {
                return method.Invoke(arg1, arg2, null).Result;
            }
            else
            {
                TResult result = default(TResult);

                int numRetries = _loadBalancer.GetWorkersCount();
                LoadBalancingHelper.Execute(() =>
                {
                    var endpoint = _loadBalancer.GetWorkerNodeEndPointBaseNext().ToString();
                    Contract.Assert(endpoint != null, "Load balancer failed to return a worker node endpoint!");

                    Trace.TraceInformation("\tIssuing request to endpoint " + endpoint);

                    result = method.Invoke(arg1, arg2, endpoint).Result;
                },
                new RetryOnAllExceptionsPolicy(),
                new NoOpBackOffScheme(), numRetries: numRetries);

                return result;
            }
        }


        // Executes the asynchronous method with load balancing in case of virtual network
        internal void ExecuteWithVirtualNetworkLoadBalancing(Func<string, Task> method)
        {
            if (_loadBalancer == null)
            {
                method.Invoke(null).Wait();
            }
            else
            {
                int numRetries = _loadBalancer.GetWorkersCount();
                LoadBalancingHelper.Execute(() =>
                {
                    var endpoint = _loadBalancer.GetWorkerNodeEndPointBaseNext().ToString();
                    Contract.Assert(endpoint != null, "Load balancer failed to return a worker node endpoint!");

                    Trace.TraceInformation("\tIssuing request to endpoint " + endpoint);

                    method.Invoke(endpoint).Wait();
                },
                new RetryOnAllExceptionsPolicy(),
                new NoOpBackOffScheme(), numRetries: numRetries);
            }
        }

        internal void ExecuteWithVirtualNetworkLoadBalancing<TArg>(Func<TArg, string, Task> method, TArg arg)
        {
            if (_loadBalancer == null)
            {
                method.Invoke(arg, null).Wait();
            }
            else
            {
                int numRetries = _loadBalancer.GetWorkersCount();
                LoadBalancingHelper.Execute(() =>
                {
                    var endpoint = _loadBalancer.GetWorkerNodeEndPointBaseNext().ToString();
                    Contract.Assert(endpoint != null, "Load balancer failed to return a worker node endpoint!");

                    Trace.TraceInformation("\tIssuing request to endpoint " + endpoint);

                    method.Invoke(arg, endpoint).Wait();
                },
                new RetryOnAllExceptionsPolicy(),
                new NoOpBackOffScheme(), numRetries: numRetries);
            }
        }

        internal void ExecuteWithVirtualNetworkLoadBalancing<TArgA, TArgB>(Func<TArgA, TArgB, string, Task> method, TArgA arg1, TArgB arg2)
        {
            if (_loadBalancer == null)
            {
                method.Invoke(arg1, arg2, null).Wait();
            }
            else
            {
                int numRetries = _loadBalancer.GetWorkersCount();
                LoadBalancingHelper.Execute(() =>
                {
                    var endpoint = _loadBalancer.GetWorkerNodeEndPointBaseNext().ToString();
                    Contract.Assert(endpoint != null, "Load balancer failed to return a worker node endpoint!");

                    Trace.TraceInformation("\tIssuing request to endpoint " + endpoint);

                    method.Invoke(arg1, arg2, endpoint).Wait();
                },
                new RetryOnAllExceptionsPolicy(),
                new NoOpBackOffScheme(), numRetries: numRetries);
            }
        }
    }
}