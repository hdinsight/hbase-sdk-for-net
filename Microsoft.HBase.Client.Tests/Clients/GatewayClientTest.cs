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

namespace Microsoft.HBase.Client.Tests.Clients
{
    using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.HBase.Client.Tests.Utilities;
    using org.apache.hadoop.hbase.rest.protobuf.generated;
    using Microsoft.HBase.Client.LoadBalancing;
    using System.Collections.Generic;
    using System.Linq;
    using System;

    [TestClass]
    public class GatewayClientTest : HBaseClientTestBase
    {
        public override IHBaseClient CreateClient()
        {
            var options = RequestOptions.GetDefaultOptions();
            options.RetryPolicy = RetryPolicy.NoRetry;
            options.TimeoutMillis = 30000;
            options.KeepAlive = false;
            return new HBaseClient(ClusterCredentialsFactory.CreateFromFile(@".\credentials.txt"), options);
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public override void TestFullScan()
        {
            var client = CreateClient();

            StoreTestData(client);

            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;

            // full range scan
            var scanSettings = new Scanner { batch = 10 };
            ScannerInformation scannerInfo = null;
            try
            {
                scannerInfo = client.CreateScannerAsync(testTableName, scanSettings, scanOptions).Result;

                CellSet next;
                var expectedSet = new HashSet<int>(Enumerable.Range(0, 100));
                while ((next = client.ScannerGetNextAsync(scannerInfo, scanOptions).Result) != null)
                {
                    Assert.AreEqual(10, next.rows.Count);
                    foreach (CellSet.Row row in next.rows)
                    {
                        int k = BitConverter.ToInt32(row.key, 0);
                        expectedSet.Remove(k);
                    }
                }
                Assert.AreEqual(0, expectedSet.Count, "The expected set wasn't empty! Items left {0}!", string.Join(",", expectedSet));
            }
            finally
            {
                if (scannerInfo != null)
                {
                    client.DeleteScannerAsync(testTableName, scannerInfo, scanOptions).Wait();
                }
            }
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void TestScannerCreation()
        {
            var client = CreateClient();
            var scanSettings = new Scanner { batch = 2 };

            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scannerInfo = null;
            try
            {
                scannerInfo = client.CreateScannerAsync(testTableName, scanSettings, scanOptions).Result;
                Assert.AreEqual(testTableName, scannerInfo.TableName);
                Assert.IsNotNull(scannerInfo.ScannerId);
                Assert.IsFalse(scannerInfo.ScannerId.StartsWith("/"), "scanner id starts with a slash");
                Assert.IsNotNull(scannerInfo.ResponseHeaderCollection);
            }
            finally
            {
                if (scannerInfo != null)
                {
                    client.DeleteScannerAsync(testTableName, scannerInfo, scanOptions).Wait();
                }
            }
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        [ExpectedException(typeof(System.AggregateException), "The remote server returned an error: (404) Not Found.")]
        public void TestScannerDeletion()
        {
            var client = CreateClient();

            // full range scan
            var scanSettings = new Scanner { batch = 10 };
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scannerInfo = null;

            try
            {
                scannerInfo = client.CreateScannerAsync(testTableName, scanSettings, scanOptions).Result;
                Assert.AreEqual(testTableName, scannerInfo.TableName);
                Assert.IsNotNull(scannerInfo.ScannerId);
                Assert.IsFalse(scannerInfo.ScannerId.StartsWith("/"), "scanner id starts with a slash");
                Assert.IsNotNull(scannerInfo.ResponseHeaderCollection);
                // delete the scanner
                client.DeleteScannerAsync(testTableName, scannerInfo, scanOptions).Wait();
                // try to fetch data use the deleted scanner
                scanOptions.RetryPolicy = RetryPolicy.NoRetry;
                client.ScannerGetNextAsync(scannerInfo, scanOptions).Wait();
            }
            finally
            {
                if (scannerInfo != null)
                {
                    client.DeleteScannerAsync(testTableName, scannerInfo, scanOptions).Wait();
                }
            }
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public override void TestSubsetScan()
        {
            var client = CreateClient();
            const int startRow = 15;
            const int endRow = 15 + 13;
            StoreTestData(client);

            // subset range scan
            var scanSettings = new Scanner { batch = 10, startRow = BitConverter.GetBytes(startRow), endRow = BitConverter.GetBytes(endRow) };
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scannerInfo = null;
            try
            {
                scannerInfo = client.CreateScannerAsync(testTableName, scanSettings, scanOptions).Result;

                CellSet next;
                var expectedSet = new HashSet<int>(Enumerable.Range(startRow, endRow - startRow));
                while ((next = client.ScannerGetNextAsync(scannerInfo, scanOptions).Result) != null)
                {
                    foreach (CellSet.Row row in next.rows)
                    {
                        int k = BitConverter.ToInt32(row.key, 0);
                        expectedSet.Remove(k);
                    }
                }
                Assert.AreEqual(0, expectedSet.Count, "The expected set wasn't empty! Items left {0}!", string.Join(",", expectedSet));
            }
            finally
            {
                if (scannerInfo != null)
                {
                    client.DeleteScannerAsync(testTableName, scannerInfo, scanOptions).Wait();
                }
            }
        }
    }
}
