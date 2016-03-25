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

namespace Microsoft.HBase.Client.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using Microsoft.HBase.Client.Filters;
    using Microsoft.HBase.Client.Tests.Utilities;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using org.apache.hadoop.hbase.rest.protobuf.generated;
    using Microsoft.HBase.Client.LoadBalancing;

    // ReSharper disable InconsistentNaming

    [TestClass]
    public class FilterTests : DisposableContextSpecification
    {
        private const string TableNamePrefix = "filtertest";

        private const string ColumnFamilyName1 = "first";
        private const string ColumnFamilyName2 = "second";
        private const string LineNumberColumnName = "line";
        private const string ColumnNameA = "a";
        private const string ColumnNameB = "b";

        private static bool _arrangementCompleted;
        private static readonly List<FilterTestRecord> _allExpectedRecords = new List<FilterTestRecord>();
        private static ClusterCredentials _credentials;
        private static readonly Encoding _encoding = Encoding.UTF8;
        private static string _tableName;
        private static TableSchema _tableSchema;

        protected override void Context()
        {
            if (!_arrangementCompleted)
            {
                // at present, no tables are modified so only arrange the tables once per test pass
                // and putting the arrangement into a static context.
                // (this knocked test runs down to ~30 seconds from ~5 minutes).

                _credentials = ClusterCredentialsFactory.CreateFromFile(@".\credentials.txt");
                var client = new HBaseClient(_credentials);

                // ensure tables from previous tests are cleaned up
                TableList tables = client.ListTablesAsync().Result;
                foreach (string name in tables.name)
                {
                    string pinnedName = name;
                    if (name.StartsWith(TableNamePrefix, StringComparison.Ordinal))
                    {
                        client.DeleteTableAsync(pinnedName).Wait();
                    }
                }

                AddTable();
                PopulateTable();

                _arrangementCompleted = true;
            }
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_all_I_get_the_expected_results()
        {
            var client = new HBaseClient(_credentials);
            var scan = new Scanner();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scan, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();
                actualRecords.ShouldContainOnly(_allExpectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }

        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_ColumnCountGetFilter_I_get_the_expected_results()
        {
            // B column should not be returned, so set the value to null.
            List<FilterTestRecord> expectedRecords = (from r in _allExpectedRecords select r.WithBValue(null)).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();
            var filter = new ColumnCountGetFilter(2);
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_ColumnPaginationFilter_I_get_the_expected_results()
        {
            // only grabbing the LineNumber column with (1, 1)
            List<FilterTestRecord> expectedRecords = (from r in _allExpectedRecords select r.WithAValue(null).WithBValue(null)).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();
            var filter = new ColumnPaginationFilter(1, 1);
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_ColumnPrefixFilter_I_get_the_expected_results()
        {
            List<FilterTestRecord> expectedRecords = (from r in _allExpectedRecords select r.WithAValue(null).WithBValue(null)).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();
            var filter = new ColumnPrefixFilter(Encoding.UTF8.GetBytes(LineNumberColumnName));
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_ColumnRangeFilter_I_get_the_expected_results()
        {
            List<FilterTestRecord> expectedRecords = (from r in _allExpectedRecords select r.WithLineNumberValue(0).WithBValue(null)).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();
            var filter = new ColumnRangeFilter(Encoding.UTF8.GetBytes(ColumnNameA), true, Encoding.UTF8.GetBytes(ColumnNameB), false);
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }

        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_DependentColumnFilter_and_a_BinaryComparator_with_the_operator_equal_I_get_the_expected_results()
        {
            List<FilterTestRecord> expectedRecords = (from r in _allExpectedRecords where r.LineNumber == 1 select r).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();
            var filter = new DependentColumnFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                false,
                CompareFilter.CompareOp.Equal,
                new BinaryComparator(BitConverter.GetBytes(1)));
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_FamilyFilter_I_get_the_expected_results()
        {
            // B is in column family 2
            List<FilterTestRecord> expectedRecords = (from r in _allExpectedRecords select r.WithBValue(null)).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();
            var filter = new FamilyFilter(CompareFilter.CompareOp.Equal, new BinaryComparator(Encoding.UTF8.GetBytes(ColumnFamilyName1)));
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }

        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_FilterList_with_AND_logic_I_get_the_expected_results()
        {
            List<FilterTestRecord> expectedRecords = (from r in _allExpectedRecords where r.LineNumber == 1 select r).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();

            Filter f0 = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.Equal,
                BitConverter.GetBytes(1));

            Filter f1 = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.LessThanOrEqualTo,
                BitConverter.GetBytes(2));

            var filter = new FilterList(FilterList.Operator.MustPassAll, f0, f1);
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }

        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_FilterList_with_OR_logic_I_get_the_expected_results()
        {
            List<FilterTestRecord> expectedRecords = (from r in _allExpectedRecords where r.LineNumber <= 2 select r).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();

            Filter f0 = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.Equal,
                BitConverter.GetBytes(1));

            Filter f1 = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.LessThanOrEqualTo,
                BitConverter.GetBytes(2));

            var filter = new FilterList(FilterList.Operator.MustPassOne, f0, f1);
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }

        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_FirstKeyOnlyFilter_I_get_the_expected_results()
        {
            // a first key only filter does not return column values
            List<FilterTestRecord> expectedRecords =
                (from r in _allExpectedRecords select new FilterTestRecord(r.RowKey, 0, string.Empty, string.Empty)).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();
            var filter = new KeyOnlyFilter();
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }

        }


        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_InclusiveStopFilter_I_get_the_expected_results()
        {
            FilterTestRecord example = (from r in _allExpectedRecords where r.LineNumber == 2 select r).Single();
            byte[] rawRowKey = Encoding.UTF8.GetBytes(example.RowKey);

            List<FilterTestRecord> expectedRecords = (from r in _allExpectedRecords where r.LineNumber <= 2 select r).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();
            var filter = new InclusiveStopFilter(rawRowKey);
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }

        }


        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_KeyOnlyFilter_I_get_the_expected_results()
        {
            // a key only filter does not return column values
            List<FilterTestRecord> expectedRecords =
                (from r in _allExpectedRecords select new FilterTestRecord(r.RowKey, 0, string.Empty, string.Empty)).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();
            var filter = new KeyOnlyFilter();
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }

        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_MultipleColumnPrefixFilter_I_get_the_expected_results()
        {
            List<FilterTestRecord> expectedRecords = (from r in _allExpectedRecords select r.WithLineNumberValue(0)).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();

            // set this large enough so that we get all records back
            var prefixes = new List<byte[]> { Encoding.UTF8.GetBytes(ColumnNameA), Encoding.UTF8.GetBytes(ColumnNameB) };
            var filter = new MultipleColumnPrefixFilter(prefixes);
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_PageFilter_I_get_the_expected_results()
        {
            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();
            var filter = new PageFilter(2);
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.Count.ShouldBeGreaterThanOrEqualTo(2);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }

        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_PrefixFilter_I_get_the_expected_results()
        {
            FilterTestRecord example = _allExpectedRecords.First();
            byte[] rawRowkey = Encoding.UTF8.GetBytes(example.RowKey);

            const int prefixLength = 4;
            var prefix = new byte[prefixLength];
            Array.Copy(rawRowkey, prefix, prefixLength);

            List<FilterTestRecord> expectedRecords = (from r in _allExpectedRecords
                let rawKey = Encoding.UTF8.GetBytes(r.RowKey)
                where rawKey[0] == prefix[0] && rawKey[1] == prefix[1] && rawKey[2] == prefix[2] && rawKey[3] == prefix[3]
                select r).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();
            var filter = new PrefixFilter(prefix);
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }

        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_QualifierFilter_I_get_the_expected_results()
        {
            List<FilterTestRecord> expectedRecords = (from r in _allExpectedRecords select r.WithAValue(null).WithBValue(null)).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();
            var filter = new QualifierFilter(CompareFilter.CompareOp.Equal, new BinaryComparator(Encoding.UTF8.GetBytes(LineNumberColumnName)));
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }

        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_RandomRowFilter_I_get_the_expected_results()
        {
            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();

            // set this large enough so that we get all records back
            var filter = new RandomRowFilter(2000.0F);
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(_allExpectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }

        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_RowFilter_I_get_the_expected_results()
        {
            FilterTestRecord example = _allExpectedRecords.First();

            List<FilterTestRecord> expectedRecords = (from r in _allExpectedRecords where r.RowKey == example.RowKey select r).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();
            var filter = new RowFilter(CompareFilter.CompareOp.Equal, new BinaryComparator(Encoding.UTF8.GetBytes(example.RowKey)));
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }

        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_SingleColumnValueExcludeFilter_and_a_BinaryComparator_with_the_operator_equal_I_get_the_expected_results()
        {
            string bValue = (from r in _allExpectedRecords select r.B).First();

            // B column should not be returned, so set the value to null.
            List<FilterTestRecord> expectedRecords = (from r in _allExpectedRecords where r.B == bValue select r.WithBValue(null)).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();

            var filter = new SingleColumnValueExcludeFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName2),
                Encoding.UTF8.GetBytes(ColumnNameB),
                CompareFilter.CompareOp.Equal,
                Encoding.UTF8.GetBytes(bValue));
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }

        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_SingleColumnValueFilter_and_a_BinaryComparator_with_the_operator_equal_I_get_the_expected_results()
        {
            List<FilterTestRecord> expectedRecords = (from r in _allExpectedRecords where r.LineNumber == 1 select r).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();

            var filter = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.Equal,
                BitConverter.GetBytes(1),
                filterIfMissing: true);

            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_SingleColumnValueFilter_and_a_BinaryComparator_with_the_operator_greater_than_I_get_the_expected_results()
        {
            List<FilterTestRecord> expectedRecords = (from r in _allExpectedRecords where r.LineNumber > 1 select r).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();

            var filter = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.GreaterThan,
                BitConverter.GetBytes(1));
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }

        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void
            When_I_Scan_with_a_SingleColumnValueFilter_and_a_BinaryComparator_with_the_operator_greater_than_or_equal_I_get_the_expected_results()
        {
            List<FilterTestRecord> expectedRecords = (from r in _allExpectedRecords where r.LineNumber >= 1 select r).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();

            var filter = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.GreaterThanOrEqualTo,
                BitConverter.GetBytes(1));
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_SingleColumnValueFilter_and_a_BinaryComparator_with_the_operator_less_than_I_get_the_expected_results()
        {
            List<FilterTestRecord> expectedRecords = (from r in _allExpectedRecords where r.LineNumber < 1 select r).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();

            var filter = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.LessThan,
                BitConverter.GetBytes(1));
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_SingleColumnValueFilter_and_a_BinaryComparator_with_the_operator_less_than_or_equal_I_get_the_expected_results(
            )
        {
            List<FilterTestRecord> expectedRecords = (from r in _allExpectedRecords where r.LineNumber <= 1 select r).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();

            var filter = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.LessThanOrEqualTo,
                BitConverter.GetBytes(1));
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }

        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_SingleColumnValueFilter_and_a_BinaryComparator_with_the_operator_no_op_I_get_the_expected_results()
        {
            var expectedRecords = new List<FilterTestRecord>();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();

            var filter = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.NoOperation,
                BitConverter.GetBytes(1));
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }
        }


        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_SingleColumnValueFilter_and_a_BinaryComparator_with_the_operator_not_equal_I_get_the_expected_results()
        {
            List<FilterTestRecord> expectedRecords = (from r in _allExpectedRecords where r.LineNumber != 1 select r).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();
            var filter = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.NotEqual,
                BitConverter.GetBytes(1));
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }

        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_SingleColumnValueFilter_and_a_BinaryPrefixComparator_with_the_operator_equal_I_get_the_expected_results()
        {
            List<FilterTestRecord> expectedRecords = (from r in _allExpectedRecords where r.LineNumber == 3 select r).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();

            var comparer = new BinaryPrefixComparator(BitConverter.GetBytes(3));

            var filter = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.Equal,
                comparer,
                filterIfMissing: false);
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void
            When_I_Scan_with_a_SingleColumnValueFilter_and_a_BitComparator_with_the_operator_equal_and_the_bitop_XOR_I_get_the_expected_results()
        {
            List<FilterTestRecord> expectedRecords = (from r in _allExpectedRecords where r.LineNumber != 3 select r).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();

            var comparer = new BitComparator(BitConverter.GetBytes(3), BitComparator.BitwiseOp.Xor);

            var filter = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.Equal,
                comparer);
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }

        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_SingleColumnValueFilter_and_a_NullComparator_with_the_operator_not_equal_I_get_the_expected_results()
        {
            var expectedRecords = new List<FilterTestRecord>();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();

            var comparer = new NullComparator();

            var filter = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(LineNumberColumnName),
                CompareFilter.CompareOp.Equal,
                comparer);
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_SingleColumnValueFilter_and_a_SubstringComparator_with_the_operator_equal_I_get_the_expected_results()
        {
            // grab a substring that is guaranteed to match at least one record.
            string ss = _allExpectedRecords.First().A.Substring(1, 2);
            //Debug.WriteLine("The substring value is: " + ss);

            List<FilterTestRecord> expectedRecords = (from r in _allExpectedRecords where r.A.Contains(ss) select r).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();

            var comparer = new SubstringComparator(ss);

            var filter = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes(ColumnFamilyName1),
                Encoding.UTF8.GetBytes(ColumnNameA),
                CompareFilter.CompareOp.Equal,
                comparer);
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_SkipFilter_I_get_the_expected_results()
        {
            List<FilterTestRecord> expectedRecords = (from r in _allExpectedRecords where r.LineNumber != 0 select r).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();
            var filter = new SkipFilter(new ValueFilter(CompareFilter.CompareOp.NotEqual, new BinaryComparator(BitConverter.GetBytes(0))));
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_TimestampsFilter_I_get_the_expected_results()
        {
            List<FilterTestRecord> expectedRecords = _allExpectedRecords;

            // scan all and retrieve timestamps
            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanAll = null;
            List<long> timestamps = null;
            try
            {
                scanAll = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                timestamps = RetrieveTimestamps(scanAll, scanOptions).ToList();
            }
            finally
            {
                if (scanAll != null)
                {
                    client.DeleteScannerAsync(_tableName, scanAll, scanOptions).Wait();
                }
            }

            Assert.IsNotNull(timestamps);

            // timestamps scan
            scanner = new Scanner();
            var filter = new TimestampsFilter(timestamps);
            scanner.filter = filter.ToEncodedString();
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_ValueFilter_I_get_the_expected_results()
        {
            List<FilterTestRecord> expectedRecords =
                (from r in _allExpectedRecords where r.LineNumber == 3 select r.WithAValue(null).WithBValue(null)).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();
            var filter = new ValueFilter(CompareFilter.CompareOp.Equal, new BinaryComparator(BitConverter.GetBytes(3)));
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_ValueFilter_and_a_RegexStringComparator_I_get_the_expected_results()
        {
            List<FilterTestRecord> expectedRecords = _allExpectedRecords;
            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();
            var filter = new ValueFilter(CompareFilter.CompareOp.Equal, new RegexStringComparator(".*"));
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void When_I_Scan_with_a_WhileMatchFilter_I_get_the_expected_results()
        {
            List<FilterTestRecord> expectedRecords = (from r in _allExpectedRecords where r.LineNumber == 0 select r.WithBValue(null)).ToList();

            var client = new HBaseClient(_credentials);
            var scanner = new Scanner();
            var filter = new WhileMatchFilter(new ValueFilter(CompareFilter.CompareOp.NotEqual, new BinaryComparator(BitConverter.GetBytes(0))));
            scanner.filter = filter.ToEncodedString();
            RequestOptions scanOptions = RequestOptions.GetDefaultOptions();
            scanOptions.AlternativeEndpoint = Constants.RestEndpointBaseZero;
            ScannerInformation scanInfo = null;
            try
            {
                scanInfo = client.CreateScannerAsync(_tableName, scanner, scanOptions).Result;
                List<FilterTestRecord> actualRecords = RetrieveResults(scanInfo, scanOptions).ToList();

                actualRecords.ShouldContainOnly(expectedRecords);
            }
            finally
            {
                if (scanInfo != null)
                {
                    client.DeleteScannerAsync(_tableName, scanInfo, scanOptions).Wait();
                }
            }
        }

        private IEnumerable<long> RetrieveTimestamps(ScannerInformation scanInfo, RequestOptions scanOptions)
        {
            var rv = new HashSet<long>();

            var client = new HBaseClient(_credentials);
            CellSet next;

            while ((next = client.ScannerGetNextAsync(scanInfo, scanOptions).Result) != null)
            {
                foreach (CellSet.Row row in next.rows)
                {
                    List<Cell> cells = row.values;
                    foreach (Cell c in cells)
                    {
                        rv.Add(c.timestamp);
                    }
                }
            }

            return rv;
        }

        private IEnumerable<FilterTestRecord> RetrieveResults(ScannerInformation scanInfo, RequestOptions scanOptions)
        {
            var rv = new List<FilterTestRecord>();

            var client = new HBaseClient(_credentials);
            CellSet next;

            while ((next = client.ScannerGetNextAsync(scanInfo, scanOptions).Result) != null)
            {
                foreach (CellSet.Row row in next.rows)
                {
                    string rowKey = _encoding.GetString(row.key);

                    List<Cell> cells = row.values;

                    string a = null;
                    string b = null;
                    int lineNumber = 0;
                    foreach (Cell c in cells)
                    {
                        string columnName = ExtractColumnName(c.column);
                        switch (columnName)
                        {
                            case LineNumberColumnName:
                                lineNumber = c.data.Length > 0 ? BitConverter.ToInt32(c.data, 0) : 0;
                                break;

                            case ColumnNameA:
                                a = _encoding.GetString(c.data);
                                break;

                            case ColumnNameB:
                                b = _encoding.GetString(c.data);
                                break;

                            default:
                                throw new InvalidOperationException("Don't know what to do with column: " + columnName);
                        }
                    }

                    var rec = new FilterTestRecord(rowKey, lineNumber, a, b);
                    rv.Add(rec);
                }
            }

            return rv;
        }

        private void PopulateTable()
        {
            var client = new HBaseClient(_credentials);
            var cellSet = new CellSet();

            string id = Guid.NewGuid().ToString("N");
            for (int lineNumber = 0; lineNumber < 10; ++lineNumber)
            {
                string rowKey = string.Format(CultureInfo.InvariantCulture, "{0}-{1}", id, lineNumber);

                // add to expected records
                var rec = new FilterTestRecord(rowKey, lineNumber, Guid.NewGuid().ToString("N"), Guid.NewGuid().ToString("D"));
                _allExpectedRecords.Add(rec);

                // add to row
                var row = new CellSet.Row { key = _encoding.GetBytes(rec.RowKey) };

                var lineColumnValue = new Cell
                {
                    column = BuildCellColumn(ColumnFamilyName1, LineNumberColumnName),
                    data = BitConverter.GetBytes(rec.LineNumber)
                };
                row.values.Add(lineColumnValue);

                var paragraphColumnValue = new Cell { column = BuildCellColumn(ColumnFamilyName1, ColumnNameA), data = _encoding.GetBytes(rec.A) };
                row.values.Add(paragraphColumnValue);

                var columnValueB = new Cell { column = BuildCellColumn(ColumnFamilyName2, ColumnNameB), data = Encoding.UTF8.GetBytes(rec.B) };
                row.values.Add(columnValueB);

                cellSet.rows.Add(row);
            }

            client.StoreCellsAsync(_tableName, cellSet).Wait();
        }

        private Byte[] BuildCellColumn(string columnFamilyName, string columnName)
        {
            return _encoding.GetBytes(string.Format(CultureInfo.InvariantCulture, "{0}:{1}", columnFamilyName, columnName));
        }

        private string ExtractColumnName(Byte[] cellColumn)
        {
            string qualifiedColumnName = _encoding.GetString(cellColumn);
            string[] parts = qualifiedColumnName.Split(new[] { ':' }, 2);
            return parts[1];
        }

        private void AddTable()
        {
            // add a table specific to this test
            var client = new HBaseClient(_credentials);
            _tableName = TableNamePrefix + Guid.NewGuid().ToString("N");
            _tableSchema = new TableSchema { name = _tableName };
            _tableSchema.columns.Add(new ColumnSchema { name = ColumnFamilyName1 });
            _tableSchema.columns.Add(new ColumnSchema { name = ColumnFamilyName2 });

            client.CreateTableAsync(_tableSchema).Wait();
        }
    }
}
