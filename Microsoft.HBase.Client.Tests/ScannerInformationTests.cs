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
    using Microsoft.HBase.Client.Tests.Utilities;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    // ReSharper disable InconsistentNaming

    [TestClass]
    public class When_I_create_ScannerInformation : ContextSpecification
    {
        private const string expectedScannerId = "/140614753560332aa73e8";
        private const string expectedTableName = "mytable";
        private readonly Uri expectedLocation = new Uri("https://headnodehost:8090/" + expectedTableName + "/scanner" + expectedScannerId);
        private ScannerInformation target;

        protected override void Context()
        {
            target = new ScannerInformation(expectedLocation, expectedTableName);
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void It_should_have_the_expected_location()
        {
            target.Location.ShouldEqual(expectedLocation);
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void It_should_have_the_expected_scanner_identifier()
        {
            target.ScannerId.ShouldEqual(expectedScannerId);
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void It_should_have_the_expected_table_name()
        {
            target.TableName.ShouldEqual(expectedTableName);
        }
    }

    [TestClass]
    public class When_I_call_a_ScannerInformation_ctor : ContextSpecification
    {
        private const string validTableName = "mytable";
        private readonly Uri validLocation = new Uri("https://headnodehost:8090/" + validTableName + "/scanner/140614753560332aa73e8");

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void It_should_reject_empty_table_names()
        {
            object instance = null;
            typeof(ArgumentEmptyException).ShouldBeThrownBy(() => instance = new ScannerInformation(validLocation, string.Empty));
            instance.ShouldBeNull();
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void It_should_reject_null_locations()
        {
            object instance = null;
            typeof(ArgumentNullException).ShouldBeThrownBy(() => instance = new ScannerInformation(null, validTableName));
            instance.ShouldBeNull();
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void It_should_reject_null_table_names()
        {
            object instance = null;
            typeof(ArgumentNullException).ShouldBeThrownBy(() => instance = new ScannerInformation(validLocation, null));
            instance.ShouldBeNull();
        }
    }
}
