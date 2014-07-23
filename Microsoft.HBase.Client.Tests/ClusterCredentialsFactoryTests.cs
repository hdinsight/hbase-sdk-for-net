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
    using System.IO;
    using Microsoft.HBase.Client.Tests.Utilities;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    // ReSharper disable InconsistentNaming

    [TestClass]
    public class When_I_call_ClusterCredentialsFactory_CreateFromList : DisposableContextSpecification
    {
        [TestMethod]
        public void It_should_throw_when_the_list_is_null()
        {
            var ane =
                (ArgumentNullException)
                typeof(ArgumentNullException).ShouldBeThrownBy(() => DisposableHelp.SafeCreate(() => ClusterCredentialsFactory.CreateFromList(null)));
            ane.ParamName.ShouldEqual("lines");
        }

        [TestMethod]
        public void It_should_throw_when_the_list_is_too_long()
        {
            var lst = new List<string> { Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString() };
            var ae =
                (ArgumentException)
                typeof(ArgumentException).ShouldBeThrownBy(() => DisposableHelp.SafeCreate(() => ClusterCredentialsFactory.CreateFromList(lst)));
            ae.ParamName.ShouldEqual("lines");
        }

        [TestMethod]
        public void It_should_throw_when_the_list_is_too_short()
        {
            var lst = new List<string>();

            // empty
            var ae =
                (ArgumentException)
                typeof(ArgumentException).ShouldBeThrownBy(() => DisposableHelp.SafeCreate(() => ClusterCredentialsFactory.CreateFromList(lst)));
            ae.ParamName.ShouldEqual("lines");

            // one
            lst.Add(Guid.NewGuid().ToString());
            ae =
                (ArgumentException)
                typeof(ArgumentException).ShouldBeThrownBy(() => DisposableHelp.SafeCreate(() => ClusterCredentialsFactory.CreateFromList(lst)));
            ae.ParamName.ShouldEqual("lines");

            // two
            lst.Add(Guid.NewGuid().ToString());
            ae =
                (ArgumentException)
                typeof(ArgumentException).ShouldBeThrownBy(() => DisposableHelp.SafeCreate(() => ClusterCredentialsFactory.CreateFromList(lst)));
            ae.ParamName.ShouldEqual("lines");
        }
    }


    [TestClass]
    public class When_I_call_ClusterCredentialsFactory_CreateFromFile
    {
        [TestMethod]
        public void It_should_throw_the_file_does_not_exist()
        {
            string path = Path.Combine(Directory.GetCurrentDirectory(), Guid.NewGuid().ToString());
            var fnfe =
                (FileNotFoundException)
                typeof(FileNotFoundException).ShouldBeThrownBy(() => DisposableHelp.SafeCreate(() => ClusterCredentialsFactory.CreateFromFile(path)));
            fnfe.FileName.ShouldEqual(path);
        }

        [TestMethod]
        public void It_should_throw_when_the_path_is_null()
        {
            var ane =
                (ArgumentNullException)
                typeof(ArgumentNullException).ShouldBeThrownBy(() => DisposableHelp.SafeCreate(() => ClusterCredentialsFactory.CreateFromFile(null)));
            ane.ParamName.ShouldEqual("path");
        }
    }
}
