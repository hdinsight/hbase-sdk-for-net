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

namespace Microsoft.HBase.Client.Tests.Utilities
{
    using System.Collections.Immutable;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Reflection;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public abstract class TestBase
    {
        /// <summary>
        /// Steps that are run after each test.
        /// </summary>
        [TestCleanup]
        public virtual void TestCleanup()
        {
        }

        /// <summary>
        /// Steps that are run before each test.
        /// </summary>
        [TestInitialize]
        public virtual void TestInitialize()
        {
            // reset the current directory to where the tests are running.

            // ReSharper disable AssignNullToNotNullAttribute
            Directory.SetCurrentDirectory(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location));
            // ReSharper restore AssignNullToNotNullAttribute
        }

        /// <summary>
        /// Gets or sets the test context which provides information about and functionality for the current test run.
        /// </summary>
        public TestContext TestContext { get; set; }

        /// <summary>
        /// Gets the collection of assemblies under test.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        protected ImmutableHashSet<Assembly> GetAssembliesUnderTest()
        {
            ImmutableHashSet<Assembly> rv = ImmutableHashSet<Assembly>.Empty;
            foreach (Assembly asm in TestAssemblyInitializeCleanup.AssembliesUnderTest)
            {
                rv = rv.Add(asm);
            }
            return rv;
        }
    }
}
