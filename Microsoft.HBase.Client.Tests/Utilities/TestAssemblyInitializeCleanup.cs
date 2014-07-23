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
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Reflection;
    using System.Threading;
    using Microsoft.HBase.Client;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Holds the MSTest [AssemblyInitialize] and [AssemblyCleanup] for this test assembly.
    /// </summary>
    [TestClass]
    public static class TestAssemblyInitializeCleanup
    {
        private static int _windowsAzureStorageEmulatorStarted;

        /// <summary>
        /// Frees resources obtained by the assembly after all tests in the assembly have run.
        /// </summary>
        [AssemblyCleanup]
        public static void AssemblyCleanup()
        {
            StopWindowsAzureStorageEmulator();
        }

        /// <summary>
        /// Method that contains code to be executed before any tests in the assembly have run and allocate resources obtained by the assembly. 
        /// </summary>
        /// <param name="context"></param>
        [AssemblyInitialize]
        public static void AssemblyInitialize(TestContext context)
        {
            // populate assemblies under test.
            var types = new List<Type>();
            types.Add(typeof(HBaseClient));
            var assemblies = new List<Assembly>();
            foreach (Type t in types)
            {
                assemblies.Add(t.Assembly);
            }

            AssembliesUnderTest = assemblies.ToArray();
        }

        internal static IEnumerable<Assembly> AssembliesUnderTest { get; private set; }


        internal static bool WindowsAzureStorageEmulatorStarted
        {
            get { return _windowsAzureStorageEmulatorStarted != 0; }
        }


        internal static void StartWindowsAzureStorageEmulatorIfNotAlreadyStarted()
        {
            if (Interlocked.Exchange(ref _windowsAzureStorageEmulatorStarted, 1) == 0)
            {
                StartWindowsAzureStorageEmulator();
            }
        }

        private static void StartWindowsAzureStorageEmulator()
        {
            Process process = Process.GetProcessesByName("DSServiceLDB").FirstOrDefault();
            if (process == null)
            {
                var processStartInfo = new ProcessStartInfo
                {
                    FileName = @"C:\Program Files\Microsoft SDKs\Windows Azure\Emulator\csrun.exe",
                    Arguments = "/devstore",
                };
                using (Process p = Process.Start(processStartInfo))
                {
                    p.WaitForExit();
                }
            }
        }

        private static void StopWindowsAzureStorageEmulator()
        {
            Process process = Process.GetProcessesByName("DSServiceLDB").FirstOrDefault();
            if (process != null)
            {
                process.Kill();
            }
        }
    }
}
