namespace Microsoft.HBase.Client.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using Microsoft.HBase.Client.Tests.Utilities;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    // ReSharper disable InconsistentNaming
    // 
    [TestClass]
    public class PublicInternalArchitecturalTests:TestBase
    {
        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void All_declarations_not_under_an_internal_or_resources_namespace_are_public_or_nested()
        {
            List<Assembly> assemblies = GetAssembliesUnderTest().ToList();
            assemblies.Count.ShouldBeGreaterThan(0);

            var violatingTypes = new HashSet<Type>();

            foreach (Assembly asm in assemblies)
            {
                List<Type> allAssemblyTypes = asm.GetTypes().ToList();
                foreach (Type type in allAssemblyTypes)
                {
                    string namespaceName = type.Namespace ?? string.Empty;
                    if (namespaceName.Length == 0)
                    {
                        // skip anonymous types.
                        continue;
                    }

                    if (namespaceName == "JetBrains.Profiler.Core.Instrumentation" && type.Name == "DataOnStack")
                    {
                        // appears when performing test coverage using dotCover.
                        continue;
                    }

                    if (!namespaceName.Contains(".Internal") && !namespaceName.Contains(".Resources") && !type.IsPublic && !type.IsNested)
                    {
                        violatingTypes.Add(type);
                    }
                }
            }

            violatingTypes.ShouldContainOnly(new Type[] { });
        }

        [TestMethod]
        [TestCategory(TestRunMode.CheckIn)]
        public void All_declarations_under_an_internal_namespace_are_not_public()
        {
            List<Assembly> assemblies = GetAssembliesUnderTest().ToList();
            assemblies.Count.ShouldBeGreaterThan(0);

            var violatingTypes = new HashSet<Type>();

            foreach (Assembly asm in assemblies)
            {
                List<Type> allAssemblyTypes = asm.GetTypes().ToList();
                foreach (Type type in allAssemblyTypes)
                {
                    string namespaceName = type.Namespace ?? string.Empty;
                    if (namespaceName.Contains(".Internal") && type.IsPublic)
                    {
                        violatingTypes.Add(type);
                    }
                }
            }

            violatingTypes.ShouldContainOnly(new Type[] { });
        }
    }
}
