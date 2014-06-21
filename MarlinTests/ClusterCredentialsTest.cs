using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Marlin;
using NUnit.Framework.Constraints;

namespace MarlinTests
{
    using NUnit.Framework;

    [TestFixture]
    public class ClusterCredentialsTest
    {
        [Test, ExpectedException]
        public void TestTooFewLines()
        {
            var failList = new List<string>() { "single", };
            ClusterCredentials.FromFileInternal(failList);
        }

        [Test, ExpectedException]
        public void TestTooManyLines()
        {
            var failList = new List<string>() { "single", "tuple", "triple", "quadro" };
            ClusterCredentials.FromFileInternal(failList);
        }

        [Test, ExpectedException]
        public void TestFirstEmptyLine()
        {
            var failList = new List<string>() { "", "user", "_password", };
            ClusterCredentials.FromFileInternal(failList);
        }

        [Test, ExpectedException]
        public void TestSecondEmptyLine()
        {
            var failList = new List<string>() { "https://someurl.de/", "", };
            ClusterCredentials.FromFileInternal(failList);
        }

        [Test, ExpectedException]
        public void TestThirdEmptyLine()
        {
            var failList = new List<string>() { "https://someurl.de/", "user", "", };
            ClusterCredentials.FromFileInternal(failList);
        }

        [Test, ExpectedException]
        public void TestAllEmptyLines()
        {
            var failList = new List<string>() { "", "", "" };
            ClusterCredentials.FromFileInternal(failList);
        }

        [Test]
        public void TestNormalCase()
        {
            var failList = new List<string>() { "https://someurl.de/", "uzern4m3", "s_mePassW0rd", };
            ClusterCredentials creds = ClusterCredentials.FromFileInternal(failList);
            Assert.AreEqual(new Uri(failList[0]), creds.ClusterUri);
            Assert.AreEqual(failList[1], creds.UserName);
            Assert.AreEqual(failList[2], creds.ClusterPasswordAsString);
        }
    }
}
