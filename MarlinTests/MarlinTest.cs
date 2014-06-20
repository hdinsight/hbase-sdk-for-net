namespace MarlinTests
{
    using System;
    using Marlin;
    using NUnit.Framework;

    [TestFixture]
    public class MarlinTest
    {
        private ClusterCredentials _credentials;

        [SetUp]
        public void SetUp()
        {
            _credentials = ClusterCredentials.FromFile(@"..\..\credentials.txt");
        }

        [Test]
        public void TestGetVersion()
        {
            var marlin = new Marlin(_credentials);

            var version = marlin.GetVersion();
            Console.WriteLine(version);
        }
    }
}
