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
    using System.Security;
    using Microsoft.HBase.Client.Tests.Utilities;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    // ReSharper disable InconsistentNaming

    [TestClass]
    public class When_I_create_ClusterCredentials : DisposableContextSpecification
    {
        private const string expectedUserName = "uzern4m3";
        private const string expectedPlainPassword = "s_mePassW0rd";
        private readonly List<IDisposable> disposables = new List<IDisposable>();
        private readonly Uri expectedUri = new Uri("https://someurl.de/");
        private ClusterCredentials fromSecureString;
        private ClusterCredentials fromString;
        private readonly SecureString originalSecurePassword = expectedPlainPassword.ToSecureString();

        protected override void Context()
        {
            fromString = new ClusterCredentials(expectedUri, expectedUserName, expectedPlainPassword);
            fromSecureString = new ClusterCredentials(expectedUri, expectedUserName, originalSecurePassword);
        }

        [TestMethod]
        public void It_should_have_the_expected_password()
        {
            SecureString pwd = fromString.ClusterPassword;
            disposables.Add(pwd);
            pwd.ToPlainString().ShouldEqual(expectedPlainPassword);

            pwd = fromSecureString.ClusterPassword;
            disposables.Add(pwd);
            pwd.ToPlainString().ShouldEqual(expectedPlainPassword);

            // it should maintain encapsulation of the password
            pwd.ShouldNotBeTheSameAs(originalSecurePassword);
        }

        [TestMethod]
        public void It_should_have_the_expected_uri()
        {
            fromString.ClusterUri.ShouldEqual(expectedUri);
            fromSecureString.ClusterUri.ShouldEqual(expectedUri);
        }

        [TestMethod]
        public void It_should_have_the_expected_user_name()
        {
            fromString.UserName.ShouldEqual(expectedUserName);
            fromSecureString.UserName.ShouldEqual(expectedUserName);
        }
    }

    [TestClass]
    public class When_I_dispose_of_a_ClusterCredentials : DisposableContextSpecification
    {
        private ClusterCredentials target;

        protected override void Context()
        {
            target = new ClusterCredentials(new Uri("https://someurl.de/"), "uzern4m3", "s_mePassW0rd");
        }

        protected override void BecauseOf()
        {
            target.Dispose();
        }

        [TestMethod]
        public void It_should_throw_when_I_get_the_password()
        {
            typeof(ObjectDisposedException).ShouldBeThrownBy(() => DisposableHelp.SafeCreate(() => target.ClusterPassword));
        }
    }

    [TestClass]
    public class When_I_call_a_ClusterCredentials_ctor : DisposableContextSpecification
    {
        private const String validUserName = "uzern4m3";
        private const String validPassword = "s_mePassW0rd";
        private readonly SecureString securePassword = validPassword.ToSecureString();
        private readonly Uri validUri = new Uri("https://someurl.de/");

        [TestMethod]
        public void It_should_reject_empty_user_names()
        {
            typeof(ArgumentEmptyException).ShouldBeThrownBy(
                () => DisposableHelp.SafeCreate(() => new ClusterCredentials(validUri, string.Empty, validPassword)));
            typeof(ArgumentEmptyException).ShouldBeThrownBy(
                () => DisposableHelp.SafeCreate(() => new ClusterCredentials(validUri, string.Empty, securePassword)));
        }

        [TestMethod]
        public void It_should_reject_null_passwords()
        {
            typeof(ArgumentNullException).ShouldBeThrownBy(
                () => DisposableHelp.SafeCreate(() => new ClusterCredentials(validUri, validUserName, (string)null)));
            typeof(ArgumentNullException).ShouldBeThrownBy(
                () => DisposableHelp.SafeCreate(() => new ClusterCredentials(validUri, validUserName, (SecureString)null)));
        }

        [TestMethod]
        public void It_should_reject_null_uris()
        {
            typeof(ArgumentNullException).ShouldBeThrownBy(
                () => DisposableHelp.SafeCreate(() => new ClusterCredentials(null, validUserName, validPassword)));
            typeof(ArgumentNullException).ShouldBeThrownBy(
                () => DisposableHelp.SafeCreate(() => new ClusterCredentials(null, validUserName, securePassword)));
        }

        [TestMethod]
        public void It_should_reject_null_user_names()
        {
            typeof(ArgumentNullException).ShouldBeThrownBy(
                () => DisposableHelp.SafeCreate(() => new ClusterCredentials(validUri, null, validPassword)));
            typeof(ArgumentNullException).ShouldBeThrownBy(
                () => DisposableHelp.SafeCreate(() => new ClusterCredentials(validUri, null, securePassword)));
        }
    }
}
