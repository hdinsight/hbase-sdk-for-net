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
    using System.IO;
    using System.Linq;
    using Microsoft.HBase.Client.Internal;

    internal static class ClusterCredentialsFactory
    {
        /// <summary>
        /// Reads the cluster credentials from a file found under the given path.
        /// This file needs to contain exactly three lines, where the first is the cluster URI, the second the username and the third is the password.
        /// (Bad luck if your username/password contains either \r or \n!).
        /// 
        /// A possible example is:
        /// 
        /// https://csharpazurehbase.azurehdinsight.net/
        /// admin
        /// _mySup3rS4f3P4ssW0rd.
        /// 
        /// </summary>
        /// <param name="path">a file system path that contains a text file with the credentials</param>
        /// <returns>a ClusterCredentials object with the cluster URI, user and the password</returns>
        internal static ClusterCredentials CreateFromFile(string path)
        {
            path.ArgumentNotNull("path");

            List<string> lines = File.ReadAllLines(path).ToList();
            return CreateFromList(lines);
        }

        internal static ClusterCredentials CreateFromList(List<string> lines)
        {
            lines.ArgumentNotNull("lines");

            if (lines.Count() != 3)
            {
                throw new ArgumentException(
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "Expected the credentials file to have exactly three lines, " +
                        "first containing the cluster URL, second the username, third the password. " + "Given {0} lines!",
                        lines.Count()),
                    "lines");
            }

            var rv = new ClusterCredentials(new Uri(lines[0]), lines[1], lines[2]);
            return rv;
        }
    }
}
