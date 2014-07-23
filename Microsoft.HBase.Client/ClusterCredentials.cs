namespace Marlin
{
    using System;
    using System.Linq;
    using System.Collections.Generic;
    using System.IO;
    using System.Runtime.InteropServices;
    using System.Security;

    public class ClusterCredentials
    {
        public Uri ClusterUri { get; set; }

        public string UserName { get; set; }

        public SecureString ClusterPassword { get; set; }

        internal String ClusterPasswordAsString
        {
            get
            {
                IntPtr ptr = IntPtr.Zero;
                try
                {
                    ptr = Marshal.SecureStringToBSTR(ClusterPassword);
                    return Marshal.PtrToStringBSTR(ptr);
                }
                finally
                {
                    Marshal.FreeBSTR(ptr);
                }
            }
        }

        /// <summary>
        /// Reads the cluster credentials from a file found under the given path.
        /// This file needs to contain exactly three lines, where the first is the cluster uri, the second the username and the third is the password.
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
        /// <returns>a ClusterCredentials object with the cluster uri, user and the password</returns>
        public static ClusterCredentials FromFile(string path)
        {
            var lines = File.ReadAllLines(path).ToList();
            return FromFileInternal(lines);
        }

        /// <summary>
        /// Creates new cluster credentials.
        /// </summary>
        /// <param name="clusterUri">the cluster uri</param>
        /// <param name="username">the username of the cluster</param>
        /// <param name="password">the password</param>
        /// <returns>the cluster credentials</returns>
        public unsafe static ClusterCredentials Create(Uri clusterUri, string username, string password)
        {
            if (password == null || !password.Any())
            {
                throw new ArgumentException("Supplied password is null or empty!");
            }

            SecureString pw = null;
            unsafe
            {
                fixed (char* cPtr = password)
                {
                    pw = new SecureString(cPtr, password.Length);
                }
            }
            return Create(clusterUri, username, pw);
        }

        /// <summary>
        /// Creates new cluster credentials.
        /// </summary>
        /// <param name="clusterUri">the cluster uri</param>
        /// <param name="username">the username of the cluster</param>
        /// <param name="password">the secure string password</param>
        /// <returns>the cluster credentials</returns>
        public static ClusterCredentials Create(Uri clusterUri, string username, SecureString password)
        {
            var userName = username;
            if (userName == null || !userName.Any())
            {
                throw new ArgumentException("Supplied username is null or empty!");
            }

            if (password == null)
            {
                throw new ArgumentException("Supplied password is null!");
            }

            return new ClusterCredentials() { ClusterPassword = password, ClusterUri = clusterUri, UserName = userName };
        }

        internal static ClusterCredentials FromFileInternal(List<string> lines)
        {
            if (lines.Count() != 3)
            {
                throw new ArgumentException(
                    string.Format("Expected the credentials file to have three lines, " +
                        "first containing the cluster url, second the username, third the password. " +
                        "Given {0} lines!", lines.Count()));
            }
            return Create(new Uri(lines[0]), lines[1], lines[2]);
        }
    }
}
