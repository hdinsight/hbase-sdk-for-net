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
namespace Microsoft.HBase.Client.Internal
{
    using System.Collections.Generic;
    using System.Linq;

    internal class ByteArrayEqualityComparer : IEqualityComparer<byte[]>
    {
        /// <inheritdoc/>
        public bool Equals(byte[] x, byte[] y)
        {
            if (ReferenceEquals(x, null))
            {
                return ReferenceEquals(y, null);
            }

            if (ReferenceEquals(y, null))
            {
                return false;
            }

            return x.SequenceEqual(y);
        }

        /// <inheritdoc/>
        public int GetHashCode(byte[] obj)
        {
            if (obj == null || obj.Length == 0)
            {
                return 0;
            }

            return obj[0];
        }
    }
}
