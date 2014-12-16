using System;

namespace Microsoft.HBase.Client
{
    public interface IBackOffScheme
    {
        TimeSpan GetRetryInterval(int retryCount);
    }
}
