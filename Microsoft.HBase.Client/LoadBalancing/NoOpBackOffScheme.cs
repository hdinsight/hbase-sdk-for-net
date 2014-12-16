using System;

namespace Microsoft.HBase.Client
{
    public class NoOpBackOffScheme : IBackOffScheme
    {
        public TimeSpan GetRetryInterval(int retryCount)
        {
            return default(TimeSpan);
        }
    }
}
