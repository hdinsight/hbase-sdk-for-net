using System;

namespace Microsoft.HBase.Client.LoadBalancing
{
    public class RetryOnAllExceptionsPolicy : IRetryPolicy
    {
        public bool ShouldRetryAttempt(Exception e)
        {
            return true;
        }
    }
}
