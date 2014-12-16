using System;
using System.Threading;

namespace Microsoft.HBase.Client.LoadBalancing
{
    public class LoadBalancingHelper
    {
        public static void Execute(Action method, IRetryPolicy retryPolicy, IBackOffScheme backOffScheme, int numRetries = 5)
        {
            int retryCount = 0;
            bool requestSuccess = false;
            while ((!requestSuccess) && (retryCount < numRetries))
            {
                try
                {
                    method();
                    requestSuccess = true;
                }
                catch (Exception ex)
                {
                    Console.WriteLine("\tAttempt {0} failed with exception {1} - {2}", retryCount, ex.GetType(), ex.Message);

                    retryCount++;
                    if ((retryCount < numRetries) && (retryPolicy.ShouldRetryAttempt(ex)))
                    {
                        var sleepInterval = backOffScheme.GetRetryInterval(retryCount);
                        if (sleepInterval != default (TimeSpan))
                        {
                            Console.WriteLine("\tWill retry after {0} milliseconds......", sleepInterval.TotalMilliseconds);
                            Thread.Sleep(sleepInterval);
                        }
                    }
                    else
                    {
                        throw;
                    }
                }
            }
        }
    }
}
