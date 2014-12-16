using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.HBase.Client
{
    public class FixedIntervalBackOffScheme : IBackOffScheme
    {
        private TimeSpan _backOffInterval;

        public FixedIntervalBackOffScheme(TimeSpan backOffInterval = default(TimeSpan))
        {
            if (backOffInterval == default(TimeSpan))
            {
                _backOffInterval = TimeSpan.FromSeconds(10);
            }
            else
            {
                _backOffInterval = backOffInterval;
            }
        }

        public TimeSpan GetRetryInterval(int retryCount)
        {
            return _backOffInterval;
        }
    }
}
