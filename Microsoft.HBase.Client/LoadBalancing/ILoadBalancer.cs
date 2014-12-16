using System;

namespace Microsoft.HBase.Client.LoadBalancing
{
    public interface ILoadBalancer
    {
        Uri GetWorkerNodeEndPointBaseNext();
        void Reset();
        int GetWorkersCount();
    }
}
