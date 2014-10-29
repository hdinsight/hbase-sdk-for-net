namespace Microsoft.HBase.Client
{
   public interface IRetryPolicyFactory
   {
      IRetryPolicy Create();
   }
}