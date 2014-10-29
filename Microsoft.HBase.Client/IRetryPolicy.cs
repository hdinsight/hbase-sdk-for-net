namespace Microsoft.HBase.Client
{
   using System;
   using System.Diagnostics.CodeAnalysis;

   public interface IRetryPolicy
   {
      [SuppressMessage("Microsoft.Naming", "CA1704:IdentifiersShouldBeSpelledCorrectly", MessageId = "e")]
      bool ShouldRetryAttempt(Exception e);
   }
}
