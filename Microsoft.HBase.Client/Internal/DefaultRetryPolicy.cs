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
   using System;
   using System.Collections.Generic;
   using System.Reflection;
   using System.Threading;
   using System.Threading.Tasks;

   internal class DefaultRetryPolicy : IRetryPolicy
   {
      private const int MinimumAttempts = 3;
      private const Double MinimumPercentageBase1 = 0.25;
      private readonly TimeSpan _delayDuration = TimeSpan.FromSeconds(5);
      private readonly TimeSpan _maximumDuration = TimeSpan.FromMinutes(10);
      private readonly TimeSpan _minimumDuration = TimeSpan.FromMinutes(2);
      private readonly TimeSpan _noDelayDuration = TimeSpan.FromMinutes(2);
      private int _attemptCount;
      private bool _initialized;
      private int _nodeCount = 1;
      private DateTimeOffset _started;

      /// <inheritdoc/>
      public bool ShouldRetryAttempt(Exception e)
      {
         if (!_initialized)
         {
            Init();
         }

         _attemptCount++;

         DateTimeOffset now = DateTimeOffset.UtcNow;
         TimeSpan elapsed = now - _started;

         if (elapsed > _maximumDuration)
         {
            return false;
         }

         if (elapsed > _minimumDuration)
         {
            if ((((double)_attemptCount) / _nodeCount) > MinimumPercentageBase1)
            {
               if (_attemptCount >= MinimumAttempts)
               {
                  return false;
               }
            }
         }

         if (elapsed > _noDelayDuration)
         {
            Thread.Sleep(_delayDuration);
         }

         return !IsFatalException(e);
      }

      private Exception GetFirstException(Exception e)
      {
         var asAgg = e as AggregateException;
         if (asAgg.IsNotNull())
         {
            AggregateException exs = asAgg.Flatten();
            if (exs.InnerException.IsNotNull())
            {
               return GetFirstException(exs.InnerException);
            }
         }

         var asTargetOfInvoke = e as TargetInvocationException;
         if (asTargetOfInvoke.IsNotNull())
         {
            return GetFirstException(asTargetOfInvoke.InnerException);
         }

         var asTaskCancel = e as TaskCanceledException;
         if (asTaskCancel.IsNotNull())
         {
            if (asTaskCancel.InnerException.IsNotNull())
            {
               return GetFirstException(asTaskCancel.InnerException);
            }
         }
         return e;
      }

      private void Init()
      {
         _started = DateTimeOffset.UtcNow;
         // _nodeCount = UnderDevelopmentApi.GetNodeCount();
         _initialized = true;
      }

      private bool IsFatalException(Exception e)
      {
         var fatalTypes = new List<Type> { typeof(ArgumentException) };

         bool rv = false;
         if (e.IsNotNull())
         {
            Exception finalException = GetFirstException(e);
            Type exceptionType = finalException.GetType();
            foreach (Type t in fatalTypes)
            {
               if (t.IsAssignableFrom(exceptionType))
               {
                  rv = true;
                  break;
               }
            }
         }

         return rv;
      }
   }
}
