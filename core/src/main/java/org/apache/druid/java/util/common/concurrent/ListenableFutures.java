/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.java.util.common.concurrent;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.Nullable;
import java.util.function.Function;

public class ListenableFutures
{
  /**
   * Guava 19 changes the Futures.transform signature so that the async form is different. This is here as a
   * compatability layer until such a time as druid only supports Guava 19 or later, in which case
   * Futures.transformAsync should be used
   *
   * This is NOT copied from guava.
   */
  public static <I, O> ListenableFuture<O> transformAsync(
      final ListenableFuture<I> inFuture,
      final Function<I, ListenableFuture<O>> transform
  )
  {
    final SettableFuture<O> finalFuture = SettableFuture.create();
    Futures.addCallback(inFuture, new FutureCallback<I>()
    {
      @Override
      public void onSuccess(@Nullable I result)
      {
        final ListenableFuture<O> transformFuture = transform.apply(result);
        Futures.addCallback(transformFuture, new FutureCallback<O>()
        {
          @Override
          public void onSuccess(@Nullable O result)
          {
            finalFuture.set(result);
          }

          @Override
          public void onFailure(Throwable t)
          {
            finalFuture.setException(t);
          }
        });
      }

      @Override
      public void onFailure(Throwable t)
      {
        finalFuture.setException(t);
      }
    });
    return finalFuture;
  }
}
