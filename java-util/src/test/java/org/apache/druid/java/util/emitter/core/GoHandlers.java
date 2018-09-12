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

package org.apache.druid.java.util.emitter.core;

import org.apache.druid.java.util.common.ISE;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 */
public class GoHandlers
{
  public static GoHandler failingHandler()
  {
    return new GoHandler()
    {
      @Override
      protected ListenableFuture<Response> go(Request request)
      {
        throw new ISE("Shouldn't be called");
      }
    };
  }

  public static GoHandler passingHandler(final Response retVal)
  {
    return new GoHandler()
    {
      @Override
      protected ListenableFuture<Response> go(Request request)
      {
        return immediateFuture(retVal);
      }
    };
  }

  static <T> ListenableFuture<T> immediateFuture(T val)
  {
    CompletableFuture<T> future = CompletableFuture.completedFuture(val);
    return new ListenableFuture<T>()
    {
      @Override
      public void done()
      {
      }

      @Override
      public void abort(Throwable t)
      {
      }

      @Override
      public void touch()
      {
      }

      @Override
      public ListenableFuture<T> addListener(Runnable listener, Executor exec)
      {
        future.thenAcceptAsync(r -> listener.run(), exec);
        return this;
      }

      @Override
      public CompletableFuture<T> toCompletableFuture()
      {
        return future;
      }

      @Override
      public boolean cancel(boolean mayInterruptIfRunning)
      {
        return false;
      }

      @Override
      public boolean isCancelled()
      {
        return false;
      }

      @Override
      public boolean isDone()
      {
        return true;
      }

      @Override
      public T get() throws InterruptedException, ExecutionException
      {
        return future.get();
      }

      @Override
      public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
      {
        return future.get(timeout, unit);
      }
    };
  }
}
