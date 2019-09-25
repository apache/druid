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

import java.util.concurrent.atomic.AtomicInteger;

/**
*/
public abstract class GoHandler
{
  /******* Abstract Methods *********/
  protected abstract <X extends Exception> ListenableFuture<Response> go(Request request) throws X;

  /******* Non Abstract Methods ********/
  private volatile boolean succeeded = false;

  public boolean succeeded()
  {
    return succeeded;
  }

  public ListenableFuture<Response> run(Request request)
  {
    try {
      final ListenableFuture<Response> retVal = go(request);
      succeeded = true;
      return retVal;
    }
    catch (Throwable e) {
      succeeded = false;
      throw new RuntimeException(e);
    }
  }

  public GoHandler times(final int n)
  {
    final GoHandler myself = this;

    return new GoHandler()
    {
      AtomicInteger counter = new AtomicInteger(0);

      @Override
      public ListenableFuture<Response> go(final Request request)
      {
        if (counter.getAndIncrement() < n) {
          return myself.go(request);
        }
        succeeded = false;
        throw new ISE("Called more than %d times", n);
      }
    };
  }
}
