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

package org.apache.druid.msq.util;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.common.guava.FutureUtils;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Future-related utility functions that haven't been moved to {@link FutureUtils}.
 */
public class MSQFutureUtils
{
  /**
   * Similar to {@link Futures#allAsList}, but provides a "cancelOnErrorOrInterrupt" option that cancels all input
   * futures if the returned future is canceled or fails.
   */
  public static <T> ListenableFuture<List<T>> allAsList(
      final Iterable<? extends ListenableFuture<? extends T>> futures,
      final boolean cancelOnErrorOrInterrupt
  )
  {
    final ListenableFuture<List<T>> retVal = Futures.allAsList(futures);

    if (cancelOnErrorOrInterrupt) {
      Futures.addCallback(
          retVal,
          new FutureCallback<List<T>>()
          {
            @Override
            public void onSuccess(@Nullable List<T> result)
            {
              // Do nothing.
            }

            @Override
            public void onFailure(Throwable t)
            {
              for (final ListenableFuture<? extends T> inputFuture : futures) {
                inputFuture.cancel(true);
              }
            }
          }
      );
    }

    return retVal;
  }
}
