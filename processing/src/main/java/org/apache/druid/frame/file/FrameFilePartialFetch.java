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

package org.apache.druid.frame.file;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;

/**
 * Response object for {@link FrameFileHttpResponseHandler}.
 *
 * The handler mutates this object on each chunk of a chunked response. When the response is done, this object
 * is returned to the caller.
 */
public class FrameFilePartialFetch
{
  private final boolean lastFetchHeaderSet;
  private long bytesRead;

  @Nullable
  private Throwable exceptionCaught;

  @Nullable
  private ListenableFuture<?> backpressureFuture;

  FrameFilePartialFetch(boolean lastFetchHeaderSet)
  {
    this.lastFetchHeaderSet = lastFetchHeaderSet;
  }

  public boolean isLastFetch()
  {
    return exceptionCaught == null && lastFetchHeaderSet && bytesRead == 0L;
  }

  /**
   * The exception that was encountered, if {@link #isExceptionCaught()} is true.
   *
   * @throws IllegalStateException if no exception was caught
   */
  public Throwable getExceptionCaught()
  {
    if (!isExceptionCaught()) {
      throw new ISE("No exception caught");
    }

    return exceptionCaught;
  }

  /**
   * Whether an exception was encountered during response processing.
   */
  public boolean isExceptionCaught()
  {
    return exceptionCaught != null;
  }

  /**
   * Future that resolves when it is a good time to request the next chunk of the frame file.
   *
   * Must only be called once, because the future is cleared once it is returned.
   */
  public ListenableFuture<?> backpressureFuture()
  {
    final ListenableFuture<?> future = backpressureFuture;
    backpressureFuture = null;

    if (future != null) {
      return future;
    } else {
      return Futures.immediateFuture(null);
    }
  }

  void setBackpressureFuture(final ListenableFuture<?> future)
  {
    if (backpressureFuture == null) {
      backpressureFuture = future;
    }
  }

  void exceptionCaught(final Throwable t)
  {
    if (exceptionCaught == null) {
      exceptionCaught = t;
    }
  }

  void addBytesRead(final long n)
  {
    bytesRead += n;
  }
}
