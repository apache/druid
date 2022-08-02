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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Response object for {@link FrameFileHttpResponseHandler}.
 */
public class FrameFilePartialFetch
{
  private final AtomicLong bytesRead = new AtomicLong(0L);
  private final AtomicReference<Throwable> exceptionCaught = new AtomicReference<>();
  private final AtomicReference<ListenableFuture<?>> backpressureFuture = new AtomicReference<>();

  FrameFilePartialFetch()
  {
  }

  public boolean isEmptyFetch()
  {
    return exceptionCaught.get() == null && bytesRead.get() == 0L;
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

    return exceptionCaught.get();
  }

  /**
   * Whether an exception was encountered during response processing.
   */
  public boolean isExceptionCaught()
  {
    return exceptionCaught.get() != null;
  }

  /**
   * Future that resolves when it is a good time to request the next chunk of the frame file.
   */
  public ListenableFuture<?> backpressureFuture()
  {
    final ListenableFuture<?> future = backpressureFuture.getAndSet(null);
    if (future != null) {
      return future;
    } else {
      return Futures.immediateFuture(null);
    }
  }

  void setBackpressureFuture(final ListenableFuture<?> future)
  {
    backpressureFuture.compareAndSet(null, future);
  }

  void exceptionCaught(final Throwable t)
  {
    exceptionCaught.compareAndSet(null, t);
  }

  void addBytesRead(final long n)
  {
    bytesRead.addAndGet(n);
  }
}
