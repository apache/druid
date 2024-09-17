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

package org.apache.druid.frame.processor.test;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.java.util.common.ISE;

/**
 * Wraps an underlying channel and forces an async style of reading. After each call to {@link #read()}, the
 * {@link #canRead()} and {@link #isFinished()} methods return false until {@link #readabilityFuture()} is called.
 */
public class AlwaysAsyncReadableFrameChannel implements ReadableFrameChannel
{
  private final ReadableFrameChannel delegate;
  private boolean defer;

  public AlwaysAsyncReadableFrameChannel(ReadableFrameChannel delegate)
  {
    this.delegate = delegate;
  }

  @Override
  public boolean isFinished()
  {
    if (defer) {
      return false;
    }

    return delegate.isFinished();
  }

  @Override
  public boolean canRead()
  {
    if (defer) {
      return false;
    }

    return delegate.canRead();
  }

  @Override
  public Frame read()
  {
    if (defer) {
      throw new ISE("Cannot call read() while deferred");
    }

    defer = true;
    return delegate.read();
  }

  @Override
  public ListenableFuture<?> readabilityFuture()
  {
    defer = false;
    return delegate.readabilityFuture();
  }

  @Override
  public void close()
  {
    defer = false;
    delegate.close();
  }
}
