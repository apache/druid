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

package org.apache.druid.frame.channel;

import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;
import java.io.IOException;

public class BoundedWritableFrameChannel implements WritableFrameChannel
{
  private final WritableFrameChannel delegate;
  private final ByteTracker byteTracker;

  public BoundedWritableFrameChannel(WritableFrameChannel delegate, ByteTracker byteTracker)
  {
    this.delegate = delegate;
    this.byteTracker = byteTracker;
  }

  @Override
  public void write(FrameWithPartition frameWithPartition) throws IOException
  {
    byteTracker.reserve(frameWithPartition.frame().numBytes());
    delegate.write(frameWithPartition);
  }

  @Override
  public void fail(@Nullable Throwable cause) throws IOException
  {
    delegate.fail(cause);
  }

  @Override
  public void close() throws IOException
  {
    delegate.close();
  }

  @Override
  public ListenableFuture<?> writabilityFuture()
  {
    return delegate.writabilityFuture();
  }
}
