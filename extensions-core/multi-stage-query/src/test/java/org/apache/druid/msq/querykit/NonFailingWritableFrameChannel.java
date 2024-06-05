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

package org.apache.druid.msq.querykit;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.channel.FrameWithPartition;
import org.apache.druid.frame.channel.WritableFrameChannel;

import javax.annotation.Nullable;
import java.io.IOException;

public class NonFailingWritableFrameChannel implements WritableFrameChannel
{
  final WritableFrameChannel delegate;

  public NonFailingWritableFrameChannel(WritableFrameChannel delegate)
  {
    this.delegate = delegate;
  }

  @Override
  public void write(FrameWithPartition frameWithPartition) throws IOException
  {
    delegate.write(frameWithPartition);
  }

  @Override
  public void fail(@Nullable Throwable cause)
  {
  }

  @Override
  public void close() throws IOException
  {
    delegate.close();
  }

  @Override
  public boolean isClosed()
  {
    return delegate.isClosed();
  }

  @Override
  public ListenableFuture<?> writabilityFuture()
  {
    return delegate.writabilityFuture();
  }
}
