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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.file.FrameFileWriter;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Frame channel backed by a {@link FrameFileWriter}.
 */
public class WritableFrameFileChannel implements WritableFrameChannel
{
  private final FrameFileWriter writer;

  public WritableFrameFileChannel(final FrameFileWriter writer)
  {
    this.writer = writer;
  }

  @Override
  public void write(FrameWithPartition frame) throws IOException
  {
    writer.writeFrame(frame.frame(), frame.partition());
  }

  @Override
  public void fail(@Nullable Throwable cause) throws IOException
  {
    // Cause is ignored when writing to frame files. Readers can tell the file is truncated, but they won't know why.
    writer.abort();
  }

  @Override
  public void close() throws IOException
  {
    writer.close();
  }

  @Override
  public ListenableFuture<?> writabilityFuture()
  {
    return Futures.immediateFuture(true);
  }
}
