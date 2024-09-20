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

import org.apache.druid.frame.channel.PartitionedReadableFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;

import java.io.IOException;

/**
 * Implementation of {@link PartitionedReadableFrameChannel} that wraps all underlying channels in
 * {@link AlwaysAsyncReadableFrameChannel}.
 */
public class AlwaysAsyncPartitionedReadableFrameChannel implements PartitionedReadableFrameChannel
{
  private final PartitionedReadableFrameChannel delegate;

  public AlwaysAsyncPartitionedReadableFrameChannel(PartitionedReadableFrameChannel delegate)
  {
    this.delegate = delegate;
  }

  @Override
  public ReadableFrameChannel getReadableFrameChannel(int partitionNumber)
  {
    return new AlwaysAsyncReadableFrameChannel(delegate.getReadableFrameChannel(partitionNumber));
  }

  @Override
  public void close() throws IOException
  {
    delegate.close();
  }
}
