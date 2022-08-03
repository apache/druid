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
import org.apache.druid.frame.Frame;

import java.io.IOException;

/**
 * Interface for writing a sequence of frames. Supports nonblocking writes through the {@link #writabilityFuture()}
 * method.
 *
 * May be implemented using an in-memory queue, disk file, stream, etc.
 *
 * Channels implementing this interface are used by a single writer; they do not support concurrent writes.
 */
public interface WritableFrameChannel
{
  /**
   * Writes a frame with an attached partition number.
   *
   * May throw an exception if {@link #writabilityFuture()} is unresolved.
   */
  void write(FrameWithPartition frameWithPartition) throws IOException;

  /**
   * Writes a frame without an attached partition number.
   *
   * May throw an exception if {@link #writabilityFuture()} is unresolved.
   */
  default void write(Frame frame) throws IOException
  {
    write(new FrameWithPartition(frame, FrameWithPartition.NO_PARTITION));
  }

  /**
   * Finish writing to this channel, unsuccessfully. Must be followed by a call to {@link #doneWriting()}.
   */
  void fail() throws IOException;

  /**
   * Finish writing to this channel.
   *
   * After calling this method, no additional calls to {@link #write}, {@link #fail()}, or this method are permitted.
   */
  void doneWriting() throws IOException;

  /**
   * Returns a future that resolves when {@link #write} is able to receive a new frame without blocking or throwing
   * an exception. The future never resolves to an exception.
   */
  ListenableFuture<?> writabilityFuture();
}
