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

import java.io.Closeable;

/**
 * Interface for reading a sequence of frames. Supports nonblocking reads through the {@link #canRead()} and
 * {@link #readabilityFuture()} methods.
 *
 * May be implemented using an in-memory queue, disk file, stream, etc.
 *
 * Channels implementing this interface are used by a single reader; they do not support concurrent reads.
 */
public interface ReadableFrameChannel extends Closeable
{
  /**
   * Returns whether this channel is finished. Finished channels will not generate any further frames or errors.
   *
   * Generally, once you discover that a channel is finished, you should call {@link #close()} and then
   * discard it.
   *
   * Note that it is possible for a channel to be unfinished and also have no available frames or errors. This happens
   * when it is not in a ready-for-reading state. See {@link #readabilityFuture()} for details.
   */
  boolean isFinished();

  /**
   * Returns whether this channel has a frame or error condition currently available. If this method returns true, then
   * you can call {@link #read()} to retrieve the frame or error.
   *
   * Note that it is possible for a channel to be unfinished and also have no available frames or errors. This happens
   * when it is not in a ready-for-reading state. See {@link #readabilityFuture()} for details.
   */
  boolean canRead();

  /**
   * Returns the next available frame from this channel.
   *
   * Before calling this method, you should check {@link #canRead()} to ensure there is a frame or
   * error available.
   *
   * @throws java.util.NoSuchElementException if there is no frame currently available
   */
  Frame read();

  /**
   * Returns a future that will resolve when either {@link #isFinished()} or {@link #canRead()} would
   * return true. The future will never resolve to an exception. If something exceptional has happened, the exception
   * can be retrieved from {@link #read()}.
   */
  ListenableFuture<?> readabilityFuture();

  /**
   * Releases any resources associated with this readable channel. After calling this, you should not call any other
   * methods on the channel.
   */
  @Override
  void close();
}
