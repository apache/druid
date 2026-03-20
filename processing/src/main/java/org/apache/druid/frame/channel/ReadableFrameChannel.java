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
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.Frame;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

import java.io.Closeable;
import java.util.NoSuchElementException;

/**
 * Interface for reading a sequence of batches of data. Supports nonblocking reads through the {@link #canRead()} and
 * {@link #readabilityFuture()} methods.
 *
 * May be implemented using an in-memory queue, disk file, stream, etc.
 *
 * Channels implementing this interface are used by a single reader; they do not support concurrent reads.
 *
 * Despite its name, instances of this class can typically return any {@link RowsAndColumns} through the
 * {@link #read()} method.
 */
public interface ReadableFrameChannel extends Closeable
{
  /**
   * Returns whether this channel is finished. Finished channels will not generate any further data batches or errors.
   *
   * Generally, once you discover that a channel is finished, you should call {@link #close()} and then
   * discard it.
   *
   * Note that it is possible for a channel to be unfinished and also have no available data batches or errors.
   * This happens when it is not in a ready-for-reading state. See {@link #readabilityFuture()} for details.
   */
  boolean isFinished();

  /**
   * Returns whether this channel has a batch of data or error condition currently available. If this method returns
   * true, then you can call {@link #readFrame()} or {@link #read()} to retrieve the batch or error.
   *
   * Note that it is possible for a channel to be unfinished and also have no available batches or errors. This happens
   * when it is not in a ready-for-reading state. See {@link #readabilityFuture()} for details.
   */
  boolean canRead();

  /**
   * Returns the next available batch of data from this channel as a {@link RowsAndColumns}.
   *
   * Before calling this method, you should check {@link #canRead()} to ensure there is a batch of data or
   * error available.
   *
   * @throws NoSuchElementException if there is no batch currently available
   */
  RowsAndColumns read();

  /**
   * Returns the next available batch of data from this channel as a {@link Frame}.
   *
   * Before calling this method, you should check {@link #canRead()} to ensure there is a batch of data or
   * error available.
   *
   * @throws NoSuchElementException if there is no batch currently available
   * @throws DruidException         if the available batch was not available as a {@link Frame}
   */
  default Frame readFrame()
  {
    final RowsAndColumns rac = read();
    final Frame frame = rac.as(Frame.class);
    if (frame != null) {
      return frame;
    } else {
      throw DruidException.defensive("Got RAC[%s] which is not a frame", rac);
    }
  }

  /**
   * Returns a future that will resolve when either {@link #isFinished()} or {@link #canRead()} would
   * return true. The future will never resolve to an exception. If something exceptional has happened, the exception
   * can be retrieved from {@link #readFrame()} or {@link #read()}.
   */
  ListenableFuture<?> readabilityFuture();

  /**
   * Releases any resources associated with this readable channel. After calling this, you should not call any other
   * methods on the channel.
   */
  @Override
  void close();
}
