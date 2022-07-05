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

package org.apache.druid.frame.write;

import com.google.common.primitives.Ints;
import org.apache.datasketches.memory.WritableMemory;

import java.io.Closeable;
import java.nio.ByteOrder;

/**
 * Writer for {@link org.apache.druid.frame.Frame}. See that class for format information.
 *
 * Generally obtained through a {@link FrameWriters#makeFrameWriterFactory}.
 */
public interface FrameWriter extends Closeable
{
  /**
   * Write the current row to the frame that is under construction, if there is enough space to do so.
   *
   * If this method returns false on an empty frame, or in a situation where starting a new frame is impractical,
   * it is conventional (although not required) for the caller to throw {@link FrameRowTooLargeException}.
   *
   * @return true if the row was written, false if there was not enough space
   */
  boolean addSelection();

  /**
   * Returns the number of rows written so far.
   */
  int getNumRows();

  /**
   * Returns the number of bytes that would be written by {@link #writeTo} if called now.
   */
  long getTotalSize();

  /**
   * Writes the frame to the provided memory location, which must have at least {@link #getTotalSize()} bytes available.
   * Once this method is called, the frame writer is no longer usable and must be closed.
   *
   * Returns the number of bytes written, which will equal {@link #getTotalSize()}.
   *
   * @throws IllegalStateException if the provided memory does not have sufficient space to write this frame.
   */
  long writeTo(WritableMemory memory, long position);

  /**
   * Returns a frame as a newly-allocated byte array.
   * Once this method is called, the frame writer is no longer usable and must be closed.
   *
   * In general, it is more efficient to write a frame into a pre-existing buffer, but this method is provided
   * as a convenience.
   */
  default byte[] toByteArray()
  {
    // Frame sizes are expected to be much smaller than the max value of an int; do a checked cast here
    // to validate.
    final byte[] bytes = new byte[Ints.checkedCast(getTotalSize())];
    writeTo(WritableMemory.writableWrap(bytes, ByteOrder.LITTLE_ENDIAN), 0);
    return bytes;
  }

  @Override
  void close();
}
