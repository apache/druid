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

package org.apache.druid.segment.writeout;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Appendable byte sequence for temporary storage. Methods inherited from {@link OutputStream}, {@link
 * WritableByteChannel} and {@link #writeInt(int)} append to the sequence. Methods {@link
 * #writeTo(WritableByteChannel)} and {@link #asInputStream()} allow to write the sequence somewhere else. {@link
 * #readFully} allows to access the sequence randomly.
 *
 * WriteOutBytes is a resource that is managed by {@link SegmentWriteOutMedium}, so it's own {@link #close()} method
 * does nothing. However WriteOutBytes should appear closed, i. e. {@link #isOpen()} returns false, after the parental
 * SegmentWriteOutMedium is closed.
 */
public abstract class WriteOutBytes extends OutputStream implements WritableByteChannel
{
  /**
   * Writes 4 bytes of the given value in big-endian order, i. e. similar to {@link java.io.DataOutput#writeInt(int)}.
   */
  public abstract void writeInt(int v) throws IOException;

  /**
   * Returns the number of bytes written to this WriteOutBytes so far.
   */
  public abstract long size();

  /**
   * Takes all bytes that are written to this WriteOutBytes so far and writes them into the given channel.
   */
  public abstract void writeTo(WritableByteChannel channel) throws IOException;

  /**
   * Creates a finite {@link InputStream} with the bytes that are written to this WriteOutBytes so far. The returned
   * InputStream must be closed properly after it's used up.
   */
  public abstract InputStream asInputStream() throws IOException;

  /**
   * Reads bytes from the byte sequences, represented by this WriteOutBytes, at the random position, into the given
   * buffer.
   *
   * @throws java.nio.BufferUnderflowException if the byte sequence from the given pos ends before the given buffer
   * is filled
   * @throws IllegalArgumentException if the given pos is negative
   */
  public abstract void readFully(long pos, ByteBuffer buffer) throws IOException;

  /**
   * @deprecated does nothing.
   */
  @Deprecated
  @Override
  public final void close()
  {
    // Does nothing.
  }
}
