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

package org.apache.druid.frame.field;

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.frame.write.FrameWriterUtils;
import org.apache.druid.segment.DimensionSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Wraps a {@link DimensionSelector} and writes to rframe rows.
 *
 * See {@link StringFieldReader} for format details.
 */
public class StringFieldWriter implements FieldWriter
{
  public static final byte VALUE_TERMINATOR = (byte) 0x00;
  public static final byte ROW_TERMINATOR = (byte) 0x01;

  /**
   * Null rows are represented by {@code NULL_ROW}. Same byte value as {@link #VALUE_TERMINATOR}, but not ambiguous:
   * {@code NULL_ROW} can only occur as the first byte in a row, and {@link #VALUE_TERMINATOR} can never occur as
   * the first byte in a row.
   */
  public static final byte NULL_ROW = 0x00;

  /**
   * Different from the values in {@link org.apache.druid.common.config.NullHandling}, since we want to be able to
   * sort as bytes, and we want nulls to come before non-nulls.
   */
  public static final byte NULL_BYTE = 0x02;
  public static final byte NOT_NULL_BYTE = 0x03;

  private static final int NONNULL_ROW_MINIMUM_SIZE = 3; // NULL_BYTE + VALUE_TERMINATOR + ROW_TERMINATOR
  private static final byte NULL_ROW_SIZE = 2; // NULL_ROW + ROW_TERMINATOR

  private final DimensionSelector selector;

  public StringFieldWriter(final DimensionSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public long writeTo(final WritableMemory memory, final long position, final long maxSize)
  {
    final List<ByteBuffer> byteBuffers = FrameWriterUtils.getUtf8ByteBuffersFromStringSelector(selector, true);
    return writeUtf8ByteBuffers(memory, position, maxSize, byteBuffers);
  }

  @Override
  public void close()
  {
    // Nothing to close.
  }

  /**
   * Writes a collection of UTF-8 buffers in string-field format. Helper for {@link #writeTo}.
   * All buffers must be nonnull. Null strings must be represented as {@link FrameWriterUtils#NULL_STRING_MARKER_ARRAY}.
   *
   * @param memory      destination memory
   * @param position    position in memory to write to
   * @param maxSize     maximum number of bytes to write to memory
   * @param byteBuffers utf8 string array to write to memory
   *
   * @return number of bytes written, or -1 if "maxSize" was not enough memory
   */
  static long writeUtf8ByteBuffers(
      final WritableMemory memory,
      final long position,
      final long maxSize,
      @Nullable final List<ByteBuffer> byteBuffers
  )
  {
    if (byteBuffers == null) {
      if (maxSize < NULL_ROW_SIZE) {
        return -1;
      }

      memory.putByte(position, NULL_ROW);
      memory.putByte(position + 1, ROW_TERMINATOR);
      return NULL_ROW_SIZE;
    }

    long written = 0;

    for (final ByteBuffer utf8Datum : byteBuffers) {
      final int len = utf8Datum.remaining();

      if (written + NONNULL_ROW_MINIMUM_SIZE > maxSize) {
        return -1;
      }

      if (len == 1 && utf8Datum.get(utf8Datum.position()) == FrameWriterUtils.NULL_STRING_MARKER) {
        // Null.
        memory.putByte(position + written, NULL_BYTE);
        written++;
      } else {
        // Not null.
        if (written + len + NONNULL_ROW_MINIMUM_SIZE > maxSize) {
          return -1;
        }

        memory.putByte(position + written, NOT_NULL_BYTE);
        written++;

        if (len > 0) {
          FrameWriterUtils.copyByteBufferToMemory(utf8Datum, memory, position + written, len, false);
          written += len;
        }
      }

      memory.putByte(position + written, VALUE_TERMINATOR);
      written++;
    }

    if (written + 1 > maxSize) {
      return -1;
    }

    memory.putByte(position + written, ROW_TERMINATOR);
    written++;

    return written;
  }
}
