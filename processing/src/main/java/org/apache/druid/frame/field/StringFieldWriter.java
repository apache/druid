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

import com.google.common.collect.ImmutableList;
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

  /**
   * Represents the termination of a String value. This isn't merely symbolic and cannot be changed because
   * this aids in faster comparison across MVD strings. Rest all bytes are symbolic, when writing the comment,
   * and can be updated to some other value if required.
   */
  public static final byte VALUE_TERMINATOR = (byte) 0x00;

  public static final byte ROW_TERMINATOR = (byte) 0x01;

  // Different from the values in NullHandling, since we want to be able to sort as bytes, and we want
  // nulls to come before non-nulls.
  public static final byte NULL_BYTE = 0x02;
  public static final byte NOT_NULL_BYTE = 0x03;

  public static final byte NULL_ARRAY_BYTE = 0x04;
  public static final byte EMPTY_ARRAY_BYTE = 0x05;
  public static final byte NON_NULL_ARRAY_BYTE_FIRST_ELEMENT_NULL = 0x06;
  public static final byte NON_NULL_ARRAY_BYTE_FIRST_ELEMENT_NON_NULL = 0x07;

  public static final List<Byte> VALID_FIRST_BYTES = ImmutableList.of(
      NULL_ARRAY_BYTE,
      EMPTY_ARRAY_BYTE,
      NON_NULL_ARRAY_BYTE_FIRST_ELEMENT_NULL,
      NON_NULL_ARRAY_BYTE_FIRST_ELEMENT_NON_NULL
  );

  public static final List<Byte> VALID_FIRST_BYTES_LEGACY = ImmutableList.of(
      NULL_BYTE,
      NOT_NULL_BYTE
  );

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
   * @return number of bytes written, or -1 if "maxSize" was not enough memory
   */
  static long writeUtf8ByteBuffers(
      final WritableMemory memory,
      final long position,
      final long maxSize,
      @Nullable final List<ByteBuffer> byteBuffers
  )
  {
    long written = 0;

    if (maxSize < 1) {
      return -1;
    }

    if (byteBuffers == null) {
      memory.putByte(position + written, NULL_ARRAY_BYTE);
    } else {
      if (byteBuffers.size() == 0) {
        memory.putByte(position + written, EMPTY_ARRAY_BYTE);
      } else {
        if (byteBuffers.get(0).remaining() == 1 &&
            byteBuffers.get(0).get(byteBuffers.get(0).position()) == FrameWriterUtils.NULL_STRING_MARKER) {
          memory.putByte(position + written, NON_NULL_ARRAY_BYTE_FIRST_ELEMENT_NULL);
        } else {
          memory.putByte(position + written, NON_NULL_ARRAY_BYTE_FIRST_ELEMENT_NON_NULL);
        }
      }
    }

    written++;

    if (byteBuffers != null) {
      for (int i = 0; i < byteBuffers.size(); ++i) {
        ByteBuffer utf8Datum = byteBuffers.get(i);

        final int len = utf8Datum.remaining();

        if (i != 0) {
          if (written + 1 > maxSize) {
            return -1;
          }

          if (len == 1 && utf8Datum.get(utf8Datum.position()) == FrameWriterUtils.NULL_STRING_MARKER) {
            // Null.
            memory.putByte(position + written, NULL_BYTE);
          } else {
            memory.putByte(position + written, NOT_NULL_BYTE);
          }
          written++;
        }

        if (len != 1 || utf8Datum.get(utf8Datum.position()) != FrameWriterUtils.NULL_STRING_MARKER) {
          if (written + len > maxSize) {
            return -1;
          }
          if (len > 0) {
            FrameWriterUtils.copyByteBufferToMemory(utf8Datum, memory, position + written, len, false);
            written += len;
          }
        }

        if (written + 1 > maxSize) {
          return -1;
        }

        memory.putByte(position + written, VALUE_TERMINATOR);
        written++;
      }
    }

    if (written + 1 > maxSize) {
      return -1;
    }

    memory.putByte(position + written, ROW_TERMINATOR);
    written++;

    return written;
  }
}
