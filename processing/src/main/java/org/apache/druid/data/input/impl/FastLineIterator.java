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

package org.apache.druid.data.input.impl;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.NoSuchElementException;

/**
 * Like the Apache Commons LineIterator, but faster.
 *
 * Use {@link Strings} or {@link Bytes} as appropriate. Bytes is faster.
 */
public abstract class FastLineIterator<T> implements CloseableIterator<T>
{
  // visible for tests
  static final int BUFFER_SIZE = 512;

  private static final byte CR = (byte) '\r';
  private static final byte LF = (byte) '\n';

  private final InputStream source;
  private ByteBuffer buffer;
  private int lineStart = 0;
  private int position = 0;
  private int limit = 0;
  private boolean endOfStream;
  private T nextLine;

  /**
   * {@link #LF} in every byte.
   */
  private static final long LF_REPEAT = firstOccurrencePattern(LF);

  /**
   * Constructor; a local buffer will be created
   *
   * @param source
   */
  protected FastLineIterator(InputStream source)
  {
    this(source, new byte[BUFFER_SIZE]);
  }

  /**
   * Constructor; BYO buffer.
   *
   * Existing contents of the buffer will be destroyed.
   *
   * @param source
   * @param buffer a buffer used for between-read calls
   */
  protected FastLineIterator(InputStream source, byte[] buffer)
  {
    Preconditions.checkNotNull(source);
    Preconditions.checkNotNull(buffer);
    this.source = source;
    this.nextLine = null;
    setBuffer(buffer);
  }

  @Override
  public void close() throws IOException
  {
    nextLine = null;
    buffer = null;
    source.close();
  }

  @Override
  public boolean hasNext()
  {
    //noinspection VariableNotUsedInsideIf
    if (nextLine != null) {
      return true;
    }

    nextLine = readNextLine();

    return nextLine != null;
  }

  @Override
  public T next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException("no more lines");
    }

    T result = nextLine;
    nextLine = null;
    return result;
  }

  private T readNextLine()
  {
    while (true) {
      // See if there's another line already available in the buffer.
      if (endOfStream) {
        return readLineFromBuffer();
      } else {
        // Look for next LF with 8-byte stride.
        while (position < limit - Long.BYTES) {
          final long w = buffer.getLong(position);
          final int index = firstOccurrence(w, LF_REPEAT);
          if (index < Long.BYTES) {
            position = position + index;
            return readLineFromBuffer();
          } else {
            position += Long.BYTES;
          }
        }

        // Look for next LF with 1-byte stride.
        for (; position < limit; position++) {
          if (buffer.get(position) == LF) {
            return readLineFromBuffer();
          }
        }
      }

      // No line available in the buffer.
      // Ensure space exists to read at least one more byte.
      final int available = buffer.capacity() - limit;

      if (available == 0) {
        final int currentLength = limit - lineStart;

        if (lineStart == 0) {
          // Allocate a larger buffer.
          final byte[] newBuf = new byte[buffer.capacity() * 2];
          System.arraycopy(buffer.array(), lineStart, newBuf, 0, currentLength);
          setBuffer(newBuf);
        } else {
          // Move current line to the start of the existing buffer.
          System.arraycopy(buffer.array(), lineStart, buffer.array(), 0, currentLength);
        }

        position -= lineStart;
        limit -= lineStart;
        lineStart = 0;
      }

      // Read as much as we can.
      try {
        final int bytesRead = source.read(buffer.array(), limit, buffer.capacity() - limit);
        if (bytesRead < 0) {
          // End of stream.
          endOfStream = true;
          return readLineFromBuffer();
        }

        limit += bytesRead;
      }
      catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  @Nullable
  private T readLineFromBuffer()
  {
    for (; position < limit && buffer.get(position) != LF; position++) {
      // Skip to limit or next LF
    }

    final boolean isLf = position < limit && buffer.get(position) == LF;

    final int lineEnd;
    if (isLf && position > lineStart && buffer.get(position - 1) == CR) {
      // CR LF
      lineEnd = position - 1;
    } else if (isLf) {
      // LF only
      lineEnd = position;
    } else if (endOfStream) {
      // End of stream
      lineEnd = position;
    } else {
      // There wasn't a line after all
      throw DruidException.defensive("No line to read");
    }

    if (lineStart == limit && endOfStream) {
      // null signifies no more lines are coming.
      return null;
    }

    final T retVal = makeObject(buffer.array(), lineStart, lineEnd - lineStart);
    if (position < limit) {
      position++;
    }
    lineStart = position;
    return retVal;
  }

  protected abstract T makeObject(byte[] bytes, int offset, int length);

  private void setBuffer(final byte[] buffer)
  {
    this.buffer = ByteBuffer.wrap(buffer).order(ByteOrder.BIG_ENDIAN);
  }

  /**
   * Find the first {@link #LF} byte in a long. Returns 8 if there are no {@link #LF} bytes.
   *
   * @param n       input long
   * @param pattern pattern generated by repeating the byte 8 times. Use
   *                {@link #firstOccurrencePattern(byte)} to compute.
   */
  private static int firstOccurrence(long n, long pattern)
  {
    // Xor with LF_REPEAT to turn LF bytes into zero-bytes.
    final long xored = n ^ pattern;

    // Apply test from https://graphics.stanford.edu/~seander/bithacks.html#ValueInWord, which zeroes out all
    // non-zero bytes, and sets the high bits for bytes that were zero.
    final long zeroTest = (((xored - 0x0101010101010101L) & ~(xored) & 0x8080808080808080L));

    // Count number of leading zeroes, which will be a multiple of 8, then divide by 8.
    return Long.numberOfLeadingZeros(zeroTest) >>> 3;
  }

  /**
   * Generate a search pattern for {@link #firstOccurrence(long, long)}.
   */
  private static long firstOccurrencePattern(final byte b)
  {
    return Longs.fromBytes(b, b, b, b, b, b, b, b);
  }

  public static class Strings extends FastLineIterator<String>
  {
    public Strings(final InputStream source)
    {
      super(source);
    }

    @Override
    protected String makeObject(byte[] bytes, int offset, int length)
    {
      return StringUtils.fromUtf8(bytes, offset, length);
    }
  }

  public static class Bytes extends FastLineIterator<byte[]>
  {
    public Bytes(final InputStream source)
    {
      super(source);
    }

    @Override
    protected byte[] makeObject(byte[] bytes, int offset, int length)
    {
      final byte[] retVal = new byte[length];
      System.arraycopy(bytes, offset, retVal, 0, length);
      return retVal;
    }
  }
}
