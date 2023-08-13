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
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;

/**
 * Like the Apache Commons LineIterator, but faster.
 */
public class FastLineIterator implements CloseableIterator<String>
{
  // visible for tests
  static final int BUFFER_SIZE = 512;

  private static final ThreadLocal<byte[]> BUFFER_LOCAL = ThreadLocal.withInitial(() -> new byte[BUFFER_SIZE]);

  private static final byte CR = (byte) '\r';
  private static final byte LF = (byte) '\n';

  private final InputStream source;
  private final ByteArrayList buffer;

  private String nextLine;

  /**
   * Constructor; a local buffer will be created
   * @param source
   */
  public FastLineIterator(InputStream source)
  {
    this(source, new ByteArrayList());
  }

  /**
   * Constructor; BYO buffer.
   *
   * Existing contents of the buffer will be destroyed.
   * @param source
   * @param buffer a buffer used for between-read calls
   */
  public FastLineIterator(InputStream source, ByteArrayList buffer)
  {
    Preconditions.checkNotNull(source);
    Preconditions.checkNotNull(buffer);
    this.source = source;
    this.nextLine = null;
    this.buffer = buffer;
    this.buffer.size(0);
  }

  @Override
  public void close() throws IOException
  {
    nextLine = null;
    source.close();
    // Note: do not remove the thread local buffer; retain it for reuse later
  }

  @Override
  public boolean hasNext()
  {
    //noinspection VariableNotUsedInsideIf
    if (nextLine != null) {
      return true;
    }

    readNextLine();

    return nextLine != null;
  }

  @Override
  public String next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException("no more lines");
    }

    String result = nextLine;
    nextLine = null;
    return result;
  }

  void readNextLine()
  {
    byte[] load = BUFFER_LOCAL.get();

    boolean endOfFile = false;

    // load data until finished or found a line feed
    int indexOfLf = buffer.indexOf(LF);
    while (!endOfFile && indexOfLf < 0) {
      int readCount;

      try {
        readCount = source.read(load);
      }
      catch (IOException e) {
        nextLine = null;
        throw new IllegalStateException(e);
      }

      if (readCount < 0) {
        endOfFile = true;
      } else {
        int sizeBefore = buffer.size();
        buffer.addElements(buffer.size(), load, 0, readCount);

        // check if there were any LFs in the newly collected data
        for (int i = 0; i < readCount; i++) {
          if (load[i] == LF) {
            indexOfLf = sizeBefore + i;
            break;
          }
        }
      }
    }

    if (endOfFile && buffer.size() == 0) {
      // empty line and end of file
      nextLine = null;

    } else if (indexOfLf < 0) {
      // no LF at all; end of input
      nextLine = StringUtils.fromUtf8(buffer);
      buffer.removeElements(0, buffer.size());

    } else if (indexOfLf >= 1 && buffer.getByte(indexOfLf - 1) == CR) {
      // CR LF
      nextLine = StringUtils.fromUtf8(buffer.elements(), 0, indexOfLf - 1);
      buffer.removeElements(0, indexOfLf + 1);

    } else {
      // LF only
      nextLine = StringUtils.fromUtf8(buffer.elements(), 0, indexOfLf);
      buffer.removeElements(0, indexOfLf + 1);
    }
  }
}
