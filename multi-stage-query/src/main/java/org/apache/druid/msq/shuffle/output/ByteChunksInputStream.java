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

package org.apache.druid.msq.shuffle.output;

import org.apache.druid.error.DruidException;

import java.io.InputStream;
import java.util.List;

/**
 * Input stream based on a list of byte arrays.
 */
public class ByteChunksInputStream extends InputStream
{
  private final List<byte[]> chunks;
  private int chunkNum;
  private int positionWithinChunk;

  /**
   * Create a new stream wrapping a list of chunks.
   *
   * @param chunks                   byte arrays
   * @param positionWithinFirstChunk starting position within the first byte array
   */
  public ByteChunksInputStream(final List<byte[]> chunks, final int positionWithinFirstChunk)
  {
    this.chunks = chunks;
    this.positionWithinChunk = positionWithinFirstChunk;
    this.chunkNum = -1;
    advanceChunk();
  }

  @Override
  public int read()
  {
    if (chunkNum >= chunks.size()) {
      return -1;
    } else {
      final byte[] currentChunk = chunks.get(chunkNum);
      final byte b = currentChunk[positionWithinChunk++];

      if (positionWithinChunk == currentChunk.length) {
        chunkNum++;
        positionWithinChunk = 0;
      }

      return b & 0xFF;
    }
  }

  @Override
  public int read(byte[] b)
  {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len)
  {
    if (len == 0) {
      return 0;
    } else if (chunkNum >= chunks.size()) {
      return -1;
    } else {
      int r = 0;

      while (r < len && chunkNum < chunks.size()) {
        final byte[] currentChunk = chunks.get(chunkNum);
        int toReadFromCurrentChunk = Math.min(len - r, currentChunk.length - positionWithinChunk);
        System.arraycopy(currentChunk, positionWithinChunk, b, off + r, toReadFromCurrentChunk);
        r += toReadFromCurrentChunk;
        positionWithinChunk += toReadFromCurrentChunk;
        if (positionWithinChunk == currentChunk.length) {
          chunkNum++;
          positionWithinChunk = 0;
        }
      }

      return r;
    }
  }

  @Override
  public void close()
  {
    chunkNum = chunks.size();
    positionWithinChunk = 0;
  }

  private void advanceChunk()
  {
    chunkNum++;

    // Verify nonempty
    if (chunkNum < chunks.size() && chunks.get(chunkNum).length == 0) {
      throw DruidException.defensive("Empty chunk not allowed");
    }
  }
}
