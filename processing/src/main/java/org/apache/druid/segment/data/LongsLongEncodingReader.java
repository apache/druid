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

package org.apache.druid.segment.data;

import org.apache.datasketches.memory.Memory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class LongsLongEncodingReader implements CompressionFactory.LongEncodingReader
{
  private Memory buffer;

  public LongsLongEncodingReader(ByteBuffer fromBuffer, ByteOrder order)
  {
    this.buffer = Memory.wrap(fromBuffer.slice(), order);
  }

  @Override
  public void setBuffer(ByteBuffer buffer)
  {
    this.buffer = Memory.wrap(buffer.slice(), buffer.order());
  }

  @Override
  public long read(int index)
  {
    return buffer.getLong((long) index << 3);
  }

  @Override
  public void read(final long[] out, final int outPosition, final int startIndex, final int length)
  {
    buffer.getLongArray((long) startIndex << 3, out, outPosition, length);
  }

  @Override
  public int read(long[] out, int outPosition, int[] indexes, int length, int indexOffset, int limit)
  {
    for (int i = 0; i < length; i++) {
      int index = indexes[outPosition + i] - indexOffset;
      if (index >= limit) {
        return i;
      }

      out[outPosition + i] = buffer.getLong((long) index << 3);
    }

    return length;
  }

  @Override
  public CompressionFactory.LongEncodingReader duplicate()
  {
    return this;
  }
}
