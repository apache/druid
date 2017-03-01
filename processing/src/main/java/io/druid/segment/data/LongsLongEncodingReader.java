/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.data;

import com.google.common.primitives.Longs;
import io.druid.java.util.common.io.smoosh.PositionalMemoryRegion;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class LongsLongEncodingReader implements CompressionFactory.LongEncodingReader
{
  private PositionalMemoryRegion pMemory;
  private ByteOrder order;

  public LongsLongEncodingReader(PositionalMemoryRegion pMemory, ByteOrder order)
  {
    this.pMemory = pMemory.duplicate();
    this.order = order;
  }

  private LongsLongEncodingReader(PositionalMemoryRegion memory)
  {
    this.pMemory = memory;
  }

  @Override
  public void setBuffer(ByteBuffer buffer)
  {
    this.pMemory = new PositionalMemoryRegion(buffer);
  }

  @Override
  public long read(int index)
  {
    if(order == ByteOrder.BIG_ENDIAN) {
      return Long.reverseBytes(pMemory.getLong(pMemory.position() + index * Long.BYTES));
    } else {
      return pMemory.getLong(pMemory.position() + index * Long.BYTES);
    }
  }

  @Override
  public int getNumBytes(int values)
  {
    return values * Longs.BYTES;
  }

  @Override
  public CompressionFactory.LongEncodingReader duplicate()
  {
    return new LongsLongEncodingReader(pMemory.duplicate());
  }
}
