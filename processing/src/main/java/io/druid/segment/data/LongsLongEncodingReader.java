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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

public class LongsLongEncodingReader implements CompressionFactory.LongEncodingReader
{
  private LongBuffer buffer;

  public LongsLongEncodingReader(ByteBuffer fromBuffer, ByteOrder order)
  {
    this.buffer = fromBuffer.asReadOnlyBuffer().order(order).asLongBuffer();
  }

  private LongsLongEncodingReader(LongBuffer buffer)
  {
    this.buffer = buffer;
  }

  @Override
  public void setBuffer(ByteBuffer buffer)
  {
    this.buffer = buffer.asLongBuffer();
  }

  @Override
  public long read(int index)
  {
    return buffer.get(buffer.position() + index);
  }

  @Override
  public int getNumBytes(int values)
  {
    return values * Longs.BYTES;
  }

  @Override
  public CompressionFactory.LongEncodingReader duplicate()
  {
    return new LongsLongEncodingReader(buffer.duplicate());
  }
}
