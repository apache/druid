/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.data;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public abstract class FixedSizeCompressedObjectStrategy<T extends Buffer> extends CompressedObjectStrategy<T>
{
  private final int sizePer;

  protected FixedSizeCompressedObjectStrategy(
      ByteOrder order,
      BufferConverter<T> converter,
      CompressionStrategy compression,
      int sizePer
  )
  {
    super(order, converter, compression);
    this.sizePer = sizePer;
  }

  public int getSize() {
    return sizePer;
  }

  @Override
  protected ByteBuffer bufferFor(T val)
  {
    return ByteBuffer.allocate(converter.sizeOf(getSize())).order(order);
  }

  @Override
  protected void decompress(ByteBuffer buffer, int numBytes, ByteBuffer buf)
  {
    decompressor.decompress(buffer, numBytes, buf, converter.sizeOf(getSize()));
  }
}
