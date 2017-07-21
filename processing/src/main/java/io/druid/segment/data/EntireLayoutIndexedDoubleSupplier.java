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

import com.google.common.base.Supplier;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;

public class EntireLayoutIndexedDoubleSupplier implements Supplier<IndexedDoubles>
{
  private final int totalSize;
  private final DoubleBuffer buffer;

  public EntireLayoutIndexedDoubleSupplier(int totalSize, ByteBuffer fromBuffer, ByteOrder byteOrder)
  {
    this.totalSize = totalSize;
    this.buffer = fromBuffer.asReadOnlyBuffer().order(byteOrder).asDoubleBuffer();
  }

  @Override
  public IndexedDoubles get()
  {
    return new EntireLayoutIndexedDoubles();
  }

  private class EntireLayoutIndexedDoubles implements IndexedDoubles
  {
    @Override
    public int size()
    {
      return totalSize;
    }

    @Override
    public double get(int index)
    {
      return buffer.get(buffer.position() + index);
    }

    @Override
    public void fill(int index, double[] toFill)
    {
      if (totalSize - index < toFill.length) {
        throw new IndexOutOfBoundsException(
            String.format(
                "Cannot fill array of size[%,d] at index[%,d].  Max size[%,d]", toFill.length, index, totalSize
            )
        );
      }
      for (int i = 0; i < toFill.length; i++) {
        toFill[i] = get(index + i);
      }
    }

    @Override
    public void close()
    {

    }

    @Override
    public String toString()
    {
      return "EntireCompressedIndexedDoubles_Anonymous{" +
             ", totalSize=" + totalSize +
             '}';
    }
  }
}
