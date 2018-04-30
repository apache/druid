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

public class EntireLayoutColumnarDoublesSupplier implements Supplier<ColumnarDoubles>
{
  private final int totalSize;
  private final DoubleBuffer buffer;

  public EntireLayoutColumnarDoublesSupplier(int totalSize, ByteBuffer fromBuffer, ByteOrder byteOrder)
  {
    this.totalSize = totalSize;
    this.buffer = fromBuffer.asReadOnlyBuffer().order(byteOrder).asDoubleBuffer();
  }

  @Override
  public ColumnarDoubles get()
  {
    return new EntireLayoutColumnarDoubles();
  }

  private class EntireLayoutColumnarDoubles implements ColumnarDoubles
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
    public void close()
    {

    }

    @Override
    public String toString()
    {
      return "EntireLayoutColumnarDoubles{" +
             ", totalSize=" + totalSize +
             '}';
    }
  }
}
