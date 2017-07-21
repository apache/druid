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
import com.google.common.primitives.Doubles;
import io.druid.collections.ResourceHolder;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;


public class BlockLayoutIndexedDoubleSupplier implements Supplier<IndexedDoubles>
{
  private final GenericIndexed<ResourceHolder<ByteBuffer>> baseDoubleBuffers;
  private final int totalSize;
  private final int sizePer;

  public BlockLayoutIndexedDoubleSupplier(
      int totalSize,
      int sizePer,
      ByteBuffer fromBuffer,
      ByteOrder byteOrder,
      CompressedObjectStrategy.CompressionStrategy strategy,
      SmooshedFileMapper fileMapper
  )
  {

    baseDoubleBuffers = GenericIndexed.read(
        fromBuffer,
        VSizeCompressedObjectStrategy.getBufferForOrder(byteOrder,
                                                        strategy,
                                                        sizePer * Doubles.BYTES
        ),
        fileMapper
    );

    this.totalSize = totalSize;
    this.sizePer = sizePer;
  }

  @Override
  public IndexedDoubles get()
  {
    return new BlockLayoutIndexedDoubles();
  }

  private class BlockLayoutIndexedDoubles implements IndexedDoubles
  {
    final Indexed<ResourceHolder<ByteBuffer>> resourceHolderIndexed = baseDoubleBuffers.singleThreaded();
    int currIndex = -1;
    ResourceHolder<ByteBuffer> holder;
    ByteBuffer buffer;
    DoubleBuffer doubleBuffer;
    @Override
    public int size()
    {
      return totalSize;
    }

    @Override
    public double get(int index)
    {
      final int bufferNum = index / sizePer;
      final int bufferIndex = index % sizePer;

      if (bufferNum != currIndex) {
        loadBuffer(bufferNum);
      }

      return doubleBuffer.get(doubleBuffer.position() + bufferIndex);
    }

    protected void loadBuffer(int bufferNum)
    {
      CloseQuietly.close(holder);
      holder = resourceHolderIndexed.get(bufferNum);
      buffer = holder.get();
      doubleBuffer = buffer.asDoubleBuffer();
      currIndex = bufferNum;
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
      if (holder != null) {
        holder.close();
      }
    }

    @Override
    public String toString()
    {
      return "BlockCompressedIndexedDoubles_Anonymous{" +
             "currIndex=" + currIndex +
             ", sizePer=" + sizePer +
             ", numChunks=" + resourceHolderIndexed.size() +
             ", totalSize=" + totalSize +
             '}';
    }
  }
}
