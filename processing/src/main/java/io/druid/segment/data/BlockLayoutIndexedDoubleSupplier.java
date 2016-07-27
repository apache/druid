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
import com.google.common.io.Closeables;
import com.google.common.primitives.Doubles;
import com.metamx.common.guava.CloseQuietly;
import io.druid.collections.ResourceHolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;

public class BlockLayoutIndexedDoubleSupplier implements Supplier<IndexedDoubles>
{
  private final GenericIndexed<ResourceHolder<ByteBuffer>> baseDoubleBuffers;
  private final int totalSize;
  private final int sizePer;

  public BlockLayoutIndexedDoubleSupplier(
      int totalSize, int sizePer, ByteBuffer fromBuffer, ByteOrder order,
      CompressedObjectStrategy.CompressionStrategy strategy
  )
  {
    baseDoubleBuffers = GenericIndexed.read(fromBuffer, VSizeCompressedObjectStrategy.getBufferForOrder(
        order, strategy, sizePer * Doubles.BYTES
    ));
    this.totalSize = totalSize;
    this.sizePer = sizePer;
  }

  @Override
  public IndexedDoubles get()
  {
    final int div = Integer.numberOfTrailingZeros(sizePer);
    final int rem = sizePer - 1;
    final boolean powerOf2 = sizePer == (1 << div);
    if (powerOf2) {
        return new BlockLayoutIndexedDoubles()
        {
          @Override
          public double get(int index)
          {
            // optimize division and remainder for powers of 2
            final int bufferNum = index >> div;

            if (bufferNum != currIndex) {
              loadBuffer(bufferNum);
            }

            final int bufferIndex = index & rem;
            return doubleBuffer.get(doubleBuffer.position() + bufferIndex);
          }
        };
    } else {
      return new BlockLayoutIndexedDoubles();
    }
  }

  private class BlockLayoutIndexedDoubles implements IndexedDoubles
  {
    final Indexed<ResourceHolder<ByteBuffer>> singleThreadedDoubleBuffers = baseDoubleBuffers.singleThreaded();
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
      // division + remainder is optimized by the compiler so keep those together
      final int bufferNum = index / sizePer;
      final int bufferIndex = index % sizePer;

      if (bufferNum != currIndex) {
        loadBuffer(bufferNum);
      }

      return doubleBuffer.get(doubleBuffer.position() + bufferIndex);
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

    protected void loadBuffer(int bufferNum)
    {
      CloseQuietly.close(holder);
      holder = singleThreadedDoubleBuffers.get(bufferNum);
      buffer = holder.get();
      doubleBuffer = buffer.asDoubleBuffer();
      currIndex = bufferNum;
    }

    @Override
    public String toString()
    {
      return "BlockCompressedIndexedDoubles_Anonymous{" +
             "currIndex=" + currIndex +
             ", sizePer=" + sizePer +
             ", numChunks=" + singleThreadedDoubleBuffers.size() +
             ", totalSize=" + totalSize +
             '}';
    }

    @Override
    public void close() throws IOException
    {
      Closeables.close(holder, false);
    }
  }
}
