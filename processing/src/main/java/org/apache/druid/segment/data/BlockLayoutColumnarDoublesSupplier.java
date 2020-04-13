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

import com.google.common.base.Supplier;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.guava.CloseQuietly;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;

public class BlockLayoutColumnarDoublesSupplier implements Supplier<ColumnarDoubles>
{
  private final GenericIndexed<ResourceHolder<ByteBuffer>> baseDoubleBuffers;

  // The number of rows in this column.
  private final int totalSize;

  // The number of doubles per buffer.
  private final int sizePer;

  public BlockLayoutColumnarDoublesSupplier(
      int totalSize,
      int sizePer,
      ByteBuffer fromBuffer,
      ByteOrder byteOrder,
      CompressionStrategy strategy
  )
  {
    baseDoubleBuffers = GenericIndexed.read(fromBuffer, new DecompressingByteBufferObjectStrategy(byteOrder, strategy));
    this.totalSize = totalSize;
    this.sizePer = sizePer;
  }

  @Override
  public ColumnarDoubles get()
  {
    final int div = Integer.numberOfTrailingZeros(sizePer);
    final int rem = sizePer - 1;
    final boolean isPowerOf2 = sizePer == (1 << div);
    if (isPowerOf2) {
      return new BlockLayoutColumnarDoubles()
      {
        @Override
        public double get(int index)
        {
          // optimize division and remainder for powers of 2
          final int bufferNum = index >> div;

          if (bufferNum != currBufferNum) {
            loadBuffer(bufferNum);
          }

          final int bufferIndex = index & rem;
          return doubleBuffer.get(bufferIndex);
        }
      };
    } else {
      return new BlockLayoutColumnarDoubles();
    }
  }

  private class BlockLayoutColumnarDoubles implements ColumnarDoubles
  {
    final Indexed<ResourceHolder<ByteBuffer>> singleThreadedDoubleBuffers = baseDoubleBuffers.singleThreaded();

    int currBufferNum = -1;
    ResourceHolder<ByteBuffer> holder;
    /**
     * doubleBuffer's position must be 0
     */
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

      if (bufferNum != currBufferNum) {
        loadBuffer(bufferNum);
      }

      return doubleBuffer.get(bufferIndex);
    }

    @Override
    public void get(final double[] out, final int start, final int length)
    {
      // division + remainder is optimized by the compiler so keep those together
      int bufferNum = start / sizePer;
      int bufferIndex = start % sizePer;

      int p = 0;

      while (p < length) {
        if (bufferNum != currBufferNum) {
          loadBuffer(bufferNum);
        }

        final int limit = Math.min(length - p, sizePer - bufferIndex);
        final int oldPosition = doubleBuffer.position();
        try {
          doubleBuffer.position(bufferIndex);
          doubleBuffer.get(out, p, limit);
        }
        finally {
          doubleBuffer.position(oldPosition);
        }
        p += limit;
        bufferNum++;
        bufferIndex = 0;
      }
    }

    @Override
    public void get(final double[] out, final int[] indexes, final int length)
    {
      int p = 0;

      while (p < length) {
        int bufferNum = indexes[p] / sizePer;
        if (bufferNum != currBufferNum) {
          loadBuffer(bufferNum);
        }

        final int indexOffset = bufferNum * sizePer;

        int i = p;
        for (; i < length; i++) {
          int index = indexes[i] - indexOffset;
          if (index >= sizePer) {
            break;
          }

          out[i] = doubleBuffer.get(index);
        }

        assert i > p;
        p = i;
      }
    }

    protected void loadBuffer(int bufferNum)
    {
      CloseQuietly.close(holder);
      holder = singleThreadedDoubleBuffers.get(bufferNum);
      // asDoubleBuffer() makes the doubleBuffer's position = 0
      doubleBuffer = holder.get().asDoubleBuffer();
      currBufferNum = bufferNum;
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
      return "BlockCompressedColumnarDoubles_Anonymous{" +
             "currBufferNum=" + currBufferNum +
             ", sizePer=" + sizePer +
             ", numChunks=" + singleThreadedDoubleBuffers.size() +
             ", totalSize=" + totalSize +
             '}';
    }
  }
}
