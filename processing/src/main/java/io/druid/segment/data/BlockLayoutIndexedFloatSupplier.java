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
import com.google.common.primitives.Floats;
import io.druid.collections.ResourceHolder;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;

public class BlockLayoutIndexedFloatSupplier implements Supplier<IndexedFloats>
{
  private final GenericIndexed<ResourceHolder<ByteBuffer>> baseFloatBuffers;
  private final int totalSize;
  private final int sizePer;

  public BlockLayoutIndexedFloatSupplier(
      int totalSize,
      int sizePer,
      ByteBuffer fromBuffer,
      ByteOrder order,
      CompressedObjectStrategy.CompressionStrategy strategy,
      SmooshedFileMapper mapper
  )
  {
    baseFloatBuffers = GenericIndexed.read(
        fromBuffer,
        VSizeCompressedObjectStrategy.getBufferForOrder(
            order,
            strategy,
            sizePer * Floats.BYTES
        ),
        mapper
    );
    this.totalSize = totalSize;
    this.sizePer = sizePer;
  }

  @Override
  public IndexedFloats get()
  {
    final int div = Integer.numberOfTrailingZeros(sizePer);
    final int rem = sizePer - 1;
    final boolean powerOf2 = sizePer == (1 << div);
    if (powerOf2) {
        return new BlockLayoutIndexedFloats()
        {
          @Override
          public float get(int index)
          {
            // optimize division and remainder for powers of 2
            final int bufferNum = index >> div;

            if (bufferNum != currIndex) {
              loadBuffer(bufferNum);
            }

            final int bufferIndex = index & rem;
            return floatBuffer.get(floatBuffer.position() + bufferIndex);
          }
        };
    } else {
      return new BlockLayoutIndexedFloats();
    }
  }

  private class BlockLayoutIndexedFloats implements IndexedFloats
  {
    final Indexed<ResourceHolder<ByteBuffer>> singleThreadedFloatBuffers = baseFloatBuffers.singleThreaded();
    int currIndex = -1;
    ResourceHolder<ByteBuffer> holder;
    ByteBuffer buffer;
    FloatBuffer floatBuffer;

    @Override
    public int size()
    {
      return totalSize;
    }

    @Override
    public float get(int index)
    {
      // division + remainder is optimized by the compiler so keep those together
      final int bufferNum = index / sizePer;
      final int bufferIndex = index % sizePer;

      if (bufferNum != currIndex) {
        loadBuffer(bufferNum);
      }

      return floatBuffer.get(floatBuffer.position() + bufferIndex);
    }

    @Override
    public void fill(int index, float[] toFill)
    {
      if (totalSize - index < toFill.length) {
        throw new IndexOutOfBoundsException(
            StringUtils.format(
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
      holder = singleThreadedFloatBuffers.get(bufferNum);
      buffer = holder.get();
      floatBuffer = buffer.asFloatBuffer();
      currIndex = bufferNum;
    }

    @Override
    public String toString()
    {
      return "BlockCompressedIndexedFloats_Anonymous{" +
             "currIndex=" + currIndex +
             ", sizePer=" + sizePer +
             ", numChunks=" + singleThreadedFloatBuffers.size() +
             ", totalSize=" + totalSize +
             '}';
    }

    @Override
    public void close()
    {
      if (holder != null) {
        holder.close();
      }
    }
  }
}
