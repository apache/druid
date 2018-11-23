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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.CloseQuietly;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;

public class BlockLayoutColumnarFloatsSupplier implements Supplier<ColumnarFloats>
{
  private final GenericIndexed<ResourceHolder<ByteBuffer>> baseFloatBuffers;
  private final int totalSize;
  private final int sizePer;

  public BlockLayoutColumnarFloatsSupplier(
      int totalSize,
      int sizePer,
      ByteBuffer fromBuffer,
      ByteOrder byteOrder,
      CompressionStrategy strategy
  )
  {
    baseFloatBuffers = GenericIndexed.read(fromBuffer, new DecompressingByteBufferObjectStrategy(byteOrder, strategy));
    this.totalSize = totalSize;
    this.sizePer = sizePer;
  }

  @Override
  public ColumnarFloats get()
  {
    final int div = Integer.numberOfTrailingZeros(sizePer);
    final int rem = sizePer - 1;
    final boolean isPowerOf2 = sizePer == (1 << div);
    if (isPowerOf2) {
      return new BlockLayoutColumnarFloats()
      {
        @Override
        public float get(int index)
        {
          // optimize division and remainder for powers of 2
          final int bufferNum = index >> div;

          if (bufferNum != currBufferNum) {
            loadBuffer(bufferNum);
          }

          final int bufferIndex = index & rem;
          return floatBuffer.get(bufferIndex);
        }
      };
    } else {
      return new BlockLayoutColumnarFloats();
    }
  }

  private class BlockLayoutColumnarFloats implements ColumnarFloats
  {
    final Indexed<ResourceHolder<ByteBuffer>> singleThreadedFloatBuffers = baseFloatBuffers.singleThreaded();

    int currBufferNum = -1;
    ResourceHolder<ByteBuffer> holder;
    /** floatBuffer's position must be 0 */
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

      if (bufferNum != currBufferNum) {
        loadBuffer(bufferNum);
      }

      return floatBuffer.get(bufferIndex);
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
      // asFloatBuffer() makes the floatBuffer's position = 0
      floatBuffer = holder.get().asFloatBuffer();
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
      return "BlockCompressedColumnarFloats_Anonymous{" +
             "currBufferNum=" + currBufferNum +
             ", sizePer=" + sizePer +
             ", numChunks=" + singleThreadedFloatBuffers.size() +
             ", totalSize=" + totalSize +
             '}';
    }
  }
}
