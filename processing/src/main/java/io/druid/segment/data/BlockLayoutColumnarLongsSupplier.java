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

package io.druid.segment.data;

import com.google.common.base.Supplier;
import io.druid.collections.ResourceHolder;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.guava.CloseQuietly;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

public class BlockLayoutColumnarLongsSupplier implements Supplier<ColumnarLongs>
{
  private final GenericIndexed<ResourceHolder<ByteBuffer>> baseLongBuffers;
  private final int totalSize;
  private final int sizePer;
  private final CompressionFactory.LongEncodingReader baseReader;

  public BlockLayoutColumnarLongsSupplier(
      int totalSize,
      int sizePer,
      ByteBuffer fromBuffer,
      ByteOrder order,
      CompressionFactory.LongEncodingReader reader,
      CompressionStrategy strategy
  )
  {
    baseLongBuffers = GenericIndexed.read(fromBuffer, new DecompressingByteBufferObjectStrategy(order, strategy));
    this.totalSize = totalSize;
    this.sizePer = sizePer;
    this.baseReader = reader;
  }

  @Override
  public ColumnarLongs get()
  {
    final int div = Integer.numberOfTrailingZeros(sizePer);
    final int rem = sizePer - 1;
    final boolean isPowerOf2 = sizePer == (1 << div);
    if (isPowerOf2) {
      // this provide slightly better performance than calling the LongsEncodingReader.read, probably because Java
      // doesn't inline the method call for some reason. This should be removed when test show that performance
      // of using read method is same as directly accessing the longbuffer
      if (baseReader instanceof LongsLongEncodingReader) {
        return new BlockLayoutColumnarLongs()
        {
          @Override
          public long get(int index)
          {
            // optimize division and remainder for powers of 2
            final int bufferNum = index >> div;

            if (bufferNum != currBufferNum) {
              loadBuffer(bufferNum);
            }

            final int bufferIndex = index & rem;
            return longBuffer.get(bufferIndex);
          }

          @Override
          protected void loadBuffer(int bufferNum)
          {
            CloseQuietly.close(holder);
            holder = singleThreadedLongBuffers.get(bufferNum);
            buffer = holder.get();
            // asLongBuffer() makes the longBuffer's position = 0
            longBuffer = buffer.asLongBuffer();
            currBufferNum = bufferNum;
          }
        };
      } else {
        return new BlockLayoutColumnarLongs()
        {
          @Override
          public long get(int index)
          {
            // optimize division and remainder for powers of 2
            final int bufferNum = index >> div;

            if (bufferNum != currBufferNum) {
              loadBuffer(bufferNum);
            }

            final int bufferIndex = index & rem;
            return reader.read(bufferIndex);
          }
        };
      }

    } else {
      return new BlockLayoutColumnarLongs();
    }
  }

  private class BlockLayoutColumnarLongs implements ColumnarLongs
  {
    final CompressionFactory.LongEncodingReader reader = baseReader.duplicate();
    final Indexed<ResourceHolder<ByteBuffer>> singleThreadedLongBuffers = baseLongBuffers.singleThreaded();

    int currBufferNum = -1;
    ResourceHolder<ByteBuffer> holder;
    ByteBuffer buffer;
    /** longBuffer's position must be 0 */
    LongBuffer longBuffer;

    @Override
    public int size()
    {
      return totalSize;
    }

    @Override
    public long get(int index)
    {
      // division + remainder is optimized by the compiler so keep those together
      final int bufferNum = index / sizePer;
      final int bufferIndex = index % sizePer;

      if (bufferNum != currBufferNum) {
        loadBuffer(bufferNum);
      }

      return reader.read(bufferIndex);
    }

    @Override
    public void fill(int index, long[] toFill)
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
      holder = singleThreadedLongBuffers.get(bufferNum);
      buffer = holder.get();
      currBufferNum = bufferNum;
      reader.setBuffer(buffer);
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
      return "BlockCompressedColumnarLongs_Anonymous{" +
             "currBufferNum=" + currBufferNum +
             ", sizePer=" + sizePer +
             ", numChunks=" + singleThreadedLongBuffers.size() +
             ", totalSize=" + totalSize +
             '}';
    }
  }
}
