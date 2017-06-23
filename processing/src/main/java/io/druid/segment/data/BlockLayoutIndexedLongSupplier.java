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
import io.druid.collections.ResourceHolder;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

public class BlockLayoutIndexedLongSupplier implements Supplier<IndexedLongs>
{

  private final GenericIndexed<ResourceHolder<ByteBuffer>> baseLongBuffers;
  private final int totalSize;
  private final int sizePer;
  private final CompressionFactory.LongEncodingReader baseReader;

  public BlockLayoutIndexedLongSupplier(
      int totalSize,
      int sizePer,
      ByteBuffer fromBuffer,
      ByteOrder order,
      CompressionFactory.LongEncodingReader reader,
      CompressedObjectStrategy.CompressionStrategy strategy,
      SmooshedFileMapper fileMapper
  )
  {
    baseLongBuffers = GenericIndexed.read(
        fromBuffer,
        VSizeCompressedObjectStrategy.getBufferForOrder(
            order,
            strategy,
            reader.getNumBytes(sizePer)
        ),
        fileMapper
    );
    this.totalSize = totalSize;
    this.sizePer = sizePer;
    this.baseReader = reader;
  }

  @Override
  public IndexedLongs get()
  {
    final int div = Integer.numberOfTrailingZeros(sizePer);
    final int rem = sizePer - 1;
    final boolean powerOf2 = sizePer == (1 << div);
    if (powerOf2) {
      // this provide slightly better performance than calling the LongsEncodingReader.read, probably because Java
      // doesn't inline the method call for some reason. This should be removed when test show that performance
      // of using read method is same as directly accessing the longbuffer
      if (baseReader instanceof LongsLongEncodingReader) {
        return new BlockLayoutIndexedLongs()
        {
          @Override
          public long get(int index)
          {
            // optimize division and remainder for powers of 2
            final int bufferNum = index >> div;

            if (bufferNum != currIndex) {
              loadBuffer(bufferNum);
            }

            final int bufferIndex = index & rem;
            return longBuffer.get(longBuffer.position() + bufferIndex);
          }

          @Override
          protected void loadBuffer(int bufferNum)
          {
            CloseQuietly.close(holder);
            holder = singleThreadedLongBuffers.get(bufferNum);
            buffer = holder.get();
            longBuffer = buffer.asLongBuffer();
            currIndex = bufferNum;
          }
        };
      } else {
        return new BlockLayoutIndexedLongs()
        {
          @Override
          public long get(int index)
          {
            // optimize division and remainder for powers of 2
            final int bufferNum = index >> div;

            if (bufferNum != currIndex) {
              loadBuffer(bufferNum);
            }

            final int bufferIndex = index & rem;
            return reader.read(bufferIndex);
          }
        };
      }

    } else {
      return new BlockLayoutIndexedLongs();
    }
  }

  private class BlockLayoutIndexedLongs implements IndexedLongs
  {
    final CompressionFactory.LongEncodingReader reader = baseReader.duplicate();
    final Indexed<ResourceHolder<ByteBuffer>> singleThreadedLongBuffers = baseLongBuffers.singleThreaded();
    int currIndex = -1;
    ResourceHolder<ByteBuffer> holder;
    ByteBuffer buffer;
    LongBuffer longBuffer;

    @Override
    public int size()
    {
      return totalSize;
    }

    @Override
    public long get(int index)
    {
      final int bufferNum = index / sizePer;
      final int bufferIndex = index % sizePer;

      if (bufferNum != currIndex) {
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
      currIndex = bufferNum;
      reader.setBuffer(buffer);
    }

    @Override
    public String toString()
    {
      return "BlockCompressedIndexedLongs_Anonymous{" +
             "currIndex=" + currIndex +
             ", sizePer=" + sizePer +
             ", numChunks=" + singleThreadedLongBuffers.size() +
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
