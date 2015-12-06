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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.common.guava.CloseQuietly;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidResourceHolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.util.Arrays;
import java.util.List;

/**
 */
public class InMemoryCompressedFloats implements IndexedFloats
{
  public static final CompressedObjectStrategy.CompressionStrategy COMPRESSION = CompressedObjectStrategy.DEFAULT_COMPRESSION_STRATEGY;
  private final CompressedFloatBufferObjectStrategy strategy;
  private final int sizePer;

  private List<byte[]> compressedBuffers = Lists.newArrayList();
  private int numInserted = 0;
  private int numCompressed = 0;

  private ResourceHolder<FloatBuffer> holder = null;
  private FloatBuffer loadBuffer = null;
  private int loadBufferIndex = -1;

  private FloatBuffer endBuffer;

  public InMemoryCompressedFloats(
      int sizePer,
      ByteOrder order
  )
  {
    this.sizePer = sizePer;
    strategy = CompressedFloatBufferObjectStrategy.getBufferForOrder(
        order,
        COMPRESSION,
        sizePer
    );

    endBuffer = FloatBuffer.allocate(sizePer);
    endBuffer.mark();
  }

  @Override
  public int size()
  {
    return numInserted;
  }

  public int add(float value)
  {
    int retVal = -1;

    if (! endBuffer.hasRemaining()) {
      endBuffer.rewind();
      compressedBuffers.add(strategy.toBytes(StupidResourceHolder.create(endBuffer)));
      endBuffer = FloatBuffer.allocate(sizePer);
      endBuffer.mark();
      numCompressed += sizePer;
    }

    retVal = numCompressed + endBuffer.position();
    endBuffer.put(value);

    ++numInserted;
    return retVal;
  }

  public int addAll(Iterable<Float> values)
  {
    int retVal = -1;
    for (Float value : values) {
      retVal = add(value);
    }
    return retVal;
  }

  @Override
  public float get(int index)
  {
    int bufferNum = index / sizePer;
    int bufferIndex = index % sizePer;

    if (bufferNum == compressedBuffers.size()) {
      return endBuffer.get(bufferIndex);
    }
    if (bufferNum != loadBufferIndex) {
      loadBuffer(bufferNum);
    }

    return loadBuffer.get(loadBuffer.position() + bufferIndex);
  }

  @Override
  public void fill(int index, float[] toFill)
  {
    if (size() - index < toFill.length) {
      throw new IndexOutOfBoundsException(
          String.format(
              "Cannot fill array of size[%,d] at index[%,d].  Max size[%,d]", toFill.length, index, size()
          )
      );
    }

    int bufferNum = index / sizePer;
    int bufferIndex = index % sizePer;

    int leftToFill = toFill.length;
    while (leftToFill > 0) {
      if (bufferNum == compressedBuffers.size()) {
        endBuffer.mark();
        endBuffer.position(bufferIndex);
        endBuffer.get(toFill, toFill.length - leftToFill, leftToFill);
        endBuffer.rewind();
        return;
      }
      if (bufferNum != loadBufferIndex) {
        loadBuffer(bufferNum);
      }

      loadBuffer.mark();
      loadBuffer.position(loadBuffer.position() + bufferIndex);
      final int numToGet = Math.min(loadBuffer.remaining(), leftToFill);
      loadBuffer.get(toFill, toFill.length - leftToFill, numToGet);
      loadBuffer.rewind();
      leftToFill -= numToGet;
      ++bufferNum;
      bufferIndex = 0;
    }
  }

  private void loadBuffer(int bufferNum)
  {
    loadBuffer = null;
    CloseQuietly.close(holder);
    final byte[] compressedBytes = compressedBuffers.get(bufferNum);
    holder = strategy.fromByteBuffer(ByteBuffer.wrap(compressedBytes), compressedBytes.length);
    loadBuffer = holder.get();
    loadBufferIndex = bufferNum;
  }

  public CompressedFloatsIndexedSupplier toCompressedFloatsIndexedSupplier()
  {
    final FloatBuffer endBufCopy = endBuffer.asReadOnlyBuffer();
    endBufCopy.flip();
    
    return new CompressedFloatsIndexedSupplier(
        numInserted,
        sizePer,
        GenericIndexed.fromIterable(
            Iterables.<ResourceHolder<FloatBuffer>>concat(
                Iterables.transform(
                    compressedBuffers,
                    new Function<byte[], ResourceHolder<FloatBuffer>>()
                    {
                      @Override
                      public ResourceHolder<FloatBuffer> apply(byte[] input)
                      {
                        return strategy.fromByteBuffer(ByteBuffer.wrap(input), input.length);
                      }
                    }
                ),
                Arrays.<ResourceHolder<FloatBuffer>>asList(StupidResourceHolder.create(endBufCopy))
            ),
            strategy
        ),
        COMPRESSION
    );
  }

  @Override
  public void close() throws IOException
  {
    CloseQuietly.close(holder);
  }
}
