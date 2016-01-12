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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.io.Closeables;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.metamx.common.IAE;
import com.metamx.common.guava.CloseQuietly;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidResourceHolder;
import io.druid.segment.CompressedPools;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;

/**
 */
public class CompressedDoublesIndexedSupplier implements Supplier<IndexedDoubles>
{
  public static final byte LZF_VERSION = 0x1;
  public static final byte version = 0x2;
  public static final int MAX_DOUBLES_IN_BUFFER = CompressedPools.BUFFER_SIZE / Doubles.BYTES;

  private final int totalSize;
  private final int sizePer;
  private final GenericIndexed<ResourceHolder<DoubleBuffer>> baseDoubleBuffers;
  private final CompressedObjectStrategy.CompressionStrategy compression;

  CompressedDoublesIndexedSupplier(
      int totalSize,
      int sizePer,
      GenericIndexed<ResourceHolder<DoubleBuffer>> baseDoubleBuffers,
      CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    this.totalSize = totalSize;
    this.sizePer = sizePer;
    this.baseDoubleBuffers = baseDoubleBuffers;
    this.compression = compression;
  }

  public int size()
  {
    return totalSize;
  }

  @Override
  public IndexedDoubles get()
  {
    final int div = Integer.numberOfTrailingZeros(sizePer);
    final int rem = sizePer - 1;
    final boolean powerOf2 = sizePer == (1 << div);
    if(powerOf2) {
      return new CompressedIndexedDoubles() {
        @Override
        public double get(int index)
        {
          // optimize division and remainder for powers of 2
          final int bufferNum = index >> div;

          if (bufferNum != currIndex) {
            loadBuffer(bufferNum);
          }

          final int bufferIndex = index & rem;
          return buffer.get(buffer.position() + bufferIndex);
        }
      };
    } else {
      return new CompressedIndexedDoubles();
    }
  }

  public long getSerializedSize()
  {
    return baseDoubleBuffers.getSerializedSize() + 1 + 4 + 4 + 1;
  }

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{version}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(totalSize)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(sizePer)));
    channel.write(ByteBuffer.wrap(new byte[]{compression.getId()}));
    baseDoubleBuffers.writeToChannel(channel);
  }

  public CompressedDoublesIndexedSupplier convertByteOrder(ByteOrder order)
  {
    return new CompressedDoublesIndexedSupplier(
        totalSize,
        sizePer,
        GenericIndexed.fromIterable(baseDoubleBuffers, CompressedDoubleBufferObjectStrategy.getBufferForOrder(order, compression, sizePer)),
        compression
    );
  }

  /**
   * For testing. Do not depend on unless you like things breaking.
   */
  GenericIndexed<ResourceHolder<DoubleBuffer>> getBaseDoubleBuffers()
  {
    return baseDoubleBuffers;
  }

  public static CompressedDoublesIndexedSupplier fromByteBuffer(ByteBuffer buffer, ByteOrder order)
  {
    byte versionFromBuffer = buffer.get();

    if (versionFromBuffer == version) {
      final int totalSize = buffer.getInt();
      final int sizePer = buffer.getInt();
      final CompressedObjectStrategy.CompressionStrategy compression =
          CompressedObjectStrategy.CompressionStrategy.forId(buffer.get());

      return new CompressedDoublesIndexedSupplier(
          totalSize,
          sizePer,
          GenericIndexed.read(
              buffer,
              CompressedDoubleBufferObjectStrategy.getBufferForOrder(
                  order,
                  compression,
                  sizePer
              )
          ),
          compression
      );
    } else if (versionFromBuffer == LZF_VERSION) {
      final int totalSize = buffer.getInt();
      final int sizePer = buffer.getInt();
      final CompressedObjectStrategy.CompressionStrategy compression = CompressedObjectStrategy.CompressionStrategy.LZF;
      return new CompressedDoublesIndexedSupplier(
          totalSize,
          sizePer,
          GenericIndexed.read(
              buffer,
              CompressedDoubleBufferObjectStrategy.getBufferForOrder(
                  order,
                  compression,
                  sizePer
              )
          ),
          compression
      );
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  public static CompressedDoublesIndexedSupplier fromDoubleBuffer(DoubleBuffer buffer, final ByteOrder order, CompressedObjectStrategy.CompressionStrategy compression)
  {
    return fromDoubleBuffer(buffer, MAX_DOUBLES_IN_BUFFER, order, compression);
  }

  public static CompressedDoublesIndexedSupplier fromDoubleBuffer(
      final DoubleBuffer buffer, final int chunkFactor, final ByteOrder order, final CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    Preconditions.checkArgument(
        chunkFactor <= MAX_DOUBLES_IN_BUFFER, "Chunks must be <= 64k bytes. chunkFactor was[%s]", chunkFactor
    );

    return new CompressedDoublesIndexedSupplier(
        buffer.remaining(),
        chunkFactor,
        GenericIndexed.fromIterable(
            new Iterable<ResourceHolder<DoubleBuffer>>()
            {
              @Override
              public Iterator<ResourceHolder<DoubleBuffer>> iterator()
              {
                return new Iterator<ResourceHolder<DoubleBuffer>>()
                {
                  DoubleBuffer myBuffer = buffer.asReadOnlyBuffer();

                  @Override
                  public boolean hasNext()
                  {
                    return myBuffer.hasRemaining();
                  }

                  @Override
                  public ResourceHolder<DoubleBuffer> next()
                  {
                    final DoubleBuffer retVal = myBuffer.asReadOnlyBuffer();

                    if (chunkFactor < myBuffer.remaining()) {
                      retVal.limit(retVal.position() + chunkFactor);
                    }
                    myBuffer.position(myBuffer.position() + retVal.remaining());

                    return StupidResourceHolder.create(retVal);
                  }

                  @Override
                  public void remove()
                  {
                    throw new UnsupportedOperationException();
                  }
                };
              }
            },
            CompressedDoubleBufferObjectStrategy.getBufferForOrder(order, compression, chunkFactor)
        ),
        compression
    );
  }

  private class CompressedIndexedDoubles implements IndexedDoubles
  {
    final Indexed<ResourceHolder<DoubleBuffer>> singleThreadedDoubleBuffers = baseDoubleBuffers.singleThreaded();

    int currIndex = -1;
    ResourceHolder<DoubleBuffer> holder;
    DoubleBuffer buffer;

    @Override
    public int size()
    {
      return totalSize;
    }

    @Override
    public double get(final int index)
    {
      // division + remainder is optimized by the compiler so keep those together
      final int bufferNum = index / sizePer;
      final int bufferIndex = index % sizePer;

      if (bufferNum != currIndex) {
        loadBuffer(bufferNum);
      }
      return buffer.get(buffer.position() + bufferIndex);
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

      int bufferNum = index / sizePer;
      int bufferIndex = index % sizePer;

      int leftToFill = toFill.length;
      while (leftToFill > 0) {
        if (bufferNum != currIndex) {
          loadBuffer(bufferNum);
        }

        buffer.mark();
        buffer.position(buffer.position() + bufferIndex);
        final int numToGet = Math.min(buffer.remaining(), leftToFill);
        buffer.get(toFill, toFill.length - leftToFill, numToGet);
        buffer.reset();
        leftToFill -= numToGet;
        ++bufferNum;
        bufferIndex = 0;
      }
    }

    protected void loadBuffer(int bufferNum)
    {
      CloseQuietly.close(holder);
      holder = singleThreadedDoubleBuffers.get(bufferNum);
      buffer = holder.get();
      currIndex = bufferNum;
    }

    @Override
    public String toString()
    {
      return "CompressedDoublesIndexedSupplier_Anonymous{" +
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
