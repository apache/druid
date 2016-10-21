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
import com.google.common.io.Closeables;
import com.google.common.primitives.Ints;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidResourceHolder;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.segment.CompressedPools;
import it.unimi.dsi.fastutil.ints.IntIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.List;

public class CompressedIntsIndexedSupplier implements WritableSupplier<IndexedInts>
{
  public static final byte VERSION = 0x2;
  public static final int MAX_INTS_IN_BUFFER = CompressedPools.BUFFER_SIZE / Ints.BYTES;


  private final int totalSize;
  private final int sizePer;
  private final GenericIndexed<ResourceHolder<IntBuffer>> baseIntBuffers;
  private final CompressedObjectStrategy.CompressionStrategy compression;

  CompressedIntsIndexedSupplier(
      int totalSize,
      int sizePer,
      GenericIndexed<ResourceHolder<IntBuffer>> baseIntBuffers,
      CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    this.totalSize = totalSize;
    this.sizePer = sizePer;
    this.baseIntBuffers = baseIntBuffers;
    this.compression = compression;
  }

  public int size()
  {
    return totalSize;
  }

  @Override
  public IndexedInts get()
  {
    final int div = Integer.numberOfTrailingZeros(sizePer);
    final int rem = sizePer - 1;
    final boolean powerOf2 = sizePer == (1 << div);
    if(powerOf2) {
      return new CompressedIndexedInts() {
        @Override
        public int get(int index)
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
      return new CompressedIndexedInts();
    }
  }

  public long getSerializedSize()
  {
    return 1 + // version
           4 + // totalSize
           4 + // sizePer
           1 + // compressionId
           baseIntBuffers.getSerializedSize(); // data
  }

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{VERSION}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(totalSize)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(sizePer)));
    channel.write(ByteBuffer.wrap(new byte[]{compression.getId()}));
    baseIntBuffers.writeToChannel(channel);
  }

  public CompressedIntsIndexedSupplier convertByteOrder(ByteOrder order)
  {
    return new CompressedIntsIndexedSupplier(
        totalSize,
        sizePer,
        GenericIndexed.fromIterable(baseIntBuffers, CompressedIntBufferObjectStrategy.getBufferForOrder(order, compression, sizePer)),
        compression
    );
  }

  /**
   * For testing.  Do not use unless you like things breaking
   */
  GenericIndexed<ResourceHolder<IntBuffer>> getBaseIntBuffers()
  {
    return baseIntBuffers;
  }

  public static CompressedIntsIndexedSupplier fromByteBuffer(ByteBuffer buffer, ByteOrder order)
  {
    byte versionFromBuffer = buffer.get();

    if (versionFromBuffer == VERSION) {
      final int totalSize = buffer.getInt();
      final int sizePer = buffer.getInt();
      final CompressedObjectStrategy.CompressionStrategy compression = CompressedObjectStrategy.CompressionStrategy.forId(buffer.get());
      return new CompressedIntsIndexedSupplier(
          totalSize,
          sizePer,
          GenericIndexed.read(buffer, CompressedIntBufferObjectStrategy.getBufferForOrder(order, compression, sizePer)),
          compression
      );
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  public static CompressedIntsIndexedSupplier fromIntBuffer(IntBuffer buffer, final ByteOrder byteOrder, CompressedObjectStrategy.CompressionStrategy compression)
  {
    return fromIntBuffer(buffer, MAX_INTS_IN_BUFFER, byteOrder, compression);
  }

  public static CompressedIntsIndexedSupplier fromIntBuffer(
      final IntBuffer buffer, final int chunkFactor, final ByteOrder byteOrder, CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    Preconditions.checkArgument(
        chunkFactor <= MAX_INTS_IN_BUFFER, "Chunks must be <= 64k bytes. chunkFactor was[%s]", chunkFactor
    );

    return new CompressedIntsIndexedSupplier(
        buffer.remaining(),
        chunkFactor,
        GenericIndexed.fromIterable(
            new Iterable<ResourceHolder<IntBuffer>>()
            {
              @Override
              public Iterator<ResourceHolder<IntBuffer>> iterator()
              {
                return new Iterator<ResourceHolder<IntBuffer>>()
                {
                  IntBuffer myBuffer = buffer.asReadOnlyBuffer();

                  @Override
                  public boolean hasNext()
                  {
                    return myBuffer.hasRemaining();
                  }

                  @Override
                  public ResourceHolder<IntBuffer> next()
                  {
                    IntBuffer retVal = myBuffer.asReadOnlyBuffer();

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
            CompressedIntBufferObjectStrategy.getBufferForOrder(byteOrder, compression, chunkFactor)
        ),
        compression
    );
  }

  public static CompressedIntsIndexedSupplier fromList(
      final List<Integer> list , final int chunkFactor, final ByteOrder byteOrder, CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    Preconditions.checkArgument(
        chunkFactor <= MAX_INTS_IN_BUFFER, "Chunks must be <= 64k bytes. chunkFactor was[%s]", chunkFactor
    );

    return new CompressedIntsIndexedSupplier(
        list.size(),
        chunkFactor,
        GenericIndexed.fromIterable(
            new Iterable<ResourceHolder<IntBuffer>>()
            {
              @Override
              public Iterator<ResourceHolder<IntBuffer>> iterator()
              {
                return new Iterator<ResourceHolder<IntBuffer>>()
                {
                  int position = 0;

                  @Override
                  public boolean hasNext()
                  {
                    return position < list.size();
                  }

                  @Override
                  public ResourceHolder<IntBuffer> next()
                  {
                    IntBuffer retVal = IntBuffer.allocate(chunkFactor);

                    if (chunkFactor > list.size() - position) {
                      retVal.limit(list.size() - position);
                    }
                    final List<Integer> ints = list.subList(position, position + retVal.remaining());
                    for(int value : ints) {
                      retVal.put(value);
                    }
                    retVal.rewind();
                    position += retVal.remaining();

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
            CompressedIntBufferObjectStrategy.getBufferForOrder(byteOrder, compression, chunkFactor)
        ),
        compression
    );
  }

  private class CompressedIndexedInts implements IndexedInts
  {
    final Indexed<ResourceHolder<IntBuffer>> singleThreadedIntBuffers = baseIntBuffers.singleThreaded();

    int currIndex = -1;
    ResourceHolder<IntBuffer> holder;
    IntBuffer buffer;

    @Override
    public int size()
    {
      return totalSize;
    }

    @Override
    public int get(int index)
    {
      final int bufferNum = index / sizePer;
      final int bufferIndex = index % sizePer;

      if (bufferNum != currIndex) {
        loadBuffer(bufferNum);
      }

      return buffer.get(buffer.position() + bufferIndex);
    }

    @Override
    public IntIterator iterator()
    {
      return new IndexedIntsIterator(this);
    }

    @Override
    public void fill(int index, int[] toFill)
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
      holder = singleThreadedIntBuffers.get(bufferNum);
      buffer = holder.get();
      currIndex = bufferNum;
    }

    @Override
    public String toString()
    {
      return "CompressedIntsIndexedSupplier_Anonymous{" +
             "currIndex=" + currIndex +
             ", sizePer=" + sizePer +
             ", numChunks=" + singleThreadedIntBuffers.size() +
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
