/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.data;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.io.Closeables;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.metamx.common.IAE;
import com.metamx.common.guava.CloseQuietly;
import io.druid.collections.ResourcePool;
import io.druid.collections.StupidResourceHolder;
import io.druid.segment.CompressedPools;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;

/**
 */
public class CompressedFloatsIndexedSupplier implements Supplier<IndexedFloats>
{
  public static final byte LZF_VERSION = 0x1;
  public static final byte version = 0x2;
  public static final int MAX_FLOATS_IN_BUFFER = CompressedPools.BUFFER_SIZE / Floats.BYTES;

  private final int totalSize;
  private final int sizePer;
  private final GenericIndexed<ResourcePool.ResourceHolder<FloatBuffer>> baseFloatBuffers;
  private final CompressedObjectStrategy.CompressionStrategy compression;

  CompressedFloatsIndexedSupplier(
      int totalSize,
      int sizePer,
      GenericIndexed<ResourcePool.ResourceHolder<FloatBuffer>> baseFloatBuffers,
      CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    this.totalSize = totalSize;
    this.sizePer = sizePer;
    this.baseFloatBuffers = baseFloatBuffers;
    this.compression = compression;
  }

  public int size()
  {
    return totalSize;
  }

  @Override
  public IndexedFloats get()
  {
    final int div = Integer.numberOfTrailingZeros(sizePer);
    final int rem = sizePer - 1;
    final boolean powerOf2 = sizePer == (1 << div);
    if(powerOf2) {
      return new CompressedIndexedFloats() {
        @Override
        public float get(int index)
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
      return new CompressedIndexedFloats();
    }
  }

  public long getSerializedSize()
  {
    return baseFloatBuffers.getSerializedSize() + 1 + 4 + 4 + 1;
  }

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{version}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(totalSize)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(sizePer)));
    channel.write(ByteBuffer.wrap(new byte[]{compression.getId()}));
    baseFloatBuffers.writeToChannel(channel);
  }

  public CompressedFloatsIndexedSupplier convertByteOrder(ByteOrder order)
  {
    return new CompressedFloatsIndexedSupplier(
        totalSize,
        sizePer,
        GenericIndexed.fromIterable(baseFloatBuffers, CompressedFloatBufferObjectStrategy.getBufferForOrder(order, compression, sizePer)),
        compression
    );
  }

  /**
   * For testing. Do not depend on unless you like things breaking.
   */
  GenericIndexed<ResourcePool.ResourceHolder<FloatBuffer>> getBaseFloatBuffers()
  {
    return baseFloatBuffers;
  }

  public static CompressedFloatsIndexedSupplier fromByteBuffer(ByteBuffer buffer, ByteOrder order)
  {
    byte versionFromBuffer = buffer.get();

    if (versionFromBuffer == version) {
      final int totalSize = buffer.getInt();
      final int sizePer = buffer.getInt();
      final CompressedObjectStrategy.CompressionStrategy compression =
          CompressedObjectStrategy.CompressionStrategy.forId(buffer.get());

      return new CompressedFloatsIndexedSupplier(
          totalSize,
          sizePer,
          GenericIndexed.read(
              buffer,
              CompressedFloatBufferObjectStrategy.getBufferForOrder(
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
      return new CompressedFloatsIndexedSupplier(
          totalSize,
          sizePer,
          GenericIndexed.read(
              buffer,
              CompressedFloatBufferObjectStrategy.getBufferForOrder(
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

  public static CompressedFloatsIndexedSupplier fromFloatBuffer(FloatBuffer buffer, final ByteOrder order, CompressedObjectStrategy.CompressionStrategy compression)
  {
    return fromFloatBuffer(buffer, MAX_FLOATS_IN_BUFFER, order, compression);
  }

  public static CompressedFloatsIndexedSupplier fromFloatBuffer(
      final FloatBuffer buffer, final int chunkFactor, final ByteOrder order, final CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    Preconditions.checkArgument(
        chunkFactor <= MAX_FLOATS_IN_BUFFER, "Chunks must be <= 64k bytes. chunkFactor was[%s]", chunkFactor
    );

    return new CompressedFloatsIndexedSupplier(
        buffer.remaining(),
        chunkFactor,
        GenericIndexed.fromIterable(
            new Iterable<ResourcePool.ResourceHolder<FloatBuffer>>()
            {
              @Override
              public Iterator<ResourcePool.ResourceHolder<FloatBuffer>> iterator()
              {
                return new Iterator<ResourcePool.ResourceHolder<FloatBuffer>>()
                {
                  FloatBuffer myBuffer = buffer.asReadOnlyBuffer();

                  @Override
                  public boolean hasNext()
                  {
                    return myBuffer.hasRemaining();
                  }

                  @Override
                  public ResourcePool.ResourceHolder<FloatBuffer> next()
                  {
                    final FloatBuffer retVal = myBuffer.asReadOnlyBuffer();

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
            CompressedFloatBufferObjectStrategy.getBufferForOrder(order, compression, chunkFactor)
        ),
        compression
    );
  }

  private class CompressedIndexedFloats implements IndexedFloats
  {
    int currIndex = -1;
    ResourcePool.ResourceHolder<FloatBuffer> holder;
    FloatBuffer buffer;

    @Override
    public int size()
    {
      return totalSize;
    }

    @Override
    public float get(final int index)
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
    public void fill(int index, float[] toFill)
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
      holder = baseFloatBuffers.get(bufferNum);
      buffer = holder.get();
      currIndex = bufferNum;
    }

    @Override
    public String toString()
    {
      return "CompressedFloatsIndexedSupplier_Anonymous{" +
             "currIndex=" + currIndex +
             ", sizePer=" + sizePer +
             ", numChunks=" + baseFloatBuffers.size() +
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
