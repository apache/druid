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
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metamx.common.IAE;
import com.metamx.common.guava.CloseQuietly;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidResourceHolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;

/**
 */
public class CompressedLongsIndexedSupplier implements Supplier<IndexedLongs>
{
  public static final byte version = 0x1;

  private final int totalSize;
  private final int sizePer;
  private final GenericIndexed<ResourceHolder<LongBuffer>> baseLongBuffers;

  CompressedLongsIndexedSupplier(
      int totalSize,
      int sizePer,
      GenericIndexed<ResourceHolder<LongBuffer>> baseLongBuffers
  )
  {
    this.totalSize = totalSize;
    this.sizePer = sizePer;
    this.baseLongBuffers = baseLongBuffers;
  }

  public int size()
  {
    return totalSize;
  }

  @Override
  public IndexedLongs get()
  {
    return new IndexedLongs()
    {
      int currIndex = -1;
      ResourceHolder<LongBuffer> holder;
      LongBuffer buffer;

      @Override
      public int size()
      {
        return totalSize;
      }

      @Override
      public long get(int index)
      {
        int bufferNum = index / sizePer;
        int bufferIndex = index % sizePer;

        if (bufferNum != currIndex) {
          loadBuffer(bufferNum);
        }

        return buffer.get(buffer.position() + bufferIndex);
      }

      @Override
      public void fill(int index, long[] toFill)
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

      private void loadBuffer(int bufferNum)
      {
        CloseQuietly.close(holder);
        holder = baseLongBuffers.get(bufferNum);
        buffer = holder.get();
        currIndex = bufferNum;
      }

      @Override
      public int binarySearch(long key)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public int binarySearch(long key, int from, int to)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public String toString()
      {
        return "CompressedLongsIndexedSupplier_Anonymous{" +
               "currIndex=" + currIndex +
               ", sizePer=" + sizePer +
               ", numChunks=" + baseLongBuffers.size() +
               ", totalSize=" + totalSize +
               '}';
      }

      @Override
      public void close() throws IOException
      {
        Closeables.close(holder, false);
      }
    };
  }

  public long getSerializedSize()
  {
    return baseLongBuffers.getSerializedSize() + 1 + 4 + 4;
  }

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{version}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(totalSize)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(sizePer)));
    baseLongBuffers.writeToChannel(channel);
  }

  public CompressedLongsIndexedSupplier convertByteOrder(ByteOrder order)
  {
    return new CompressedLongsIndexedSupplier(
        totalSize,
        sizePer,
        GenericIndexed.fromIterable(baseLongBuffers, CompressedLongBufferObjectStrategy.getBufferForOrder(order))
    );
  }

  /**
   * For testing.  Do not use unless you like things breaking
   */
  GenericIndexed<ResourceHolder<LongBuffer>> getBaseLongBuffers()
  {
    return baseLongBuffers;
  }

  public static CompressedLongsIndexedSupplier fromByteBuffer(ByteBuffer buffer, ByteOrder order)
  {
    byte versionFromBuffer = buffer.get();

    if (versionFromBuffer == version) {
      return new CompressedLongsIndexedSupplier(
        buffer.getInt(),
        buffer.getInt(),
        GenericIndexed.read(buffer, CompressedLongBufferObjectStrategy.getBufferForOrder(order))
      );
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  public static CompressedLongsIndexedSupplier fromLongBuffer(LongBuffer buffer, final ByteOrder byteOrder)
  {
    return fromLongBuffer(buffer, 0xFFFF / Longs.BYTES, byteOrder);
  }

  public static CompressedLongsIndexedSupplier fromLongBuffer(
      final LongBuffer buffer, final int chunkFactor, final ByteOrder byteOrder
  )
  {
    Preconditions.checkArgument(
        chunkFactor * Longs.BYTES <= 0xffff, "Chunks must be <= 64k bytes. chunkFactor was[%s]", chunkFactor
    );

    return new CompressedLongsIndexedSupplier(
        buffer.remaining(),
        chunkFactor,
        GenericIndexed.fromIterable(
            new Iterable<ResourceHolder<LongBuffer>>()
            {
              @Override
              public Iterator<ResourceHolder<LongBuffer>> iterator()
              {
                return new Iterator<ResourceHolder<LongBuffer>>()
                {
                  LongBuffer myBuffer = buffer.asReadOnlyBuffer();

                  @Override
                  public boolean hasNext()
                  {
                    return myBuffer.hasRemaining();
                  }

                  @Override
                  public ResourceHolder<LongBuffer> next()
                  {
                    LongBuffer retVal = myBuffer.asReadOnlyBuffer();

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
            CompressedLongBufferObjectStrategy.getBufferForOrder(byteOrder)
        )
    );
  }

}
