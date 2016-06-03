package io.druid.segment.data;


import com.google.common.base.Supplier;
import com.google.common.io.Closeables;
import com.metamx.common.guava.CloseQuietly;
import io.druid.collections.ResourceHolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

public class BlockCompressionFormatSerde
{

  public static class BlockCompressedIndexedLongsSupplier implements Supplier<IndexedLongs> {

    private final GenericIndexed<ResourceHolder<LongBuffer>> baseLongBuffers;
    private final int totalSize;
    private final int sizePer;

    public BlockCompressedIndexedLongsSupplier (int totalSize, int sizePer, ByteBuffer fromBuffer, ByteOrder order,
                                                CompressedObjectStrategy.CompressionStrategy strategy)
    {
      baseLongBuffers = GenericIndexed.read(fromBuffer, CompressedLongBufferObjectStrategy.getBufferForOrder(order, strategy, sizePer));
      this.totalSize = totalSize;
      this.sizePer = sizePer;
    }

    @Override
    public IndexedLongs get()
    {
      final int div = Integer.numberOfTrailingZeros(sizePer);
      final int rem = sizePer - 1;
      final boolean powerOf2 = sizePer == (1 << div);
      if (powerOf2) {
        return new BlockCompressedIndexedLongs()
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
            return buffer.get(buffer.position() + bufferIndex);
          }
        };
      } else {
        return new BlockCompressedIndexedLongs();
      }
    }

    private class BlockCompressedIndexedLongs implements IndexedLongs
    {
      final Indexed<ResourceHolder<LongBuffer>> singleThreadedLongBuffers = baseLongBuffers.singleThreaded();
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
        final int bufferNum = index / sizePer;
        final int bufferIndex = index % sizePer;

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

      protected void loadBuffer(int bufferNum)
      {
        CloseQuietly.close(holder);
        holder = singleThreadedLongBuffers.get(bufferNum);
        buffer = holder.get();
        currIndex = bufferNum;
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
      public void close() throws IOException
      {
        Closeables.close(holder, false);
      }
    }
  }

}
