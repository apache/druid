package io.druid.segment.data;

import com.google.common.base.Supplier;
import com.google.common.io.Closeables;
import com.metamx.common.guava.CloseQuietly;
import io.druid.collections.ResourceHolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

public class BlockUncompressedFormatSerde
{

  public static class BlockUncompressedIndexedLongsSupplier implements Supplier<IndexedLongs>
  {
    private final int totalSize;
    private final int sizePer;
    private final int valuesOffset;
    private final ByteBuffer buffer;

    public BlockUncompressedIndexedLongsSupplier(int totalSize, int sizePer, ByteBuffer fromBuffer, ByteOrder order)
    {
      this.buffer = fromBuffer;
      this.totalSize = totalSize;
      this.sizePer = sizePer;

      buffer.position(buffer.position() + 6);
      int size = buffer.getInt();
      valuesOffset = buffer.position() + (size << 2);

      buffer.order(order);
    }

    @Override
    public IndexedLongs get()
    {
      return new BlockUncompressedIndexedLongs();
    }

    private class BlockUncompressedIndexedLongs implements IndexedLongs
    {
      @Override
      public int size()
      {
        return totalSize;
      }

      @Override
      public long get(int index)
      {
        return buffer.getLong(valuesOffset + (index / sizePer + 1) * 4 + index * 8);
      }

      @Override
      public void fill(int index, long[] toFill)
      {
//      if (totalSize - index < toFill.length) {
//        throw new IndexOutOfBoundsException(
//            String.format(
//                "Cannot fill array of size[%,d] at index[%,d].  Max size[%,d]", toFill.length, index, totalSize
//            )
//        );
//      }
//
//      int bufferNum = index / sizePer;
//      int bufferIndex = index % sizePer;
//
//      int leftToFill = toFill.length;
//      while (leftToFill > 0) {
//        if (bufferNum != currIndex) {
//          loadBuffer(bufferNum);
//        }
//
//        buffer.mark();
//        buffer.position(buffer.position() + bufferIndex);
//        final int numToGet = Math.min(buffer.remaining(), leftToFill);
//        buffer.get(toFill, toFill.length - leftToFill, numToGet);
//        buffer.reset();
//        leftToFill -= numToGet;
//        ++bufferNum;
//        bufferIndex = 0;
//      }
      }

      @Override
      public String toString()
      {
        return "BlockUncompressedIndexedLongs_Anonymous{" +
               ", sizePer=" + sizePer +
               ", totalSize=" + totalSize +
               '}';
      }

      @Override
      public void close() throws IOException
      {
      }
    }
  }
}
