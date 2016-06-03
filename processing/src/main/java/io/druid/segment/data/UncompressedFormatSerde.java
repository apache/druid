package io.druid.segment.data;

import com.google.common.base.Supplier;
import com.google.common.io.Closeables;
import com.metamx.common.IAE;
import com.metamx.common.guava.CloseQuietly;
import io.druid.collections.ResourceHolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

public class UncompressedFormatSerde
{

  public static class UncompressedIndexedLongsSupplier implements Supplier<IndexedLongs>
  {

    private static final byte V1 = 0x1;
    private final int totalSize;
    private final ByteBuffer buffer;

    public UncompressedIndexedLongsSupplier (int totalSize, ByteBuffer fromBuffer, ByteOrder order)
    {
      this.totalSize = totalSize;
      this.buffer = fromBuffer.asReadOnlyBuffer();
      byte version = buffer.get();
      if (version == V1) {
        buffer.order(order);
        buffer.limit(buffer.position() + totalSize * 8);
      }
      throw new IAE("Unknown version[%s]", version);
    }

    @Override
    public IndexedLongs get()
    {
      return new UncompressedIndexedLongs();
    }

    private class UncompressedIndexedLongs implements IndexedLongs
    {

      @Override
      public int size()
      {
        return totalSize;
      }

      @Override
      public long get(int index)
      {
        return buffer.getLong(buffer.position() + index << 3);
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
        return "UncompressedIndexedLongs_Anonymous{" +
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
