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

      byte version = buffer.get();
      if (version == GenericIndexed.version) {
        buffer.position(buffer.position() + 5);
        int size = buffer.getInt();
        valuesOffset = buffer.position() + (size << 2);
        buffer.order(order);
      } else {
        throw new IAE("Unknown version[%s]", version);
      }
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
        return buffer.getLong(valuesOffset + ((index / sizePer + 1) << 2) + (index << 3));
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
        for (int i = 0; i < toFill.length; i++) {
          toFill[i] = get(index + i);
        }
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
