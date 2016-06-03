package io.druid.segment.data;

import com.google.common.base.Supplier;
import com.metamx.common.IAE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;

public class TableCompressionFormatSerde
{

  public static class TableCompressedIndexedLongsSupplier implements Supplier<IndexedLongs>
  {
    private static final byte V1 = 0x1;

    private long table[];
    private final int totalSize;
    private final int tableSize;
    private final int entryBitLength;
    private final ByteBuffer buffer;
    private final VSizeLongSerde.LongDeserializer deserializer;

    public TableCompressedIndexedLongsSupplier (int totalSize, ByteBuffer fromBuffer, ByteOrder order)
    {
      this.buffer = fromBuffer.asReadOnlyBuffer();
      this.totalSize = totalSize;
      byte version = buffer.get();
      if (version == V1) {
        tableSize = buffer.getInt();
        if (tableSize < 0 || tableSize > 256) {
          throw new IAE("Invalid table size[%s]", tableSize);
        }
        entryBitLength = VSizeLongSerde.getBitsForMax(tableSize);
        table = new long[tableSize];
        for (int i = 0; i < tableSize; i++) {
          table[i] = buffer.getLong();
        }
        buffer.order(order);
        deserializer = VSizeLongSerde.getDeserializer(entryBitLength, buffer, buffer.position());
      }
      throw new IAE("Unknown version[%s]", version);
    }

    @Override
    public IndexedLongs get()
    {
      return new TableCompressedIndexedLongs();
    }

    private class TableCompressedIndexedLongs implements IndexedLongs
    {

      @Override
      public int size()
      {
        return totalSize;
      }

      @Override
      public long get(int index)
      {
        return table[(int)deserializer.get(index)];
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
