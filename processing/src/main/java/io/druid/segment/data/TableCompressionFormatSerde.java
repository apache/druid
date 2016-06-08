package io.druid.segment.data;

import com.google.common.base.Supplier;
import com.google.common.collect.BiMap;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metamx.common.IAE;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import java.util.Set;

/**
 * Storage Format v1 :
 * Byte 1 : version
 * Byte 2 - 5 : table size
 * Byte 6 - (6 + 8 * tableSize - 1) : table
 * rest: values
 */
public class TableCompressionFormatSerde
{

  public static int MAX_TABLE_SIZE;

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
        if (tableSize < 0 || tableSize > MAX_TABLE_SIZE) {
          throw new IAE("Invalid table size[%s]", tableSize);
        }
        entryBitLength = VSizeLongSerde.getBitsForMax(tableSize);
        table = new long[tableSize];
        for (int i = 0; i < tableSize; i++) {
          table[i] = buffer.getLong();
        }
        buffer.order(order);
        deserializer = VSizeLongSerde.getDeserializer(entryBitLength, buffer, buffer.position());
      } else {
        throw new IAE("Unknown version[%s]", version);
      }
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

  public static class TableCompressedLongSupplierSerializer implements LongSupplierSerializer {

    private final IOPeon ioPeon;
    private final String valueFile;
    private final String headerFile;
    private final String metaFile;
    private CountingOutputStream valuesOut = null;

    private VSizeLongSerde.LongSerializer serializer;
    private BiMap<Long, Integer> table;
    private int numInserted = 0;

    public TableCompressedLongSupplierSerializer(
        IOPeon ioPeon,
        String filenameBase,
        ByteOrder order,
        BiMap<Long, Integer> table
    )
    {
      this.ioPeon = ioPeon;
      this.valueFile = filenameBase + ".value";
      this.headerFile = filenameBase + ".header";
      this.metaFile = filenameBase + ".meta";
      if (table.size() > MAX_TABLE_SIZE) {
        throw new IAE("Invalid table size[%s]", table.size());
      }
      this.table = table;
    }

    public void open() throws IOException
    {
      valuesOut = new CountingOutputStream(ioPeon.makeOutputStream(valueFile));
      serializer = VSizeLongSerde.getSerializer(VSizeLongSerde.getBitsForMax(table.size()), valuesOut);
    }

    public int size()
    {
      return numInserted;
    }

    public void add(long value) throws IOException
    {
      serializer.write(table.get(value));
      ++numInserted;
    }

    public void closeAndConsolidate(ByteSink consolidatedOut) throws IOException
    {
      close();
      try (OutputStream out = consolidatedOut.openStream();
           InputStream meta = ioPeon.makeInputStream(metaFile);
           InputStream header = ioPeon.makeInputStream(headerFile);
           InputStream value = ioPeon.makeInputStream(valueFile)) {
        out.write(CompressedLongsIndexedSupplier.version);
        out.write(Ints.toByteArray(numInserted));
        out.write(Ints.toByteArray(0));
        out.write(CompressionFactory.CompressionFormat.TABLE.getId());
        ByteStreams.copy(meta, out);
        ByteStreams.copy(header, out);
        ByteStreams.copy(value, out);
      }
    }

    public void close() throws IOException {
      serializer.close();
      valuesOut.close();
      try (OutputStream headerOut = ioPeon.makeOutputStream(headerFile)) {
        for (int i = 0; i < table.size() ; i++) {
          headerOut.write(Longs.toByteArray(table.inverse().get(i)));
        }
      }
      try (OutputStream metaOut = ioPeon.makeOutputStream(metaFile)) {
        metaOut.write(0x1);
        metaOut.write(Ints.toByteArray(table.size()));
      }
    }

    public long getSerializedSize()
    {
      return 1 +              // version
             Ints.BYTES +     // elements num
             Ints.BYTES +     // sizePer
             1 +              // compression id
             1 +              // format version
             Ints.BYTES +     // table size
             table.size() * Longs.BYTES + // table values
             valuesOut.getCount(); // values
    }

    public void writeToChannel(WritableByteChannel channel) throws IOException
    {
      channel.write(ByteBuffer.wrap(new byte[]{CompressedLongsIndexedSupplier.version}));
      channel.write(ByteBuffer.wrap(Ints.toByteArray(numInserted)));
      channel.write(ByteBuffer.wrap(Ints.toByteArray(0)));
      channel.write(ByteBuffer.wrap(new byte[]{CompressionFactory.CompressionFormat.TABLE.getId()}));
      try (InputStream meta = ioPeon.makeInputStream(metaFile);
           InputStream header = ioPeon.makeInputStream(headerFile);
           InputStream value = ioPeon.makeInputStream(valueFile)) {
        ByteStreams.copy(Channels.newChannel(meta), channel);
        ByteStreams.copy(Channels.newChannel(header), channel);
        ByteStreams.copy(Channels.newChannel(value), channel);
      }
    }
  }
}
