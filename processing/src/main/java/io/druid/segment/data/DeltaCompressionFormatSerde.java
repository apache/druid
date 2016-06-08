package io.druid.segment.data;

import com.google.common.base.Supplier;
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

/**
 * Storage Format v1 :
 * Byte 1 : version
 * Byte 2 - 9 : base value
 * Byte 10 - 13 : number of bits per value
 * Byte (14 + x * ValueBytes) - (14 + (x+1) * ValueBytes - 1): xth value
 */
public class DeltaCompressionFormatSerde
{

  public static class DeltaCompressedIndexedLongsSupplier implements Supplier<IndexedLongs>
  {
    private static final byte V1 = 0x1;

    private final int totalSize;
    private final long base;
    private final int entryBitLength;
    private final ByteBuffer buffer;
    private final VSizeLongSerde.LongDeserializer deserializer;

    public DeltaCompressedIndexedLongsSupplier (int totalSize, ByteBuffer fromBuffer, ByteOrder order)
    {
      this.buffer = fromBuffer.asReadOnlyBuffer();
      this.totalSize = totalSize;
      byte version = buffer.get();
      if (version == V1) {
        base = buffer.getLong();
        entryBitLength = buffer.getInt();
        buffer.order(order);
        deserializer = VSizeLongSerde.getDeserializer(entryBitLength, buffer, buffer.position());
      } else {
        throw new IAE("Unknown version[%s]", version);
      }
    }

    @Override
    public IndexedLongs get()
    {
      return new DeltaCompressedIndexedLongs();
    }

    private class DeltaCompressedIndexedLongs implements IndexedLongs
    {

      @Override
      public int size()
      {
        return totalSize;
      }

      @Override
      public long get(int index)
      {
        return base + deserializer.get(index);
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

  public static class DeltaCompressedLongSupplierSerializer implements LongSupplierSerializer {

    private final IOPeon ioPeon;
    private final String valueFile;
    private final String metaFile;
    private CountingOutputStream valuesOut = null;

    private VSizeLongSerde.LongSerializer serializer;
    private long base;
    private int bitsPerValue;
    private int numInserted = 0;

    public DeltaCompressedLongSupplierSerializer(
        IOPeon ioPeon,
        String filenameBase,
        ByteOrder order,
        long base,
        long delta
    )
    {
      this.ioPeon = ioPeon;
      this.valueFile = filenameBase + ".value";
      this.metaFile = filenameBase + ".meta";
      this.base = base;
      this.bitsPerValue = VSizeLongSerde.getBitsForMax(delta + 1);
    }

    public void open() throws IOException
    {
      valuesOut = new CountingOutputStream(ioPeon.makeOutputStream(valueFile));
      serializer = VSizeLongSerde.getSerializer(bitsPerValue, valuesOut);
    }

    public int size()
    {
      return numInserted;
    }

    public void add(long value) throws IOException
    {
      serializer.write(value);
      ++numInserted;
    }

    public void closeAndConsolidate(ByteSink consolidatedOut) throws IOException
    {
      close();
      try (OutputStream out = consolidatedOut.openStream();
           InputStream meta = ioPeon.makeInputStream(metaFile);
           InputStream value = ioPeon.makeInputStream(valueFile)) {
        out.write(CompressedLongsIndexedSupplier.version);
        out.write(Ints.toByteArray(numInserted));
        out.write(Ints.toByteArray(0));
        out.write(CompressionFactory.CompressionFormat.DELTA.getId());
        ByteStreams.copy(meta, out);
        ByteStreams.copy(value, out);
      }
    }

    public void close() throws IOException {
      serializer.close();
      valuesOut.close();
      try (OutputStream metaOut = ioPeon.makeOutputStream(metaFile)) {
        metaOut.write(0x1);
        metaOut.write(Longs.toByteArray(base));
        metaOut.write(Ints.toByteArray(bitsPerValue));
      }
    }

    public long getSerializedSize()
    {
      return 1 +              // version
             Ints.BYTES +     // elements num
             Ints.BYTES +     // sizePer
             1 +              // compression id
             1 +              // format version
             Longs.BYTES +    // base value
             Ints.BYTES +     // bits per value
             valuesOut.getCount(); // values
    }

    public void writeToChannel(WritableByteChannel channel) throws IOException
    {
      channel.write(ByteBuffer.wrap(new byte[]{CompressedLongsIndexedSupplier.version}));
      channel.write(ByteBuffer.wrap(Ints.toByteArray(numInserted)));
      channel.write(ByteBuffer.wrap(Ints.toByteArray(0)));
      channel.write(ByteBuffer.wrap(new byte[]{CompressionFactory.CompressionFormat.DELTA.getId()}));
      try (InputStream meta = ioPeon.makeInputStream(metaFile);
           InputStream value = ioPeon.makeInputStream(valueFile)) {
        ByteStreams.copy(Channels.newChannel(meta), channel);
        ByteStreams.copy(Channels.newChannel(value), channel);
      }
    }
  }
}
