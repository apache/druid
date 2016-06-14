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
import java.nio.LongBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

/**
 * Storage Format v1 :
 * Byte 1 : version
 * Byte (2 + x * ValueBytes) - (2 + (x+1) * ValueBytes - 1): xth value
 */
public class UncompressedFormatSerde
{

  public static class UncompressedIndexedLongsSupplier implements Supplier<IndexedLongs>
  {

    private static final byte V1 = 0x1;
    private final int totalSize;
    private final ByteBuffer buffer;
    private final LongBuffer lbuffer;

    public UncompressedIndexedLongsSupplier (int totalSize, ByteBuffer fromBuffer, ByteOrder order)
    {
      this.totalSize = totalSize;
      this.buffer = fromBuffer.asReadOnlyBuffer();
      byte version = buffer.get();
      if (version == V1) {
        buffer.order(order);
        buffer.limit(buffer.position() + totalSize * 8);
        lbuffer = buffer.asLongBuffer();
      } else {
        throw new IAE("Unknown version[%s]", version);
      }
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
        return buffer.getLong(buffer.position() + (index << 3));
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
        lbuffer.position(index);
        lbuffer.get(toFill);
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


  public static class UncompressedLongSupplierSerializer implements LongSupplierSerializer {

    private final IOPeon ioPeon;
    private final String valueFile;
    private final String metaFile;
    private CountingOutputStream valuesOut = null;

    private int numInserted = 0;

    private ByteBuffer orderBuffer;

    public UncompressedLongSupplierSerializer(
        IOPeon ioPeon,
        String filenameBase,
        ByteOrder order
    )
    {
      this.ioPeon = ioPeon;
      this.valueFile = filenameBase + ".value";
      this.metaFile = filenameBase + ".meta";

      orderBuffer = ByteBuffer.allocate(Longs.BYTES);
      orderBuffer.order(order);
    }

    public void open() throws IOException
    {
      valuesOut = new CountingOutputStream(ioPeon.makeOutputStream(valueFile));
    }

    public int size()
    {
      return numInserted;
    }

    public void add(long value) throws IOException
    {
      orderBuffer.rewind();
      orderBuffer.putLong(value);
      valuesOut.write(orderBuffer.array());
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
        out.write(CompressionFactory.CompressionFormat.UNCOMPRESSED_NEW.getId());
        ByteStreams.copy(meta, out);
        ByteStreams.copy(value, out);
      }
    }

    public void close() throws IOException {
      valuesOut.close();

      try (OutputStream metaOut = ioPeon.makeOutputStream(metaFile)) {
        metaOut.write(0x1);
      }
    }

    public long getSerializedSize()
    {
      return 1 +              // version
             Ints.BYTES +     // elements num
             Ints.BYTES +     // sizePer
             1 +              // compression id
             1 +              // format version
             valuesOut.getCount(); // values
    }

    public void writeToChannel(WritableByteChannel channel) throws IOException
    {
      channel.write(ByteBuffer.wrap(new byte[]{CompressedLongsIndexedSupplier.version}));
      channel.write(ByteBuffer.wrap(Ints.toByteArray(numInserted)));
      channel.write(ByteBuffer.wrap(Ints.toByteArray(0)));
      channel.write(ByteBuffer.wrap(new byte[]{CompressionFactory.CompressionFormat.UNCOMPRESSED_NEW.getId()}));
      try (InputStream meta = ioPeon.makeInputStream(metaFile);
           InputStream value = ioPeon.makeInputStream(valueFile)) {
        ByteStreams.copy(Channels.newChannel(meta), channel);
        ByteStreams.copy(Channels.newChannel(value), channel);
      }
    }
  }
}
