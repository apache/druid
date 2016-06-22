package io.druid.segment.data;

import com.google.common.base.Supplier;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import com.google.common.primitives.Ints;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class EntireLayoutSerde
{
  public static class EntireLayoutIndexedLongSupplier implements Supplier<IndexedLongs> {

    private final int totalSize;
    private final CompressionFactory.LongEncodingFormatReader reader;

    public EntireLayoutIndexedLongSupplier(int totalSize, CompressionFactory.LongEncodingFormatReader reader) {
      this.totalSize = totalSize;
      this.reader = reader;
    }

    @Override
    public IndexedLongs get()
    {
      return new EntireLayoutIndexedLongs();
    }

    private class EntireLayoutIndexedLongs implements IndexedLongs
    {

      @Override
      public int size()
      {
        return totalSize;
      }

      @Override
      public long get(int index)
      {
        return reader.read(index);
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
        return "EntireCompressedIndexedLongs_Anonymous{" +
               ", totalSize=" + totalSize +
               '}';
      }

      @Override
      public void close() throws IOException
      {
      }
    }
  }

  public static class EntireLayoutLongSerializer implements LongSupplierSerializer {

    private final IOPeon ioPeon;
    private final String valueFile;
    private final String metaFile;
    private CountingOutputStream valuesOut;
    private CountingOutputStream metaOut;
    private final CompressionFactory.LongEncodingFormatWriter writer;

    private int numInserted = 0;

    public EntireLayoutLongSerializer(IOPeon ioPeon, String filenameBase, ByteOrder order,
                                      CompressionFactory.LongEncodingFormatWriter writer)
    {
      this.ioPeon = ioPeon;
      this.valueFile = filenameBase + ".value";
      this.metaFile = filenameBase + ".format";
      this.writer = writer;
    }

    @Override
    public void open() throws IOException
    {
      valuesOut = new CountingOutputStream(ioPeon.makeOutputStream(valueFile));
      writer.setOutputStream(valuesOut);
    }

    @Override
    public int size()
    {
      return numInserted;
    }

    @Override
    public void add(long value) throws IOException
    {
      writer.write(value);
      ++numInserted;
    }

    @Override
    public void closeAndConsolidate(ByteSink consolidatedOut) throws IOException
    {
      close();
      try (OutputStream out = consolidatedOut.openStream();
           InputStream meta = ioPeon.makeInputStream(metaFile);
           InputStream value = ioPeon.makeInputStream(valueFile)) {
        ByteStreams.copy(meta, out);
        ByteStreams.copy(value, out);
      }
    }

    @Override
    public void close() throws IOException
    {
      writer.close();
      valuesOut.close();
      metaOut = new CountingOutputStream(ioPeon.makeOutputStream(metaFile));
      metaOut.write(CompressedLongsIndexedSupplier.version);
      metaOut.write(Ints.toByteArray(numInserted));
      metaOut.write(Ints.toByteArray(0));
      writer.putMeta(metaOut, CompressedObjectStrategy.CompressionStrategy.NONE);
      metaOut.close();
    }

    @Override
    public long getSerializedSize()
    {
      return metaOut.getCount() + valuesOut.getCount();
    }

    @Override
    public void writeToChannel(WritableByteChannel channel) throws IOException
    {
      try (InputStream meta = ioPeon.makeInputStream(metaFile);
           InputStream value = ioPeon.makeInputStream(valueFile)) {
        ByteStreams.copy(Channels.newChannel(meta), channel);
        ByteStreams.copy(Channels.newChannel(value), channel);
      }
    }
  }
}
