package io.druid.segment.data;


import com.google.common.base.Supplier;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.CountingOutputStream;
import com.google.common.primitives.Ints;
import com.metamx.common.guava.CloseQuietly;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidResourceHolder;
import io.druid.segment.CompressedPools;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class BlockLayoutSerde
{

  public static class BlockLayoutIndexedLongsSupplier implements Supplier<IndexedLongs> {

    private final GenericIndexed<ResourceHolder<ByteBuffer>> baseLongBuffers;
    private final int totalSize;
    private final int sizePer;
    private final ByteOrder order;
    private final CompressionFactory.LongEncodingFormatReader baseReader;

    public BlockLayoutIndexedLongsSupplier(int totalSize, int sizePer, ByteBuffer fromBuffer, ByteOrder order,
                                           CompressionFactory.LongEncodingFormatReader reader,
                                           CompressedObjectStrategy.CompressionStrategy strategy)
    {
      baseLongBuffers = GenericIndexed.read(fromBuffer, VSizeCompressedObjectStrategy.getBufferForOrder(
          order, strategy, reader.numBytes(sizePer)
      ));
      this.totalSize = totalSize;
      this.sizePer = sizePer;
      this.order = order;
      this.baseReader = reader;
    }

    @Override
    public IndexedLongs get()
    {
      final int div = Integer.numberOfTrailingZeros(sizePer);
      final int rem = sizePer - 1;
      final boolean powerOf2 = sizePer == (1 << div);
      if (powerOf2) {
        // this provide slightly better performance than calling the LongsEncodingReader.read, probably because Java
        // doesn't inline the method call for some reason. This should be removed when test show that performance
        // of using read method is same as directly using accessing the longbuffer
        if (baseReader instanceof LongsEncodingFormatSerde.LongsEncodingReader) {
          return new BlockLayoutIndexedLongs()
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
              return longBuffer.get(longBuffer.position() + bufferIndex);
            }

            protected void loadBuffer(int bufferNum)
            {
              CloseQuietly.close(holder);
              holder = singleThreadedLongBuffers.get(bufferNum);
              buffer = holder.get();
              longBuffer = buffer.order(order).asLongBuffer();
              currIndex = bufferNum;
            }
          };
        }
        else {
          return new BlockLayoutIndexedLongs()
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
              return reader.read(bufferIndex);
            }
          };
        }

      } else {
        return new BlockLayoutIndexedLongs();
      }
    }

    private class BlockLayoutIndexedLongs implements IndexedLongs
    {
      final CompressionFactory.LongEncodingFormatReader reader = baseReader.duplicate();
      final Indexed<ResourceHolder<ByteBuffer>> singleThreadedLongBuffers = baseLongBuffers.singleThreaded();
      int currIndex = -1;
      ResourceHolder<ByteBuffer> holder;
      ByteBuffer buffer;
      LongBuffer longBuffer;

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

        return reader.read(bufferIndex);
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
//        int bufferNum = index / sizePer;
//        int bufferIndex = index % sizePer;
//
//        int leftToFill = toFill.length;
//        while (leftToFill > 0) {
//          if (bufferNum != currIndex) {
//            loadBuffer(bufferNum);
//          }
//
//          buffer.mark();
//          buffer.position(buffer.position() + bufferIndex);
//          final int numToGet = Math.min(buffer.remaining(), leftToFill);
//          buffer.get(toFill, toFill.length - leftToFill, numToGet);
//          buffer.reset();
//          leftToFill -= numToGet;
//          ++bufferNum;
//          bufferIndex = 0;
//        }
        for (int i = 0; i < toFill.length; i++) {
          toFill[i] = get(index + i);
        }
      }

      protected void loadBuffer(int bufferNum)
      {
        CloseQuietly.close(holder);
        holder = singleThreadedLongBuffers.get(bufferNum);
        buffer = holder.get();
        currIndex = bufferNum;
        reader.setBuffer(buffer);
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


  public static class BlockLayoutLongSupplierSerializer implements LongSupplierSerializer {

    private final IOPeon ioPeon;
    private final int sizePer;
    private final CompressionFactory.LongEncodingFormatWriter writer;
    private final GenericIndexedWriter<ResourceHolder<ByteBuffer>> flattener;
    private final CompressedObjectStrategy.CompressionStrategy compression;
    private final String metaFile;
    private CountingOutputStream metaOut;

    private int numInserted = 0;

    private ByteBuffer endBuffer = null;

    public BlockLayoutLongSupplierSerializer(
        IOPeon ioPeon,
        String filenameBase,
        ByteOrder order,
        CompressionFactory.LongEncodingFormatWriter writer,
        CompressedObjectStrategy.CompressionStrategy compression
    )
    {
      this.ioPeon = ioPeon;
      this.sizePer = writer.getBlockSize(CompressedPools.BUFFER_SIZE);
      this.flattener = new GenericIndexedWriter<ResourceHolder<ByteBuffer>>(
                            ioPeon,
                            filenameBase,
                            VSizeCompressedObjectStrategy.getBufferForOrder(
                                order,
                                compression,
                                writer.getNumBytes(sizePer)
                            )
                        );
      this.metaFile = filenameBase + ".format";
      this.writer = writer;
      this.compression = compression;
    }

    public void open() throws IOException
    {
      flattener.open();
    }

    public int size()
    {
      return numInserted;
    }

    public void add(long value) throws IOException
    {
      if (numInserted % sizePer == 0) {
        if (endBuffer != null) {
          writer.close();
          endBuffer.limit(endBuffer.position());
          endBuffer.rewind();
          flattener.write(StupidResourceHolder.create(endBuffer));
        }
        endBuffer = ByteBuffer.allocate(writer.getNumBytes(sizePer));
        writer.setBuffer(endBuffer);
      }

      writer.write(value);
      ++numInserted;
    }

    public void closeAndConsolidate(ByteSink consolidatedOut) throws IOException
    {
      close();
      try (OutputStream out = consolidatedOut.openStream();
           InputStream meta = ioPeon.makeInputStream(metaFile)) {
        ByteStreams.copy(meta, out);
        flattener.combineStreams().copyTo(out);
      }
    }

    public void close() throws IOException {
      if (endBuffer != null) {
        writer.close();
        endBuffer.limit(endBuffer.position());
        endBuffer.rewind();
        flattener.write(StupidResourceHolder.create(endBuffer));
      }
      endBuffer = null;
      flattener.close();

      metaOut = new CountingOutputStream(ioPeon.makeOutputStream(metaFile));
      metaOut.write(CompressedLongsIndexedSupplier.version);
      metaOut.write(Ints.toByteArray(numInserted));
      metaOut.write(Ints.toByteArray(sizePer));
      writer.putMeta(metaOut, compression);
      metaOut.close();
    }

    public long getSerializedSize()
    {
      return metaOut.getCount() + flattener.getSerializedSize();
    }

    public void writeToChannel(WritableByteChannel channel) throws IOException
    {
      try (InputStream meta = ioPeon.makeInputStream(metaFile);
           InputStream input = flattener.combineStreams().openStream()) {
        ByteStreams.copy(Channels.newChannel(meta), channel);
        final ReadableByteChannel from = Channels.newChannel(input);
        ByteStreams.copy(from, channel);
      }
    }
  }

}
