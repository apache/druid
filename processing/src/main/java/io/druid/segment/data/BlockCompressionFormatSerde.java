package io.druid.segment.data;


import com.google.common.base.Supplier;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.primitives.Ints;
import com.metamx.common.guava.CloseQuietly;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidResourceHolder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

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


  public static class BlockCompressedLongSupplierSerializer implements LongSupplierSerializer {

    private final int sizePer;
    private final GenericIndexedWriter<ResourceHolder<LongBuffer>> flattener;
    private final CompressedObjectStrategy.CompressionStrategy compression;

    private int numInserted = 0;

    private LongBuffer endBuffer;

    public BlockCompressedLongSupplierSerializer(
        IOPeon ioPeon,
        String filenameBase,
        ByteOrder order,
        CompressedObjectStrategy.CompressionStrategy compression
    )
    {
      this.sizePer = CompressedLongsIndexedSupplier.MAX_LONGS_IN_BUFFER;
      this.flattener = new GenericIndexedWriter<ResourceHolder<LongBuffer>>(
                            ioPeon,
                            filenameBase,
                            CompressedLongBufferObjectStrategy.getBufferForOrder(
                                order,
                                compression,
                                CompressedLongsIndexedSupplier.MAX_LONGS_IN_BUFFER
                            )
                        );
      this.compression = compression;

      endBuffer = LongBuffer.allocate(sizePer);
      endBuffer.mark();
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
      if (!endBuffer.hasRemaining()) {
        endBuffer.rewind();
        flattener.write(StupidResourceHolder.create(endBuffer));
        endBuffer = LongBuffer.allocate(sizePer);
        endBuffer.mark();
      }

      endBuffer.put(value);
      ++numInserted;
    }

    public void closeAndConsolidate(ByteSink consolidatedOut) throws IOException
    {
      close();
      try (OutputStream out = consolidatedOut.openStream()) {
        out.write(CompressedLongsIndexedSupplier.version);
        out.write(Ints.toByteArray(numInserted));
        out.write(Ints.toByteArray(sizePer));
        out.write(new byte[]{compression.getId()});
        flattener.combineStreams().copyTo(out);
      }
    }

    public void close() throws IOException {
      endBuffer.limit(endBuffer.position());
      endBuffer.rewind();
      flattener.write(StupidResourceHolder.create(endBuffer));
      endBuffer = null;
      flattener.close();
    }

    public long getSerializedSize()
    {
      return 1 +              // version
             Ints.BYTES +     // elements num
             Ints.BYTES +     // sizePer
             1 +              // compression id
             flattener.getSerializedSize();
    }

    public void writeToChannel(WritableByteChannel channel) throws IOException
    {
      channel.write(ByteBuffer.wrap(new byte[]{CompressedLongsIndexedSupplier.version}));
      channel.write(ByteBuffer.wrap(Ints.toByteArray(numInserted)));
      channel.write(ByteBuffer.wrap(Ints.toByteArray(sizePer)));
      channel.write(ByteBuffer.wrap(new byte[]{compression.getId()}));
      try (InputStream input = flattener.combineStreams().openStream()) {
        final ReadableByteChannel from = Channels.newChannel(input);
        ByteStreams.copy(from, channel);
      }
    }
  }

}
