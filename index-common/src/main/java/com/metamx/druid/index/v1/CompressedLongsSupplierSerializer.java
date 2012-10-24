package com.metamx.druid.index.v1;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.OutputSupplier;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metamx.druid.collect.ResourceHolder;
import com.metamx.druid.collect.StupidResourceHolder;
import com.metamx.druid.kv.FlattenedArrayWriter;
import com.metamx.druid.kv.IOPeon;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

/**
 */
public class CompressedLongsSupplierSerializer
{
  public static CompressedLongsSupplierSerializer create(
      IOPeon ioPeon, final String filenameBase, final ByteOrder order
  ) throws IOException
  {
    final CompressedLongsSupplierSerializer retVal = new CompressedLongsSupplierSerializer(
        0xFFFF / Longs.BYTES,
        new FlattenedArrayWriter<ResourceHolder<LongBuffer>>(
            ioPeon, filenameBase, CompressedLongBufferObjectStrategy.getBufferForOrder(order)
        )
    );
    return retVal;
  }


  private final int sizePer;
  private final FlattenedArrayWriter<ResourceHolder<LongBuffer>> flattener;

  private int numInserted = 0;

  private LongBuffer endBuffer;

  public CompressedLongsSupplierSerializer(
      int sizePer,
      FlattenedArrayWriter<ResourceHolder<LongBuffer>> flattener
  )
  {
    this.sizePer = sizePer;
    this.flattener = flattener;

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
    if (! endBuffer.hasRemaining()) {
      endBuffer.rewind();
      flattener.write(StupidResourceHolder.create(endBuffer));
      endBuffer = LongBuffer.allocate(sizePer);
      endBuffer.mark();
    }

    endBuffer.put(value);
    ++numInserted;
  }

  public void closeAndConsolidate(OutputSupplier<? extends OutputStream> consolidatedOut) throws IOException
  {
    endBuffer.limit(endBuffer.position());
    endBuffer.rewind();
    flattener.write(StupidResourceHolder.create(endBuffer));
    endBuffer = null;
    
    flattener.close();

    OutputStream out = null;
    try {
      out = consolidatedOut.getOutput();

      out.write(CompressedLongsIndexedSupplier.version);
      out.write(Ints.toByteArray(numInserted));
      out.write(Ints.toByteArray(sizePer));
      ByteStreams.copy(flattener.combineStreams(), out);
    }
    finally {
      Closeables.closeQuietly(out);
    }
  }
}
