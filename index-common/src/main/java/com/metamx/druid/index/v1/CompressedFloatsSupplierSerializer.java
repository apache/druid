package com.metamx.druid.index.v1;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.OutputSupplier;
import com.google.common.primitives.Ints;
import com.metamx.druid.collect.ResourceHolder;
import com.metamx.druid.collect.StupidResourceHolder;
import com.metamx.druid.kv.FlattenedArrayWriter;
import com.metamx.druid.kv.IOPeon;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;

/**
 */
public class CompressedFloatsSupplierSerializer
{
  public static CompressedFloatsSupplierSerializer create(
      IOPeon ioPeon, final String filenameBase, final ByteOrder order
  ) throws IOException
  {
    return create(ioPeon, filenameBase, CompressedFloatsIndexedSupplier.MAX_FLOATS_IN_BUFFER, order);
  }

  public static CompressedFloatsSupplierSerializer create(
      IOPeon ioPeon, final String filenameBase, final int sizePer, final ByteOrder order
  ) throws IOException
  {
    final CompressedFloatsSupplierSerializer retVal = new CompressedFloatsSupplierSerializer(
        sizePer,
        new FlattenedArrayWriter<ResourceHolder<FloatBuffer>>(
            ioPeon, filenameBase, CompressedFloatBufferObjectStrategy.getBufferForOrder(order)
        )
    );
    return retVal;
  }

  private final int sizePer;
  private final FlattenedArrayWriter<ResourceHolder<FloatBuffer>> flattener;

  private int numInserted = 0;

  private FloatBuffer endBuffer;

  public CompressedFloatsSupplierSerializer(
      int sizePer,
      FlattenedArrayWriter<ResourceHolder<FloatBuffer>> flattener
  )
  {
    this.sizePer = sizePer;
    this.flattener = flattener;

    endBuffer = FloatBuffer.allocate(sizePer);
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

  public void add(float value) throws IOException
  {
    if (! endBuffer.hasRemaining()) {
      endBuffer.rewind();
      flattener.write(StupidResourceHolder.create(endBuffer));
      endBuffer = FloatBuffer.allocate(sizePer);
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

      out.write(CompressedFloatsIndexedSupplier.version);
      out.write(Ints.toByteArray(numInserted));
      out.write(Ints.toByteArray(sizePer));
      ByteStreams.copy(flattener.combineStreams(), out);
    }
    finally {
      Closeables.closeQuietly(out);
    }
  }
}
