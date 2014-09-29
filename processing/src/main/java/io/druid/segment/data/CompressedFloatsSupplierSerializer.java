/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.data;

import com.google.common.io.ByteStreams;
import com.google.common.io.OutputSupplier;
import com.google.common.primitives.Ints;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidResourceHolder;

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
        new GenericIndexedWriter<ResourceHolder<FloatBuffer>>(
            ioPeon, filenameBase, CompressedFloatBufferObjectStrategy.getBufferForOrder(order)
        )
    );
    return retVal;
  }

  private final int sizePer;
  private final GenericIndexedWriter<ResourceHolder<FloatBuffer>> flattener;

  private int numInserted = 0;

  private FloatBuffer endBuffer;

  public CompressedFloatsSupplierSerializer(
      int sizePer,
      GenericIndexedWriter<ResourceHolder<FloatBuffer>> flattener
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

    try (OutputStream out = consolidatedOut.getOutput()) {
      out.write(CompressedFloatsIndexedSupplier.version);
      out.write(Ints.toByteArray(numInserted));
      out.write(Ints.toByteArray(sizePer));
      ByteStreams.copy(flattener.combineStreams(), out);
    }
  }
}
