/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.index.v1;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.OutputSupplier;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metamx.druid.collect.ResourceHolder;
import com.metamx.druid.collect.StupidResourceHolder;
import com.metamx.druid.kv.FlattenedArrayWriter;
import com.metamx.druid.kv.IOPeon;

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
