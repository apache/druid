/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
      IOPeon ioPeon, final String filenameBase, final ByteOrder order, final CompressedObjectStrategy.CompressionStrategy compression
  ) throws IOException
  {
    return create(ioPeon, filenameBase, CompressedFloatsIndexedSupplier.MAX_FLOATS_IN_BUFFER, order, compression);
  }

  public static CompressedFloatsSupplierSerializer create(
      IOPeon ioPeon, final String filenameBase, final int sizePer, final ByteOrder order, final CompressedObjectStrategy.CompressionStrategy compression
  ) throws IOException
  {
    final CompressedFloatsSupplierSerializer retVal = new CompressedFloatsSupplierSerializer(
        sizePer,
        new GenericIndexedWriter<ResourceHolder<FloatBuffer>>(
            ioPeon, filenameBase, CompressedFloatBufferObjectStrategy.getBufferForOrder(order, compression, sizePer)
        ),
        compression
    );
    return retVal;
  }

  private final int sizePer;
  private final GenericIndexedWriter<ResourceHolder<FloatBuffer>> flattener;
  private final CompressedObjectStrategy.CompressionStrategy compression;

  private int numInserted = 0;

  private FloatBuffer endBuffer;

  public CompressedFloatsSupplierSerializer(
      int sizePer,
      GenericIndexedWriter<ResourceHolder<FloatBuffer>> flattener,
      CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    this.sizePer = sizePer;
    this.flattener = flattener;
    this.compression = compression;

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
      out.write(new byte[]{compression.getId()});
      ByteStreams.copy(flattener.combineStreams(), out);
    }
  }
}
