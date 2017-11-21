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

package io.druid.segment;

import com.google.common.annotations.VisibleForTesting;
import io.druid.io.Channels;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.io.Closer;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.data.CompressedVSizeIntsIndexedSupplier;
import io.druid.segment.data.CompressionStrategy;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedIterable;
import io.druid.segment.data.IndexedMultivalue;
import io.druid.segment.data.WritableSupplier;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;

/**
 * Format -
 * byte 1 - version
 * offsets - indexed integers of length num of rows + 1 representing offsets of starting index of first element of each row in values index and last element equal to length of values column,
 * the last element in the offsets represents the total length of values column.
 * values - indexed integer representing values in each row
 */

public class CompressedVSizeIndexedSupplier implements WritableSupplier<IndexedMultivalue<IndexedInts>>
{
  private static final byte version = 0x2;
  //offsets - indexed integers of length num of rows + 1 representing offsets of starting index of first element of each row in values index
  // last element represents the length of values column
  private final CompressedVSizeIntsIndexedSupplier offsetSupplier;

  //values - indexed integers representing actual values in each row
  private final CompressedVSizeIntsIndexedSupplier valueSupplier;

  private CompressedVSizeIndexedSupplier(
      CompressedVSizeIntsIndexedSupplier offsetSupplier,
      CompressedVSizeIntsIndexedSupplier valueSupplier
  )
  {
    this.offsetSupplier = offsetSupplier;
    this.valueSupplier = valueSupplier;
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    return 1 + offsetSupplier.getSerializedSize() + valueSupplier.getSerializedSize();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    Channels.writeFully(channel, ByteBuffer.wrap(new byte[]{version}));
    offsetSupplier.writeTo(channel, smoosher);
    valueSupplier.writeTo(channel, smoosher);
  }

  public static CompressedVSizeIndexedSupplier fromByteBuffer(ByteBuffer buffer, ByteOrder order)
  {
    byte versionFromBuffer = buffer.get();

    if (versionFromBuffer == version) {
      CompressedVSizeIntsIndexedSupplier offsetSupplier = CompressedVSizeIntsIndexedSupplier.fromByteBuffer(
          buffer,
          order
      );
      CompressedVSizeIntsIndexedSupplier valueSupplier = CompressedVSizeIntsIndexedSupplier.fromByteBuffer(
          buffer,
          order
      );
      return new CompressedVSizeIndexedSupplier(offsetSupplier, valueSupplier);
    }
    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  @VisibleForTesting
  public static CompressedVSizeIndexedSupplier fromIterable(
      final Iterable<IndexedInts> objectsIterable,
      final int maxValue,
      final ByteOrder byteOrder,
      final CompressionStrategy compression,
      final Closer closer
  )
  {
    Iterator<IndexedInts> objects = objectsIterable.iterator();
    IntList offsetList = new IntArrayList();
    IntList values = new IntArrayList();

    int offset = 0;
    while (objects.hasNext()) {
      IndexedInts next = objects.next();
      offsetList.add(offset);
      for (int i = 0; i < next.size(); i++) {
        values.add(next.get(i));
      }
      offset += next.size();
    }
    offsetList.add(offset);
    int offsetMax = offset;
    CompressedVSizeIntsIndexedSupplier headerSupplier = CompressedVSizeIntsIndexedSupplier.fromList(
        offsetList,
        offsetMax,
        CompressedVSizeIntsIndexedSupplier.maxIntsInBufferForValue(offsetMax),
        byteOrder,
        compression,
        closer
    );
    CompressedVSizeIntsIndexedSupplier valuesSupplier = CompressedVSizeIntsIndexedSupplier.fromList(
        values,
        maxValue,
        CompressedVSizeIntsIndexedSupplier.maxIntsInBufferForValue(maxValue),
        byteOrder,
        compression,
        closer
    );
    return new CompressedVSizeIndexedSupplier(headerSupplier, valuesSupplier);
  }


  @Override
  public IndexedMultivalue<IndexedInts> get()
  {
    return new CompressedVSizeIndexed(offsetSupplier.get(), valueSupplier.get());
  }

  public static class CompressedVSizeIndexed implements IndexedMultivalue<IndexedInts>
  {
    private final IndexedInts offsets;
    private final IndexedInts values;


    CompressedVSizeIndexed(IndexedInts offsets, IndexedInts values)
    {
      this.offsets = offsets;
      this.values = values;
    }

    @Override
    public void close() throws IOException
    {
      offsets.close();
      values.close();
    }

    @Override
    public Class<? extends IndexedInts> getClazz()
    {
      return IndexedInts.class;
    }

    @Override
    public int size()
    {
      return offsets.size() - 1;
    }

    @Override
    public IndexedInts get(int index)
    {
      final int offset = offsets.get(index);
      final int size = offsets.get(index + 1) - offset;

      return new IndexedInts()
      {
        @Override
        public int size()
        {
          return size;
        }

        @Override
        public int get(int index)
        {
          if (index >= size) {
            throw new IAE("Index[%d] >= size[%d]", index, size);
          }
          return values.get(index + offset);
        }

        @Override
        public void close() throws IOException
        {
          // no-op
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("values", values);
        }
      };
    }

    @Override
    public int indexOf(IndexedInts value)
    {
      throw new UnsupportedOperationException("Reverse lookup not allowed.");
    }

    @Override
    public Iterator<IndexedInts> iterator()
    {
      return IndexedIterable.create(this).iterator();
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("offsets", offsets);
      inspector.visit("values", values);
    }
  }

}
