/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.data;

import com.google.common.annotations.VisibleForTesting;
import io.druid.io.Channels;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.io.Closer;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
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
 * offsets - {@link ColumnarInts} of length num of rows + 1 representing offsets of starting index of first element of
 *           each row in values index and last element equal to length of values column, the last element in the offsets
 *           represents the total length of values column.
 * values - {@link ColumnarInts} representing concatenated values of all rows
 */
public class CompressedVSizeColumnarMultiIntsSupplier implements WritableSupplier<ColumnarMultiInts>
{
  private static final byte version = 0x2;

  /** See class-level comment */
  private final CompressedVSizeColumnarIntsSupplier offsetSupplier;
  /** See class-level comment */
  private final CompressedVSizeColumnarIntsSupplier valueSupplier;

  private CompressedVSizeColumnarMultiIntsSupplier(
      CompressedVSizeColumnarIntsSupplier offsetSupplier,
      CompressedVSizeColumnarIntsSupplier valueSupplier
  )
  {
    this.offsetSupplier = offsetSupplier;
    this.valueSupplier = valueSupplier;
  }

  @Override
  public long getSerializedSize()
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

  public static CompressedVSizeColumnarMultiIntsSupplier fromByteBuffer(ByteBuffer buffer, ByteOrder order)
  {
    byte versionFromBuffer = buffer.get();

    if (versionFromBuffer == version) {
      CompressedVSizeColumnarIntsSupplier offsetSupplier = CompressedVSizeColumnarIntsSupplier.fromByteBuffer(
          buffer,
          order
      );
      CompressedVSizeColumnarIntsSupplier valueSupplier = CompressedVSizeColumnarIntsSupplier.fromByteBuffer(
          buffer,
          order
      );
      return new CompressedVSizeColumnarMultiIntsSupplier(offsetSupplier, valueSupplier);
    }
    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  @VisibleForTesting
  public static CompressedVSizeColumnarMultiIntsSupplier fromIterable(
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
      for (int i = 0, size = next.size(); i < size; i++) {
        values.add(next.get(i));
      }
      offset += next.size();
    }
    offsetList.add(offset);
    int offsetMax = offset;
    CompressedVSizeColumnarIntsSupplier headerSupplier = CompressedVSizeColumnarIntsSupplier.fromList(
        offsetList,
        offsetMax,
        CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForValue(offsetMax),
        byteOrder,
        compression,
        closer
    );
    CompressedVSizeColumnarIntsSupplier valuesSupplier = CompressedVSizeColumnarIntsSupplier.fromList(
        values,
        maxValue,
        CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForValue(maxValue),
        byteOrder,
        compression,
        closer
    );
    return new CompressedVSizeColumnarMultiIntsSupplier(headerSupplier, valuesSupplier);
  }


  @Override
  public ColumnarMultiInts get()
  {
    return new CompressedVSizeColumnarMultiInts(offsetSupplier.get(), valueSupplier.get());
  }

  public static class CompressedVSizeColumnarMultiInts implements ColumnarMultiInts
  {
    private final ColumnarInts offsets;
    private final ColumnarInts values;

    private final SliceIndexedInts rowValues;

    CompressedVSizeColumnarMultiInts(ColumnarInts offsets, ColumnarInts values)
    {
      this.offsets = offsets;
      this.values = values;
      this.rowValues = new SliceIndexedInts(values);
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
      rowValues.setValues(offset, size);
      return rowValues;
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
