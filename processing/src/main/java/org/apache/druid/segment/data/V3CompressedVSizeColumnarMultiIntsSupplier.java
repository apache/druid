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

package org.apache.druid.segment.data;

import com.google.common.annotations.VisibleForTesting;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;

/**
 * The format is mostly the same with {@link CompressedVSizeColumnarMultiIntsSupplier} (which has version 0x2, so we
 * call it V2), the only difference is V3's offsets is not VSize encoded, it's just compressed.
 * The reason we provide this is we can streams the data out in the binary format with {@link
 * V3CompressedVSizeColumnarMultiIntsSerializer}.
 * If we want to streams VSizeInts, we must know the max value in the value sets. It's easy to know the max id of
 * values(like dimension cardinality while encoding dimension), but difficult to known the max id of offsets.
 */
public class V3CompressedVSizeColumnarMultiIntsSupplier implements WritableSupplier<ColumnarMultiInts>
{
  public static final byte VERSION = 0x3;

  private final CompressedColumnarIntsSupplier offsetSupplier;
  private final CompressedVSizeColumnarIntsSupplier valueSupplier;

  private V3CompressedVSizeColumnarMultiIntsSupplier(
      CompressedColumnarIntsSupplier offsetSupplier,
      CompressedVSizeColumnarIntsSupplier valueSupplier
  )
  {
    this.offsetSupplier = offsetSupplier;
    this.valueSupplier = valueSupplier;
  }

  public static V3CompressedVSizeColumnarMultiIntsSupplier fromByteBuffer(ByteBuffer buffer, ByteOrder order)
  {
    byte versionFromBuffer = buffer.get();

    if (versionFromBuffer == VERSION) {
      CompressedColumnarIntsSupplier offsetSupplier = CompressedColumnarIntsSupplier.fromByteBuffer(
          buffer,
          order
      );
      CompressedVSizeColumnarIntsSupplier valueSupplier = CompressedVSizeColumnarIntsSupplier.fromByteBuffer(
          buffer,
          order
      );
      return new V3CompressedVSizeColumnarMultiIntsSupplier(offsetSupplier, valueSupplier);
    }
    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  @VisibleForTesting
  public static V3CompressedVSizeColumnarMultiIntsSupplier fromIterable(
      final Iterable<IndexedInts> objectsIterable,
      final int offsetChunkFactor,
      final int maxValue,
      final ByteOrder byteOrder,
      final CompressionStrategy compression,
      final Closer closer
  )
  {
    Iterator<IndexedInts> objects = objectsIterable.iterator();
    IntArrayList offsetList = new IntArrayList();
    IntArrayList values = new IntArrayList();

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
    CompressedColumnarIntsSupplier headerSupplier = CompressedColumnarIntsSupplier.fromList(
        offsetList,
        offsetChunkFactor,
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
    return new V3CompressedVSizeColumnarMultiIntsSupplier(headerSupplier, valuesSupplier);
  }

  @Override
  public long getSerializedSize()
  {
    return 1 + offsetSupplier.getSerializedSize() + valueSupplier.getSerializedSize();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{VERSION}));
    offsetSupplier.writeTo(channel, smoosher);
    valueSupplier.writeTo(channel, smoosher);
  }

  @Override
  public ColumnarMultiInts get()
  {
    return new CompressedVSizeColumnarMultiIntsSupplier.CompressedVSizeColumnarMultiInts(
        offsetSupplier.get(),
        valueSupplier.get()
    );
  }

}
