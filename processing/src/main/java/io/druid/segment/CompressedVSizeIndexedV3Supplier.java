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

import io.druid.java.util.common.IAE;
import io.druid.segment.data.CompressedIntsIndexedSupplier;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.CompressedVSizeIntsIndexedSupplier;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedMultivalue;
import io.druid.segment.data.WritableSupplier;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The format is mostly the same with CompressedVSizeIndexedSupplier(which has version 0x2, so we call it V2),
 * the only difference is V3's offsets is not VSize encoded, it's just compressed.
 * The reason we provide this is we can streams the data out in the binary format with CompressedVSizeIndexedV3Writer.
 * If we want to streams VSizeInts, we must know the max value in the value sets. It's easy to know the max id of
 * values(like dimension cardinality while encoding dimension), but difficult to known the max id of offsets.
 */
public class CompressedVSizeIndexedV3Supplier implements WritableSupplier<IndexedMultivalue<IndexedInts>>
{
  public static final byte VERSION = 0x3;

  private final CompressedIntsIndexedSupplier offsetSupplier;
  private final CompressedVSizeIntsIndexedSupplier valueSupplier;

  CompressedVSizeIndexedV3Supplier(
      CompressedIntsIndexedSupplier offsetSupplier,
      CompressedVSizeIntsIndexedSupplier valueSupplier
  )
  {
    this.offsetSupplier = offsetSupplier;
    this.valueSupplier = valueSupplier;
  }

  public static CompressedVSizeIndexedV3Supplier fromByteBuffer(ByteBuffer buffer, ByteOrder order)
  {
    byte versionFromBuffer = buffer.get();

    if (versionFromBuffer == VERSION) {
      CompressedIntsIndexedSupplier offsetSupplier = CompressedIntsIndexedSupplier.fromByteBuffer(
          buffer,
          order
      );
      CompressedVSizeIntsIndexedSupplier valueSupplier = CompressedVSizeIntsIndexedSupplier.fromByteBuffer(
          buffer,
          order
      );
      return new CompressedVSizeIndexedV3Supplier(offsetSupplier, valueSupplier);
    }
    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  // for test
  public static CompressedVSizeIndexedV3Supplier fromIterable(
      Iterable<IndexedInts> objectsIterable,
      int offsetChunkFactor,
      int maxValue,
      final ByteOrder byteOrder,
      CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    Iterator<IndexedInts> objects = objectsIterable.iterator();
    List<Integer> offsetList = new ArrayList<>();
    List<Integer> values = new ArrayList<>();

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
    CompressedIntsIndexedSupplier headerSupplier = CompressedIntsIndexedSupplier.fromList(
        offsetList,
        offsetChunkFactor,
        byteOrder,
        compression
    );
    CompressedVSizeIntsIndexedSupplier valuesSupplier = CompressedVSizeIntsIndexedSupplier.fromList(
        values,
        maxValue,
        CompressedVSizeIntsIndexedSupplier.maxIntsInBufferForValue(maxValue),
        byteOrder,
        compression
    );
    return new CompressedVSizeIndexedV3Supplier(headerSupplier, valuesSupplier);
  }

  @Override
  public long getSerializedSize()
  {
    return 1 + offsetSupplier.getSerializedSize() + valueSupplier.getSerializedSize();
  }

  @Override
  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{VERSION}));
    offsetSupplier.writeToChannel(channel);
    valueSupplier.writeToChannel(channel);
  }

  @Override
  public IndexedMultivalue<IndexedInts> get()
  {
    return new CompressedVSizeIndexedSupplier.CompressedVSizeIndexed(offsetSupplier.get(), valueSupplier.get());
  }

}
