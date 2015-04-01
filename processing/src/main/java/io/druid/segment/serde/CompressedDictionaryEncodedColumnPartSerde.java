/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.segment.serde;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Suppliers;
import com.google.common.primitives.Ints;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.spatial.ImmutableRTree;
import com.metamx.common.IAE;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferSerializer;
import io.druid.segment.data.CompressedIntsIndexedSupplier;
import io.druid.segment.data.CompressedVSizeIntsIndexedSupplier;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedMultivalue;
import io.druid.segment.data.IndexedRTree;
import io.druid.segment.data.VSizeIndexed;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

@JsonTypeName("stringDictionary")
public class CompressedDictionaryEncodedColumnPartSerde extends DictionaryEncodedColumnPartSerde
{
  private final GenericIndexed<String> dictionary;
  private final CompressedVSizeIntsIndexedSupplier singleValuedColumn;
  private final VSizeIndexed multiValuedColumn;
  private final GenericIndexed<ImmutableBitmap> bitmaps;
  private final ImmutableRTree spatialIndex;
  private final long size;
  private final int flags;


  enum Feature {
    MULTI_VALUE;
    public boolean isSet(int flags) { return (getMask() & flags) != 0; }
    public int getMask() { return (1 << ordinal()); }
  }


  public CompressedDictionaryEncodedColumnPartSerde(BitmapSerdeFactory bitmapSerdeFactory, ByteOrder byteOrder)
  {
    super(bitmapSerdeFactory, byteOrder);

    this.size = -1;
    this.dictionary = null;
    this.singleValuedColumn = null;
    this.multiValuedColumn = null;
    this.bitmaps = null;
    this.spatialIndex = null;
    this.flags = 0;
  }

  public CompressedDictionaryEncodedColumnPartSerde(
      GenericIndexed<String> dictionary,
      CompressedVSizeIntsIndexedSupplier singleValuedColumn,
      VSizeIndexed multiValuedColumn,
      BitmapSerdeFactory bitmapSerdeFactory,
      GenericIndexed<ImmutableBitmap> bitmaps,
      ImmutableRTree spatialIndex,
      ByteOrder byteOrder

  )
  {
    super(bitmapSerdeFactory, byteOrder);

    this.flags = multiValuedColumn != null ? Feature.MULTI_VALUE.getMask() : 0;;
    this.dictionary = dictionary;
    this.singleValuedColumn = singleValuedColumn;
    this.multiValuedColumn = multiValuedColumn;
    this.bitmaps = bitmaps;
    this.spatialIndex = spatialIndex;

    long size = bitmaps.getSerializedSize();

    if (Feature.MULTI_VALUE.isSet(flags)) {
      size += multiValuedColumn.getSerializedSize();
    } else {
      size += singleValuedColumn.getSerializedSize();
    }
    size += dictionary.getSerializedSize();
    if (spatialIndex != null) {
      size += spatialIndex.size() + Ints.BYTES;
    }
    this.size = size;
  }

  @Override
  public long numBytes()
  {
    return 1 + Ints.BYTES + size;
  }

  @Override
  public void write(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{DictionaryEncodedColumnPartSerde.TYPE.COMPRESSED.asByte()}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(flags)));

    dictionary.writeToChannel(channel);
    bitmaps.writeToChannel(channel);

    if (Feature.MULTI_VALUE.isSet(flags)) {
      multiValuedColumn.writeToChannel(channel);
    }
    else {
      singleValuedColumn.writeToChannel(channel);
    }

    if (spatialIndex != null) {
      ByteBufferSerializer.writeToChannel(
          spatialIndex,
          new IndexedRTree.ImmutableRTreeObjectStrategy(bitmapSerdeFactory.getBitmapFactory()),
          channel
      );
    }
  }

  @Override
  public CompressedDictionaryEncodedColumnPartSerde read(
      ByteBuffer buffer, ColumnBuilder builder, ColumnConfig columnConfig
  )
  {
    final byte version = buffer.get();
    if(version != DictionaryEncodedColumnPartSerde.TYPE.COMPRESSED.asByte()) {
      throw new IAE("Unknown version[%s]", version);
    }
    final int flags = buffer.getInt();

    builder.setType(ValueType.STRING);

    final GenericIndexed<String> dictionary = GenericIndexed.read(buffer, GenericIndexed.stringStrategy);

    GenericIndexed<ImmutableBitmap> bitmaps = GenericIndexed.read(
        buffer, bitmapSerdeFactory.getObjectStrategy()
    );
    builder.setBitmapIndex(
        new BitmapIndexColumnPartSupplier(
            bitmapSerdeFactory.getBitmapFactory(),
            bitmaps,
            dictionary
        )
    );

    final CompressedVSizeIntsIndexedSupplier singleValuedColumn;
    final VSizeIndexed multiValuedColumn;

    final boolean isMultiValued = Feature.MULTI_VALUE.isSet(flags);
    if(isMultiValued) {
      multiValuedColumn = VSizeIndexed.readFromByteBuffer(buffer);
      singleValuedColumn = null;
    } else {
      singleValuedColumn = CompressedVSizeIntsIndexedSupplier.fromByteBuffer(buffer, byteOrder);
      multiValuedColumn = null;
    }
    builder.setHasMultipleValues(isMultiValued)
           .setDictionaryEncodedColumn(
               new DictionaryEncodedColumnSupplier(
                   dictionary,
                   singleValuedColumn,
                   Suppliers.<IndexedMultivalue<IndexedInts>>ofInstance(multiValuedColumn),
                   columnConfig.columnCacheSizeBytes()
               )
           );

    ImmutableRTree spatialIndex = null;
    if (buffer.hasRemaining()) {
      spatialIndex = ByteBufferSerializer.read(
          buffer, new IndexedRTree.ImmutableRTreeObjectStrategy(bitmapSerdeFactory.getBitmapFactory())
      );
      builder.setSpatialIndex(new SpatialIndexColumnPartSupplier(spatialIndex));
    }

    return new CompressedDictionaryEncodedColumnPartSerde(
        dictionary,
        singleValuedColumn,
        multiValuedColumn,
        bitmapSerdeFactory,
        bitmaps,
        spatialIndex,
        byteOrder
    );
  }
}
