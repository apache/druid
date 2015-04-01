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
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedMultivalue;
import io.druid.segment.data.IndexedRTree;
import io.druid.segment.data.VSizeIndexed;
import io.druid.segment.data.VSizeIndexedInts;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

@JsonTypeName("stringDictionary")
public class LegacyDictionaryEncodedColumnPartSerde extends DictionaryEncodedColumnPartSerde
{
  private final boolean isSingleValued;

  private final GenericIndexed<String> dictionary;
  private final VSizeIndexedInts singleValuedColumn;
  private final VSizeIndexed multiValuedColumn;
  private final GenericIndexed<ImmutableBitmap> bitmaps;
  private final ImmutableRTree spatialIndex;

  private final long size;

  public LegacyDictionaryEncodedColumnPartSerde(
      BitmapSerdeFactory bitmapSerdeFactory,
      ByteOrder byteOrder
  )
  {
    super(bitmapSerdeFactory, byteOrder);
    this.size = -1;
    this.isSingleValued = false;
    this.dictionary = null;
    this.singleValuedColumn = null;
    this.multiValuedColumn = null;
    this.bitmaps = null;
    this.spatialIndex = null;
  }

  public LegacyDictionaryEncodedColumnPartSerde(
      GenericIndexed<String> dictionary,
      VSizeIndexedInts singleValuedColumn,
      VSizeIndexed multiValuedColumn,
      BitmapSerdeFactory bitmapSerdeFactory,
      GenericIndexed<ImmutableBitmap> bitmaps,
      ImmutableRTree spatialIndex,
      ByteOrder byteOrder
  )
  {
    super(bitmapSerdeFactory, byteOrder);
    this.isSingleValued = multiValuedColumn == null;
    this.dictionary = dictionary;
    this.singleValuedColumn = singleValuedColumn;
    this.multiValuedColumn = multiValuedColumn;
    this.bitmaps = bitmaps;
    this.spatialIndex = spatialIndex;

    long size = dictionary.getSerializedSize();
    if (singleValuedColumn != null && multiValuedColumn == null) {
      size += singleValuedColumn.getSerializedSize();
    } else if (singleValuedColumn == null && multiValuedColumn != null) {
      size += multiValuedColumn.getSerializedSize();
    } else {
      throw new IAE("Either singleValCol[%s] or multiValCol[%s] must be set", singleValuedColumn, multiValuedColumn);
    }
    size += bitmaps.getSerializedSize();
    if (spatialIndex != null) {
      size += spatialIndex.size() + Ints.BYTES;
    }

    this.size = size;
  }

  @Override
  public void write(WritableByteChannel channel) throws IOException
  {
    final DictionaryEncodedColumnPartSerde.TYPE type = isSingleValued ? DictionaryEncodedColumnPartSerde.TYPE.UNCOMPRESSED_SINGLE_VALUE
                                                                      : DictionaryEncodedColumnPartSerde.TYPE.UNCOMPRESSED_MULTI_VALUE;
    channel.write(ByteBuffer.wrap(new byte[]{type.asByte()}));

    if (dictionary != null) {
      dictionary.writeToChannel(channel);
    }

    if (isSingleValued()) {
      if (singleValuedColumn != null) {
        singleValuedColumn.writeToChannel(channel);
      }
    } else {
      if (multiValuedColumn != null) {
        multiValuedColumn.writeToChannel(channel);
      }
    }

    if (bitmaps != null) {
      bitmaps.writeToChannel(channel);
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
  public LegacyDictionaryEncodedColumnPartSerde read(ByteBuffer buffer, ColumnBuilder builder, ColumnConfig columnConfig)
  {
    final DictionaryEncodedColumnPartSerde.TYPE type = DictionaryEncodedColumnPartSerde.TYPE.fromByte(buffer.get());

    final GenericIndexed<String> dictionary = GenericIndexed.read(buffer, GenericIndexed.stringStrategy);

    final VSizeIndexedInts singleValuedColumn;
    final VSizeIndexed multiValuedColumn;

    builder.setType(ValueType.STRING);

    final boolean hasMultipleValues;
    switch (type) {
      case UNCOMPRESSED_SINGLE_VALUE:
        singleValuedColumn = VSizeIndexedInts.readFromByteBuffer(buffer);
        multiValuedColumn = null;
        hasMultipleValues = false;
        break;

      case UNCOMPRESSED_MULTI_VALUE:
        singleValuedColumn = null;
        multiValuedColumn = VSizeIndexed.readFromByteBuffer(buffer);
        hasMultipleValues = true;
        break;

      default:
        throw new IAE("Unsupported column type %s", type);
    }

    builder.setHasMultipleValues(hasMultipleValues)
           .setDictionaryEncodedColumn(
               new DictionaryEncodedColumnSupplier(
                   dictionary,
                   Suppliers.<IndexedInts>ofInstance(singleValuedColumn),
                   Suppliers.<IndexedMultivalue<IndexedInts>>ofInstance(multiValuedColumn),
                   columnConfig.columnCacheSizeBytes()
               )
           );

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

    ImmutableRTree spatialIndex = null;
    if (buffer.hasRemaining()) {
      spatialIndex = ByteBufferSerializer.read(
          buffer, new IndexedRTree.ImmutableRTreeObjectStrategy(bitmapSerdeFactory.getBitmapFactory())
      );
      builder.setSpatialIndex(new SpatialIndexColumnPartSupplier(spatialIndex));
    }

    return new LegacyDictionaryEncodedColumnPartSerde(
        dictionary,
        singleValuedColumn,
        multiValuedColumn,
        bitmapSerdeFactory,
        bitmaps,
        spatialIndex,
        byteOrder
    );
  }

  @Override
  public long numBytes()
  {
    return 1 + size;
  }

  public boolean isSingleValued()
  {
    return isSingleValued;
  }
}
