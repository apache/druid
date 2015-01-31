/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Ints;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.spatial.ImmutableRTree;
import com.metamx.common.IAE;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.BitmapSerde;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferSerializer;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.IndexedRTree;
import io.druid.segment.data.VSizeIndexed;
import io.druid.segment.data.VSizeIndexedInts;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 */
public class DictionaryEncodedColumnPartSerde implements ColumnPartSerde
{
  private final boolean isSingleValued;
  private final BitmapSerdeFactory bitmapSerdeFactory;

  private final GenericIndexed<String> dictionary;
  private final VSizeIndexedInts singleValuedColumn;
  private final VSizeIndexed multiValuedColumn;
  private final GenericIndexed<ImmutableBitmap> bitmaps;
  private final ImmutableRTree spatialIndex;

  private final long size;

  public DictionaryEncodedColumnPartSerde(
      GenericIndexed<String> dictionary,
      VSizeIndexedInts singleValCol,
      VSizeIndexed multiValCol,
      BitmapSerdeFactory bitmapSerdeFactory,
      GenericIndexed<ImmutableBitmap> bitmaps,
      ImmutableRTree spatialIndex
  )
  {
    this.isSingleValued = multiValCol == null;
    this.bitmapSerdeFactory = bitmapSerdeFactory;

    this.dictionary = dictionary;
    this.singleValuedColumn = singleValCol;
    this.multiValuedColumn = multiValCol;
    this.bitmaps = bitmaps;
    this.spatialIndex = spatialIndex;

    long size = dictionary.getSerializedSize();
    if (singleValCol != null && multiValCol == null) {
      size += singleValCol.getSerializedSize();
    } else if (singleValCol == null && multiValCol != null) {
      size += multiValCol.getSerializedSize();
    } else {
      throw new IAE("Either singleValCol[%s] or multiValCol[%s] must be set", singleValCol, multiValCol);
    }
    size += bitmaps.getSerializedSize();
    if (spatialIndex != null) {
      size += spatialIndex.size() + Ints.BYTES;
    }

    this.size = size;
  }

  @JsonCreator
  public DictionaryEncodedColumnPartSerde(
      @JsonProperty("isSingleValued") boolean isSingleValued,
      @JsonProperty("bitmapSerdeFactory") BitmapSerdeFactory bitmapSerdeFactory
  )
  {
    this.isSingleValued = isSingleValued;
    this.bitmapSerdeFactory = bitmapSerdeFactory == null
                              ? new BitmapSerde.LegacyBitmapSerdeFactory()
                              : bitmapSerdeFactory;

    this.dictionary = null;
    this.singleValuedColumn = null;
    this.multiValuedColumn = null;
    this.bitmaps = null;
    this.spatialIndex = null;
    this.size = 0;
  }

  @JsonProperty
  private boolean isSingleValued()
  {
    return isSingleValued;
  }

  @JsonProperty
  public BitmapSerdeFactory getBitmapSerdeFactory()
  {
    return bitmapSerdeFactory;
  }

  @Override
  public long numBytes()
  {
    return 1 + size;
  }

  @Override
  public void write(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{(byte) (isSingleValued ? 0x0 : 0x1)}));

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
  public ColumnPartSerde read(ByteBuffer buffer, ColumnBuilder builder, ColumnConfig columnConfig)
  {
    final boolean isSingleValued = buffer.get() == 0x0;
    final GenericIndexed<String> dictionary = GenericIndexed.read(buffer, GenericIndexed.stringStrategy);
    final VSizeIndexedInts singleValuedColumn;
    final VSizeIndexed multiValuedColumn;

    builder.setType(ValueType.STRING);

    if (isSingleValued) {
      singleValuedColumn = VSizeIndexedInts.readFromByteBuffer(buffer);
      multiValuedColumn = null;
      builder.setHasMultipleValues(false)
             .setDictionaryEncodedColumn(
                 new DictionaryEncodedColumnSupplier(
                     dictionary,
                     singleValuedColumn,
                     null,
                     columnConfig.columnCacheSizeBytes()
                 )
             );
    } else {
      singleValuedColumn = null;
      multiValuedColumn = VSizeIndexed.readFromByteBuffer(buffer);
      builder.setHasMultipleValues(true)
             .setDictionaryEncodedColumn(
                 new DictionaryEncodedColumnSupplier(
                     dictionary,
                     null,
                     multiValuedColumn,
                     columnConfig.columnCacheSizeBytes()
                 )
             );
    }

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

    return new DictionaryEncodedColumnPartSerde(
        dictionary,
        singleValuedColumn,
        multiValuedColumn,
        bitmapSerdeFactory,
        bitmaps,
        spatialIndex
    );
  }
}
