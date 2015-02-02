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
import com.metamx.collections.spatial.ImmutableRTree;
import com.metamx.common.IAE;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.ByteBufferSerializer;
import io.druid.segment.data.ConciseCompressedIndexedInts;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.IndexedRTree;
import io.druid.segment.data.VSizeIndexed;
import io.druid.segment.data.VSizeIndexedInts;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 */
public class DictionaryEncodedColumnPartSerde implements ColumnPartSerde
{
  @JsonCreator
  public static DictionaryEncodedColumnPartSerde createDeserializer(
      boolean singleValued
  )
  {
    return new DictionaryEncodedColumnPartSerde();
  }

  private final GenericIndexed<String> dictionary;
  private final VSizeIndexedInts singleValuedColumn;
  private final VSizeIndexed multiValuedColumn;
  private final GenericIndexed<ImmutableConciseSet> bitmaps;
  private final ImmutableRTree spatialIndex;

  private final long size;

  public DictionaryEncodedColumnPartSerde(
      GenericIndexed<String> dictionary,
      VSizeIndexedInts singleValCol,
      VSizeIndexed multiValCol,
      GenericIndexed<ImmutableConciseSet> bitmaps,
      ImmutableRTree spatialIndex
  )
  {
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

  private DictionaryEncodedColumnPartSerde()
  {
    dictionary = null;
    singleValuedColumn = null;
    multiValuedColumn = null;
    bitmaps = null;
    spatialIndex = null;
    size = 0;
  }

  @JsonProperty
  private boolean isSingleValued()
  {
    return singleValuedColumn != null;
  }

  @Override
  public long numBytes()
  {
    return 1 + size;
  }

  @Override
  public void write(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{(byte) (isSingleValued() ? 0x0 : 0x1)}));
    dictionary.writeToChannel(channel);
    if (isSingleValued()) {
      singleValuedColumn.writeToChannel(channel);
    } else {
      multiValuedColumn.writeToChannel(channel);
    }
    bitmaps.writeToChannel(channel);
    if (spatialIndex != null) {
      ByteBufferSerializer.writeToChannel(spatialIndex, IndexedRTree.objectStrategy, channel);
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
             .setDictionaryEncodedColumn(new DictionaryEncodedColumnSupplier(dictionary, singleValuedColumn, null, columnConfig.columnCacheSizeBytes()));
    } else {
      singleValuedColumn = null;
      multiValuedColumn = VSizeIndexed.readFromByteBuffer(buffer);
      builder.setHasMultipleValues(true)
             .setDictionaryEncodedColumn(new DictionaryEncodedColumnSupplier(dictionary, null, multiValuedColumn, columnConfig.columnCacheSizeBytes()));
    }

    GenericIndexed<ImmutableConciseSet> bitmaps = GenericIndexed.read(
        buffer, ConciseCompressedIndexedInts.objectStrategy
    );
    builder.setBitmapIndex(new BitmapIndexColumnPartSupplier(bitmaps, dictionary));

    ImmutableRTree spatialIndex = null;
    if (buffer.hasRemaining()) {
      spatialIndex = ByteBufferSerializer.read(
          buffer, IndexedRTree.objectStrategy
      );
      builder.setSpatialIndex(new SpatialIndexColumnPartSupplier(spatialIndex));
    }

    return new DictionaryEncodedColumnPartSerde(
        dictionary,
        singleValuedColumn,
        multiValuedColumn,
        bitmaps,
        spatialIndex
    );
  }
}
