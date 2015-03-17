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
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
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
import io.druid.segment.data.CompressedIntsIndexedSupplier;
import io.druid.segment.data.CompressedLongBufferObjectStrategy;
import io.druid.segment.data.CompressedLongsIndexedSupplier;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedLongs;
import io.druid.segment.data.IndexedMultivalueInts;
import io.druid.segment.data.IndexedRTree;
import io.druid.segment.data.VSizeIndexed;
import io.druid.segment.data.VSizeIndexedInts;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 */
public class DictionaryEncodedColumnPartSerde implements ColumnPartSerde
{
  public static enum TYPE {
    SINGLE_VALUE, // 0x0
    MULTI_VALUE;  // 0x1

    public static TYPE fromByte(byte b) {
      final TYPE[] values = TYPE.values();
      Preconditions.checkArgument(b < values.length, "Unknown dictionary column type 0x%X", b);
      return values[b];
    }

    public byte asByte() {
      return (byte)this.ordinal();
    }
  }
  private final boolean isSingleValued;
  private final BitmapSerdeFactory bitmapSerdeFactory;

  private final GenericIndexed<String> dictionary;
  private final CompressedIntsIndexedSupplier singleValuedColumn;
  private final VSizeIndexed multiValuedColumn;
  private final GenericIndexed<ImmutableBitmap> bitmaps;
  private final ImmutableRTree spatialIndex;
  private final ByteOrder byteOrder;

  private final long size;

  public DictionaryEncodedColumnPartSerde(
      GenericIndexed<String> dictionary,
      CompressedIntsIndexedSupplier singleValCol,
      VSizeIndexed multiValCol,
      BitmapSerdeFactory bitmapSerdeFactory,
      GenericIndexed<ImmutableBitmap> bitmaps,
      ImmutableRTree spatialIndex,
      ByteOrder byteOrder
  )
  {
    this.isSingleValued = multiValCol == null;
    this.bitmapSerdeFactory = bitmapSerdeFactory;

    this.dictionary = dictionary;
    this.singleValuedColumn = singleValCol;
    this.multiValuedColumn = multiValCol;
    this.bitmaps = bitmaps;
    this.spatialIndex = spatialIndex;
    this.byteOrder = byteOrder;

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
      @JsonProperty("bitmapSerdeFactory") BitmapSerdeFactory bitmapSerdeFactory,
      @JsonProperty("byteOrder") ByteOrder byteOrder
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
    this.byteOrder = byteOrder;
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

  @JsonProperty
  public ByteOrder getByteOrder()
  {
    return byteOrder;
  }

  @Override
  public void write(WritableByteChannel channel) throws IOException
  {
    final TYPE type = isSingleValued ? TYPE.SINGLE_VALUE : TYPE.MULTI_VALUE;
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
  public ColumnPartSerde read(ByteBuffer buffer, ColumnBuilder builder, ColumnConfig columnConfig)
  {
    final TYPE type = TYPE.fromByte(buffer.get());
    final GenericIndexed<String> dictionary = GenericIndexed.read(buffer, GenericIndexed.stringStrategy);
    final CompressedIntsIndexedSupplier singleValuedColumn;
    final VSizeIndexed multiValuedColumn;

    builder.setType(ValueType.STRING);

    switch (type) {
      case SINGLE_VALUE:
        singleValuedColumn = //VSizeIndexedInts.readFromByteBuffer(buffer);
            CompressedIntsIndexedSupplier.fromByteBuffer(buffer, byteOrder);

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
        break;

      case MULTI_VALUE:
        singleValuedColumn = null;
        multiValuedColumn = VSizeIndexed.readFromByteBuffer(buffer);
        builder.setHasMultipleValues(true)
               .setDictionaryEncodedColumn(
                   new DictionaryEncodedColumnSupplier(
                       dictionary,
                       null,
                       Suppliers.<IndexedMultivalueInts<IndexedInts>>ofInstance(multiValuedColumn),
                       columnConfig.columnCacheSizeBytes()
                   )
               );
        break;

      default:
        throw new IAE("Unsupported column type %s", type);
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
        spatialIndex,
        byteOrder
    );
  }
}
