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
import com.google.common.primitives.Ints;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.spatial.ImmutableRTree;
import com.metamx.common.IAE;
import io.druid.segment.CompressedVSizeIndexedSupplier;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.BitmapSerde;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferSerializer;
import io.druid.segment.data.CompressedVSizeIntsIndexedSupplier;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedMultivalue;
import io.druid.segment.data.IndexedRTree;
import io.druid.segment.data.VSizeIndexed;
import io.druid.segment.data.VSizeIndexedInts;
import io.druid.segment.data.WritableSupplier;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;


public class DictionaryEncodedColumnPartSerde implements ColumnPartSerde
{
  private static final int NO_FLAGS = 0;

  enum Feature
  {
    MULTI_VALUE;

    public boolean isSet(int flags) { return (getMask() & flags) != 0; }

    public int getMask() { return (1 << ordinal()); }
  }

  enum VERSION
  {
    UNCOMPRESSED_SINGLE_VALUE,  // 0x0
    UNCOMPRESSED_MULTI_VALUE,   // 0x1
    COMPRESSED;                 // 0x2


    public static VERSION fromByte(byte b)
    {
      final VERSION[] values = VERSION.values();
      Preconditions.checkArgument(b < values.length, "Unsupported dictionary column version[%s]", b);
      return values[b];
    }

    public byte asByte()
    {
      return (byte) this.ordinal();
    }
  }

  public static class Builder
  {
    private VERSION version = null;
    private int flags = NO_FLAGS;
    private GenericIndexed<String> dictionary = null;
    private WritableSupplier<IndexedInts> singleValuedColumn = null;
    private WritableSupplier<IndexedMultivalue<IndexedInts>> multiValuedColumn = null;
    private BitmapSerdeFactory bitmapSerdeFactory = null;
    private GenericIndexed<ImmutableBitmap> bitmaps = null;
    private ImmutableRTree spatialIndex = null;
    private ByteOrder byteOrder = null;

    private Builder()
    {
    }

    public Builder withDictionary(GenericIndexed<String> dictionary)
    {
      this.dictionary = dictionary;
      return this;
    }

    public Builder withBitmapSerdeFactory(BitmapSerdeFactory bitmapSerdeFactory)
    {
      this.bitmapSerdeFactory = bitmapSerdeFactory;
      return this;
    }

    public Builder withBitmaps(GenericIndexed<ImmutableBitmap> bitmaps)
    {
      this.bitmaps = bitmaps;
      return this;
    }

    public Builder withSpatialIndex(ImmutableRTree spatialIndex)
    {
      this.spatialIndex = spatialIndex;
      return this;
    }

    public Builder withByteOrder(ByteOrder byteOrder)
    {
      this.byteOrder = byteOrder;
      return this;
    }

    public Builder withSingleValuedColumn(VSizeIndexedInts singleValuedColumn)
    {
      Preconditions.checkState(multiValuedColumn == null, "Cannot set both singleValuedColumn and multiValuedColumn");
      this.version = VERSION.UNCOMPRESSED_SINGLE_VALUE;
      this.singleValuedColumn = singleValuedColumn.asWritableSupplier();
      return this;
    }

    public Builder withSingleValuedColumn(CompressedVSizeIntsIndexedSupplier singleValuedColumn)
    {
      Preconditions.checkState(multiValuedColumn == null, "Cannot set both singleValuedColumn and multiValuedColumn");
      this.version = VERSION.COMPRESSED;
      this.singleValuedColumn = singleValuedColumn;
      return this;
    }

    public Builder withMultiValuedColumn(VSizeIndexed multiValuedColumn)
    {
      Preconditions.checkState(singleValuedColumn == null, "Cannot set both multiValuedColumn and singleValuedColumn");
      this.version = VERSION.UNCOMPRESSED_MULTI_VALUE;
      this.flags |= Feature.MULTI_VALUE.getMask();
      this.multiValuedColumn = multiValuedColumn.asWritableSupplier();
      return this;
    }

    public Builder withMultiValuedColumn(CompressedVSizeIndexedSupplier multiValuedColumn)
    {
      Preconditions.checkState(singleValuedColumn == null, "Cannot set both singleValuedColumn and multiValuedColumn");
      this.version = VERSION.COMPRESSED;
      this.flags |= Feature.MULTI_VALUE.getMask();
      this.multiValuedColumn = multiValuedColumn;
      return this;
    }

    public DictionaryEncodedColumnPartSerde build()
    {
      Preconditions.checkArgument(
          singleValuedColumn != null ^ multiValuedColumn != null,
          "Exactly one of singleValCol[%s] or multiValCol[%s] must be set",
          singleValuedColumn, multiValuedColumn
      );

      return new DictionaryEncodedColumnPartSerde(
          version,
          flags,
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

  public static Builder builder()
  {
    return new Builder();
  }

  private final BitmapSerdeFactory bitmapSerdeFactory;
  private final ByteOrder byteOrder;

  private final GenericIndexed<String> dictionary;
  private final WritableSupplier<IndexedInts> singleValuedColumn;
  private final WritableSupplier<IndexedMultivalue<IndexedInts>> multiValuedColumn;
  private final GenericIndexed<ImmutableBitmap> bitmaps;
  private final ImmutableRTree spatialIndex;
  private final int flags;
  private final VERSION version;
  private final long size;


  @JsonCreator
  public DictionaryEncodedColumnPartSerde(
      @Nullable @JsonProperty("bitmapSerdeFactory") BitmapSerdeFactory bitmapSerdeFactory,
      @NotNull @JsonProperty("byteOrder") ByteOrder byteOrder
  )
  {
    this.bitmapSerdeFactory = bitmapSerdeFactory == null
                              ? new BitmapSerde.LegacyBitmapSerdeFactory()
                              : bitmapSerdeFactory;
    this.byteOrder = byteOrder;

    // dummy values
    this.dictionary = null;
    this.singleValuedColumn = null;
    this.multiValuedColumn = null;
    this.bitmaps = null;
    this.spatialIndex = null;
    this.size = -1;
    this.flags = 0;
    this.version = VERSION.COMPRESSED;
  }

  private DictionaryEncodedColumnPartSerde(
      VERSION version,
      int flags,
      GenericIndexed<String> dictionary,
      WritableSupplier<IndexedInts> singleValuedColumn,
      WritableSupplier<IndexedMultivalue<IndexedInts>> multiValuedColumn,
      BitmapSerdeFactory bitmapSerdeFactory,
      GenericIndexed<ImmutableBitmap> bitmaps,
      ImmutableRTree spatialIndex,
      ByteOrder byteOrder
  )
  {
    Preconditions.checkArgument(version.compareTo(VERSION.COMPRESSED) <= 0, "Unsupported version[%s]", version);

    this.bitmapSerdeFactory = bitmapSerdeFactory;
    this.byteOrder = byteOrder;

    this.version = version;
    this.flags = flags;

    this.dictionary = dictionary;
    this.singleValuedColumn = singleValuedColumn;
    this.multiValuedColumn = multiValuedColumn;
    this.bitmaps = bitmaps;
    this.spatialIndex = spatialIndex;

    long size = dictionary.getSerializedSize();

    if (Feature.MULTI_VALUE.isSet(flags)) {
      size += multiValuedColumn.getSerializedSize();
    } else {
      size += singleValuedColumn.getSerializedSize();
    }

    size += bitmaps.getSerializedSize();
    if (spatialIndex != null) {
      size += spatialIndex.size() + Ints.BYTES;
    }

    this.size = size;
  }

  @JsonProperty
  public BitmapSerdeFactory getBitmapSerdeFactory()
  {
    return bitmapSerdeFactory;
  }

  @JsonProperty
  public ByteOrder getByteOrder()
  {
    return byteOrder;
  }

  @Override
  public void write(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{version.asByte()}));
    if (version.compareTo(VERSION.COMPRESSED) >= 0) {
      channel.write(ByteBuffer.wrap(Ints.toByteArray(flags)));
    }

    if (dictionary != null) {
      dictionary.writeToChannel(channel);
    }

    if (Feature.MULTI_VALUE.isSet(flags)) {
      if (multiValuedColumn != null) {
        multiValuedColumn.writeToChannel(channel);
      }
    } else {
      if (singleValuedColumn != null) {
        singleValuedColumn.writeToChannel(channel);
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
  public ColumnPartSerde read(
      ByteBuffer buffer, ColumnBuilder builder, ColumnConfig columnConfig
  )
  {
    final VERSION rVersion = VERSION.fromByte(buffer.get());
    final int rFlags;

    if (rVersion.compareTo(VERSION.COMPRESSED) >= 0) {
      rFlags = buffer.getInt();
    } else {
      rFlags = rVersion.equals(VERSION.UNCOMPRESSED_MULTI_VALUE) ?
               Feature.MULTI_VALUE.getMask() :
               NO_FLAGS;
    }

    final boolean hasMultipleValues = Feature.MULTI_VALUE.isSet(rFlags);

    final GenericIndexed<String> rDictionary = GenericIndexed.read(buffer, GenericIndexed.STRING_STRATEGY);
    builder.setType(ValueType.STRING);

    final WritableSupplier<IndexedInts> rSingleValuedColumn;
    final WritableSupplier<IndexedMultivalue<IndexedInts>> rMultiValuedColumn;

    if (hasMultipleValues) {
      rMultiValuedColumn = readMultiValuedColum(rVersion, buffer);
      rSingleValuedColumn = null;
    } else {
      rSingleValuedColumn = readSingleValuedColumn(rVersion, buffer);
      rMultiValuedColumn = null;
    }

    builder.setHasMultipleValues(hasMultipleValues)
           .setDictionaryEncodedColumn(
               new DictionaryEncodedColumnSupplier(
                   rDictionary,
                   rSingleValuedColumn,
                   rMultiValuedColumn,
                   columnConfig.columnCacheSizeBytes()
               )
           );

    GenericIndexed<ImmutableBitmap> rBitmaps = GenericIndexed.read(
        buffer, bitmapSerdeFactory.getObjectStrategy()
    );
    builder.setBitmapIndex(
        new BitmapIndexColumnPartSupplier(
            bitmapSerdeFactory.getBitmapFactory(),
            rBitmaps,
            rDictionary
        )
    );

    ImmutableRTree rSpatialIndex = null;
    if (buffer.hasRemaining()) {
      rSpatialIndex = ByteBufferSerializer.read(
          buffer, new IndexedRTree.ImmutableRTreeObjectStrategy(bitmapSerdeFactory.getBitmapFactory())
      );
      builder.setSpatialIndex(new SpatialIndexColumnPartSupplier(rSpatialIndex));
    }

    return new DictionaryEncodedColumnPartSerde(
        rVersion,
        rFlags,
        rDictionary,
        rSingleValuedColumn,
        rMultiValuedColumn,
        bitmapSerdeFactory,
        rBitmaps,
        rSpatialIndex,
        byteOrder
    );
  }

  private WritableSupplier<IndexedInts> readSingleValuedColumn(VERSION version, ByteBuffer buffer)
  {
    switch (version) {
      case UNCOMPRESSED_SINGLE_VALUE:
        return VSizeIndexedInts.readFromByteBuffer(buffer).asWritableSupplier();
      case COMPRESSED:
        return CompressedVSizeIntsIndexedSupplier.fromByteBuffer(buffer, byteOrder);
    }
    throw new IAE("Unsupported single-value version[%s]", version);
  }

  private WritableSupplier<IndexedMultivalue<IndexedInts>> readMultiValuedColum(VERSION version, ByteBuffer buffer)
  {
    switch (version) {
      case UNCOMPRESSED_MULTI_VALUE:
        return VSizeIndexed.readFromByteBuffer(buffer).asWritableSupplier();
      case COMPRESSED:
        return CompressedVSizeIndexedSupplier.fromByteBuffer(buffer, byteOrder);
    }
    throw new IAE("Unsupported multi-value version[%s]", version);
  }

  @Override
  public long numBytes()
  {
    return 1 + // version
           (version.compareTo(VERSION.COMPRESSED) >= 0 ? Ints.BYTES : 0) + // flag if version >= compressed
           size; // size of everything else (dictionary, bitmaps, column, spatialIndex)
  }
}
