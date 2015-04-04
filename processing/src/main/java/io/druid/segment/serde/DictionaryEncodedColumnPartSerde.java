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
import com.google.common.base.Suppliers;
import com.google.common.primitives.Ints;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.spatial.ImmutableRTree;
import com.metamx.common.IAE;
import com.metamx.common.Pair;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;


public class DictionaryEncodedColumnPartSerde implements ColumnPartSerde
{
  private static final int NO_FLAGS = 0;

  enum Feature {
    MULTI_VALUE;
    public boolean isSet(int flags) { return (getMask() & flags) != 0; }
    public int getMask() { return (1 << ordinal()); }
  }

  enum VERSION
  {
    UNCOMPRESSED_SINGLE_VALUE,  // 0x0
    UNCOMPRESSED_MULTI_VALUE,   // 0x1
    COMPRESSED;                 // 0x2

    public static VERSION fromByte(byte b) {
      final VERSION[] values = VERSION.values();
      Preconditions.checkArgument(b < values.length, "Unsupported dictionary column version[%s]", b);
      return values[b];
    }

    public byte asByte() {
      return (byte)this.ordinal();
    }
  }


  public static DictionaryEncodedColumnPartSerde createUncompressedSingleValue(
      GenericIndexed<String> dictionary,
      VSizeIndexedInts singleValuedColumn,
      BitmapSerdeFactory bitmapSerdeFactory,
      GenericIndexed<ImmutableBitmap> bitmaps,
      ImmutableRTree spatialIndex,
      ByteOrder byteOrder
  )
  {
    return new DictionaryEncodedColumnPartSerde(
        VERSION.UNCOMPRESSED_SINGLE_VALUE,
        NO_FLAGS,
        dictionary,
        singleValuedColumn.asWriteableSupplier(),
        null,
        bitmapSerdeFactory,
        bitmaps,
        spatialIndex,
        byteOrder
    );
  }

  public static DictionaryEncodedColumnPartSerde createCompressedSingleValue(
      GenericIndexed<String> dictionary,
      CompressedVSizeIntsIndexedSupplier singleValuedColumn,
      BitmapSerdeFactory bitmapSerdeFactory,
      GenericIndexed<ImmutableBitmap> bitmaps,
      ImmutableRTree spatialIndex,
      ByteOrder byteOrder
  )
  {
    return new DictionaryEncodedColumnPartSerde(
        VERSION.COMPRESSED,
        NO_FLAGS,
        dictionary,
        singleValuedColumn,
        null,
        bitmapSerdeFactory,
        bitmaps,
        spatialIndex,
        byteOrder
    );
  }

  public static DictionaryEncodedColumnPartSerde createUncompressedMultiValue(
      GenericIndexed<String> dictionary,
      VSizeIndexed multiValuedColumn,
      BitmapSerdeFactory bitmapSerdeFactory,
      GenericIndexed<ImmutableBitmap> bitmaps,
      ImmutableRTree spatialIndex,
      ByteOrder byteOrder
  )
  {
    return new DictionaryEncodedColumnPartSerde(
        VERSION.UNCOMPRESSED_MULTI_VALUE,
        Feature.MULTI_VALUE.getMask(),
        dictionary,
        null,
        multiValuedColumn,
        bitmapSerdeFactory,
        bitmaps,
        spatialIndex,
        byteOrder
    );
  }

  private final BitmapSerdeFactory bitmapSerdeFactory;
  private final ByteOrder byteOrder;

  private final GenericIndexed<String> dictionary;
  private final WritableSupplier<IndexedInts> singleValuedColumn;
  private final VSizeIndexed multiValuedColumn;
  private final GenericIndexed<ImmutableBitmap> bitmaps;
  private final ImmutableRTree spatialIndex;
  private final int flags;
  private final VERSION version;
  private final long size;


  @JsonCreator
  public DictionaryEncodedColumnPartSerde(
      @JsonProperty("bitmapSerdeFactory") BitmapSerdeFactory bitmapSerdeFactory,
      @JsonProperty("byteOrder") ByteOrder byteOrder
  )
  {
    this.bitmapSerdeFactory = bitmapSerdeFactory == null
                              ? new BitmapSerde.LegacyBitmapSerdeFactory()
                              : bitmapSerdeFactory;
    this.byteOrder = byteOrder;

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
      VSizeIndexed multiValuedColumn,
      BitmapSerdeFactory bitmapSerdeFactory,
      GenericIndexed<ImmutableBitmap> bitmaps,
      ImmutableRTree spatialIndex,
      ByteOrder byteOrder
  )
  {
    Preconditions.checkArgument(singleValuedColumn != null ^ multiValuedColumn != null,
                                "Exactly one of singleValCol[%s] or multiValCol[%s] must be set",
                                singleValuedColumn, multiValuedColumn);

    switch (version) {
      case UNCOMPRESSED_MULTI_VALUE:
        Preconditions.checkArgument(Feature.MULTI_VALUE.isSet(flags),
                                    "multi-value flag must be set for version[%s]", version.asByte());
        break;
      case UNCOMPRESSED_SINGLE_VALUE:
        Preconditions.checkArgument(!Feature.MULTI_VALUE.isSet(flags),
                                    "multi-value flag must not be set for version[%s]", version.asByte());
        break;
      case COMPRESSED:
        Preconditions.checkArgument(!Feature.MULTI_VALUE.isSet(flags),
                                    "compressed columns currently do not support multi-value columns", version.asByte());
        break;
      default:
        throw new IAE("Unsupported version[%s]", version);
    }

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
    if(version.compareTo(VERSION.COMPRESSED) >= 0) {
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

    if(rVersion.compareTo(VERSION.COMPRESSED) >= 0 ) {
      rFlags = buffer.getInt();
    } else {
      rFlags = rVersion.equals(VERSION.UNCOMPRESSED_MULTI_VALUE) ?
              Feature.MULTI_VALUE.getMask() :
              NO_FLAGS;
    }

    final boolean hasMultipleValues = Feature.MULTI_VALUE.isSet(rFlags);
    if(rVersion.equals(VERSION.COMPRESSED) && hasMultipleValues) {
      throw new IAE("Compressed dictionary encoded columns currently do not support multi-value columns");
    }

    final GenericIndexed<String> rDictionary = GenericIndexed.read(buffer, GenericIndexed.stringStrategy);
    builder.setType(ValueType.STRING);

    final WritableSupplier<IndexedInts> rSingleValuedColumn;
    final VSizeIndexed rMultiValuedColumn;

    if (rVersion.compareTo(VERSION.COMPRESSED) >= 0) {
      rSingleValuedColumn = CompressedVSizeIntsIndexedSupplier.fromByteBuffer(buffer, byteOrder);
      rMultiValuedColumn = null;
    } else {
      Pair<WritableSupplier<IndexedInts>, VSizeIndexed> cols = readUncompressed(rVersion, buffer);
      rSingleValuedColumn = cols.lhs;
      rMultiValuedColumn = cols.rhs;
    }

    builder.setHasMultipleValues(hasMultipleValues)
           .setDictionaryEncodedColumn(
               new DictionaryEncodedColumnSupplier(
                   rDictionary,
                   rSingleValuedColumn,
                   rMultiValuedColumn == null ? null : Suppliers.<IndexedMultivalue<IndexedInts>>ofInstance(rMultiValuedColumn),
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

  private static Pair<WritableSupplier<IndexedInts>, VSizeIndexed> readUncompressed(
      VERSION version,
      ByteBuffer buffer
  )
  {
    final WritableSupplier<IndexedInts> singleValuedColumn;
    final VSizeIndexed multiValuedColumn;

    switch (version) {
      case UNCOMPRESSED_SINGLE_VALUE:
        singleValuedColumn = VSizeIndexedInts.readFromByteBuffer(buffer).asWriteableSupplier();
        multiValuedColumn = null;
        break;

      case UNCOMPRESSED_MULTI_VALUE:
        singleValuedColumn = null;
        multiValuedColumn = VSizeIndexed.readFromByteBuffer(buffer);
        break;

      default:
        throw new IAE("Unsupported version[%s]", version);
    }

    return Pair.of(singleValuedColumn, multiValuedColumn);
  }

  @Override
  public long numBytes()
  {
    return 1 + // version
           (version.compareTo(VERSION.COMPRESSED) >= 0 ? Ints.BYTES : 0) + // flag if version >= compressed
           size; // size of everything else (dictionary, bitmaps, column, spatialIndex)
  }
}
