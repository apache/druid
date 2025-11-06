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

package org.apache.druid.segment.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import com.google.common.primitives.Ints;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.spatial.ImmutableRTree;
import org.apache.druid.error.DruidException;
import org.apache.druid.io.Channels;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.SelectableColumn;
import org.apache.druid.segment.column.StringEncodingStrategies;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.BitmapSerde;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.ByteBufferWriter;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.ColumnarIntsSerializer;
import org.apache.druid.segment.data.ColumnarMultiInts;
import org.apache.druid.segment.data.CompressedVSizeColumnarIntsSupplier;
import org.apache.druid.segment.data.CompressedVSizeColumnarMultiIntsSupplier;
import org.apache.druid.segment.data.DictionaryWriter;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.data.ImmutableRTreeObjectStrategy;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.V3CompressedVSizeColumnarMultiIntsSupplier;
import org.apache.druid.segment.data.VSizeColumnarInts;
import org.apache.druid.segment.data.VSizeColumnarMultiInts;
import org.apache.druid.segment.data.WritableSupplier;
import org.apache.druid.segment.file.SegmentFileBuilder;
import org.apache.druid.segment.file.SegmentFileMapper;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class DictionaryEncodedColumnPartSerde implements ColumnPartSerde
{
  public static final int STARTING_FLAGS = DictionarySerdeHelper.Feature.NO_BITMAP_INDEX.getMask();

  @JsonCreator
  public static DictionaryEncodedColumnPartSerde createDeserializer(
      @JsonProperty("bitmapSerdeFactory") @Nullable BitmapSerdeFactory bitmapSerdeFactory,
      @NotNull @JsonProperty("byteOrder") ByteOrder byteOrder
  )
  {
    return new DictionaryEncodedColumnPartSerde(
        byteOrder,
        bitmapSerdeFactory != null ? bitmapSerdeFactory : new BitmapSerde.LegacyBitmapSerdeFactory(),
        null
    );
  }

  private final ByteOrder byteOrder;
  private final BitmapSerdeFactory bitmapSerdeFactory;
  private final Serializer serializer;

  private DictionaryEncodedColumnPartSerde(
      ByteOrder byteOrder,
      BitmapSerdeFactory bitmapSerdeFactory,
      @Nullable Serializer serializer
  )
  {
    this.byteOrder = byteOrder;
    this.bitmapSerdeFactory = bitmapSerdeFactory;
    this.serializer = serializer;
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

  public static SerializerBuilder serializerBuilder(boolean hasMultiValue, boolean compressed, boolean noBitmapIndex)
  {
    return new SerializerBuilder(hasMultiValue, compressed, noBitmapIndex);
  }

  public static class SerializerBuilder
  {
    private final int flags;
    private final DictionarySerdeHelper.VERSION version;

    @Nullable
    private DictionaryWriter<String> dictionaryWriter = null;
    @Nullable
    private ColumnarIntsSerializer valueWriter = null;
    @Nullable
    private BitmapSerdeFactory bitmapSerdeFactory = null;
    @Nullable
    private GenericIndexedWriter<ImmutableBitmap> bitmapIndexWriter = null;
    @Nullable
    private ByteBufferWriter<ImmutableRTree> spatialIndexWriter = null;
    @Nullable
    private ByteOrder byteOrder = null;

    SerializerBuilder(boolean hasMultiValue, boolean compressed, boolean noBitmapIndex)
    {
      int flagBuilder = 0;
      if (noBitmapIndex) {
        flagBuilder |= DictionarySerdeHelper.Feature.NO_BITMAP_INDEX.getMask();
      }
      if (compressed) {
        flagBuilder |= DictionarySerdeHelper.Feature.COMPRESSED.getMask();
      }
      if (hasMultiValue) {
        flagBuilder |= compressed
                       ? DictionarySerdeHelper.Feature.MULTI_VALUE_V3.getMask()
                       : DictionarySerdeHelper.Feature.MULTI_VALUE.getMask();
      }
      this.flags = flagBuilder;

      if (this.flags == 0) {
        this.version = DictionarySerdeHelper.VERSION.UNCOMPRESSED_SINGLE_VALUE;
      } else if (this.flags == DictionarySerdeHelper.Feature.MULTI_VALUE.getMask()) {
        this.version = DictionarySerdeHelper.VERSION.UNCOMPRESSED_MULTI_VALUE;
      } else {
        this.version = DictionarySerdeHelper.VERSION.FLAG_BASED;
      }
    }

    public SerializerBuilder withDictionary(DictionaryWriter<String> dictionaryWriter)
    {
      this.dictionaryWriter = dictionaryWriter;
      return this;
    }

    public SerializerBuilder withBitmapSerdeFactory(BitmapSerdeFactory bitmapSerdeFactory)
    {
      this.bitmapSerdeFactory = bitmapSerdeFactory;
      return this;
    }

    public SerializerBuilder withBitmapIndex(@Nullable GenericIndexedWriter<ImmutableBitmap> bitmapIndexWriter)
    {
      this.bitmapIndexWriter = bitmapIndexWriter;
      return this;
    }

    public SerializerBuilder withSpatialIndex(ByteBufferWriter<ImmutableRTree> spatialIndexWriter)
    {
      this.spatialIndexWriter = spatialIndexWriter;
      return this;
    }

    public SerializerBuilder withByteOrder(ByteOrder byteOrder)
    {
      this.byteOrder = byteOrder;
      return this;
    }

    public SerializerBuilder withValue(ColumnarIntsSerializer valueWriter)
    {
      this.valueWriter = valueWriter;
      return this;
    }

    public DictionaryEncodedColumnPartSerde build()
    {
      if (DictionarySerdeHelper.Feature.NO_BITMAP_INDEX.isSet(flags) && bitmapIndexWriter != null) {
        throw DruidException.defensive(
            "Feature[%s] is set, but got non null bitmapIndexWriter",
            DictionarySerdeHelper.Feature.NO_BITMAP_INDEX
        );
      } else if (!DictionarySerdeHelper.Feature.NO_BITMAP_INDEX.isSet(flags) && bitmapIndexWriter == null) {
        throw DruidException.defensive(
            "Missing bitmapIndexWriter, do you want to set Feature[%s]?",
            DictionarySerdeHelper.Feature.NO_BITMAP_INDEX
        );
      }
      return new DictionaryEncodedColumnPartSerde(
          byteOrder,
          bitmapSerdeFactory,
          new Serializer()
          {
            @Override
            public long getSerializedSize() throws IOException
            {
              long size = 1 + // version
                          (version.compareTo(DictionarySerdeHelper.VERSION.FLAG_BASED) >= 0
                           ? Integer.BYTES
                           : 0); // flag
              if (dictionaryWriter != null) {
                size += dictionaryWriter.getSerializedSize();
              }
              if (valueWriter != null) {
                size += valueWriter.getSerializedSize();
              }
              if (bitmapIndexWriter != null) {
                size += bitmapIndexWriter.getSerializedSize();
              }
              if (spatialIndexWriter != null) {
                size += spatialIndexWriter.getSerializedSize();
              }
              return size;
            }

            @Override
            public void writeTo(WritableByteChannel channel, SegmentFileBuilder fileBuilder) throws IOException
            {
              Channels.writeFully(channel, ByteBuffer.wrap(new byte[]{version.asByte()}));
              if (version.isFlagBased()) {
                channel.write(ByteBuffer.wrap(Ints.toByteArray(flags)));
              }
              if (dictionaryWriter != null) {
                dictionaryWriter.writeTo(channel, fileBuilder);
              }
              if (valueWriter != null) {
                valueWriter.writeTo(channel, fileBuilder);
              }
              if (bitmapIndexWriter != null) {
                bitmapIndexWriter.writeTo(channel, fileBuilder);
              }
              if (spatialIndexWriter != null) {
                spatialIndexWriter.writeTo(channel, fileBuilder);
              }
            }
          }
      );
    }
  }

  @Override
  public Serializer getSerializer()
  {
    return serializer;
  }

  @Override
  public Deserializer getDeserializer()
  {
    return new Deserializer()
    {
      @Override
      public void read(
          ByteBuffer buffer,
          ColumnBuilder builder,
          ColumnConfig columnConfig,
          @Nullable ColumnHolder parent
      )
      {
        final DictionarySerdeHelper.VERSION rVersion = DictionarySerdeHelper.VERSION.fromByte(buffer.get());
        final int rFlags = rVersion.getFlags(buffer);

        builder.setType(ValueType.STRING);

        final Supplier<? extends Indexed<ByteBuffer>> dictionarySupplier;
        if (parent != null) {
          final Supplier<? extends SelectableColumn> parentSupplier = parent.getColumnSupplier();
          dictionarySupplier = ((StringUtf8DictionaryEncodedColumnSupplier<?>) parentSupplier).getDictionary();
        } else {
          dictionarySupplier = StringEncodingStrategies.getStringDictionarySupplier(
              builder.getFileMapper(),
              buffer,
              byteOrder
          );
        }

        final boolean hasMultipleValues = DictionarySerdeHelper.hasMultiValue(rFlags);
        final WritableSupplier<ColumnarInts> rSingleValuedColumn =
            hasMultipleValues ? null : readSingleValuedColumn(rFlags, buffer, byteOrder, builder.getFileMapper());
        final WritableSupplier<ColumnarMultiInts> rMultiValuedColumn =
            hasMultipleValues ? readMultiValuedColumn(rFlags, buffer, byteOrder, builder.getFileMapper()) : null;

        final boolean hasNulls = dictionarySupplier.get().get(0) == null;

        final StringUtf8DictionaryEncodedColumnSupplier<?> supplier = new StringUtf8DictionaryEncodedColumnSupplier<>(
            dictionarySupplier,
            rSingleValuedColumn,
            rMultiValuedColumn,
            bitmapSerdeFactory.getBitmapFactory()
        );
        builder.setHasMultipleValues(hasMultipleValues)
               .setHasNulls(hasNulls)
               .setDictionaryEncodedColumnSupplier(supplier);

        GenericIndexed<ImmutableBitmap> rBitmaps = null;
        ImmutableRTree rSpatialIndex = null;
        if (!DictionarySerdeHelper.Feature.NO_BITMAP_INDEX.isSet(rFlags)) {
          rBitmaps = GenericIndexed.read(
              buffer,
              bitmapSerdeFactory.getObjectStrategy(),
              builder.getFileMapper()
          );
        }

        if (buffer.hasRemaining()) {
          rSpatialIndex = new ImmutableRTreeObjectStrategy(
              bitmapSerdeFactory.getBitmapFactory()
          ).fromByteBufferWithSize(buffer);
        }

        if (rBitmaps != null || rSpatialIndex != null) {
          builder.setIndexSupplier(
              new StringUtf8ColumnIndexSupplier(
                  bitmapSerdeFactory.getBitmapFactory(),
                  dictionarySupplier,
                  rBitmaps,
                  rSpatialIndex
              ),
              rBitmaps != null,
              rSpatialIndex != null
          );
        }
      }
    };
  }

  public static WritableSupplier<ColumnarInts> readSingleValuedColumn(
      int flags,
      ByteBuffer buffer,
      ByteOrder byteOrder,
      SegmentFileMapper fileMapper
  )
  {
    if (DictionarySerdeHelper.hasMultiValue(flags)) {
      throw DruidException.defensive("Multi-value column");
    }
    if (DictionarySerdeHelper.Feature.COMPRESSED.isSet(flags)) {
      return CompressedVSizeColumnarIntsSupplier.fromByteBuffer(buffer, byteOrder, fileMapper);
    } else {
      return VSizeColumnarInts.readFromByteBuffer(buffer);
    }
  }

  public static WritableSupplier<ColumnarMultiInts> readMultiValuedColumn(
      int flags,
      ByteBuffer buffer,
      ByteOrder byteOrder,
      SegmentFileMapper fileMapper
  )
  {
    if (!DictionarySerdeHelper.hasMultiValue(flags)) {
      throw DruidException.defensive("Single-value column");
    }
    if (!DictionarySerdeHelper.Feature.COMPRESSED.isSet(flags)) {
      return VSizeColumnarMultiInts.readFromByteBuffer(buffer);
    } else if (DictionarySerdeHelper.Feature.MULTI_VALUE.isSet(flags)) {
      return CompressedVSizeColumnarMultiIntsSupplier.fromByteBuffer(buffer, byteOrder, fileMapper);
    } else if (DictionarySerdeHelper.Feature.MULTI_VALUE_V3.isSet(flags)) {
      return V3CompressedVSizeColumnarMultiIntsSupplier.fromByteBuffer(buffer, byteOrder, fileMapper);
    }
    throw DruidException.defensive("Unsupported multi-value flags[%s]", flags);
  }
}
