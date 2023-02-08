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
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.primitives.Ints;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.spatial.ImmutableRTree;
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.StringEncodingStrategy;
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
import org.apache.druid.segment.data.EncodedStringDictionaryWriter;
import org.apache.druid.segment.data.FrontCodedIndexed;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.data.ImmutableRTreeObjectStrategy;
import org.apache.druid.segment.data.V3CompressedVSizeColumnarMultiIntsSupplier;
import org.apache.druid.segment.data.VSizeColumnarInts;
import org.apache.druid.segment.data.VSizeColumnarMultiInts;
import org.apache.druid.segment.data.WritableSupplier;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class DictionaryEncodedColumnPartSerde implements ColumnPartSerde
{
  public static final int NO_FLAGS = 0;
  public static final int STARTING_FLAGS = Feature.NO_BITMAP_INDEX.getMask();

  public enum Feature
  {
    MULTI_VALUE,
    MULTI_VALUE_V3,
    NO_BITMAP_INDEX;

    public boolean isSet(int flags)
    {
      return (getMask() & flags) != 0;
    }

    public int getMask()
    {
      return (1 << ordinal());
    }
  }

  public enum VERSION
  {
    UNCOMPRESSED_SINGLE_VALUE,  // 0x0
    UNCOMPRESSED_MULTI_VALUE,   // 0x1
    COMPRESSED,                 // 0x2
    UNCOMPRESSED_WITH_FLAGS;    // 0x3

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

  public static SerializerBuilder serializerBuilder()
  {
    return new SerializerBuilder();
  }

  public static class SerializerBuilder
  {
    private int flags = STARTING_FLAGS;

    @Nullable
    private VERSION version = null;
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
      if (bitmapIndexWriter == null) {
        flags |= Feature.NO_BITMAP_INDEX.getMask();
      } else {
        flags &= ~Feature.NO_BITMAP_INDEX.getMask();
      }

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

    public SerializerBuilder withValue(ColumnarIntsSerializer valueWriter, boolean hasMultiValue, boolean compressed)
    {
      this.valueWriter = valueWriter;
      if (hasMultiValue) {
        if (compressed) {
          this.version = VERSION.COMPRESSED;
          this.flags |= Feature.MULTI_VALUE_V3.getMask();
        } else {
          this.version = VERSION.UNCOMPRESSED_MULTI_VALUE;
          this.flags |= Feature.MULTI_VALUE.getMask();
        }
      } else {
        if (compressed) {
          this.version = VERSION.COMPRESSED;
        } else {
          this.version = VERSION.UNCOMPRESSED_SINGLE_VALUE;
        }
      }
      return this;
    }

    public DictionaryEncodedColumnPartSerde build()
    {
      if (mustWriteFlags(flags) && version.compareTo(VERSION.COMPRESSED) < 0) {
        // Must upgrade version so we can write out flags.
        this.version = VERSION.UNCOMPRESSED_WITH_FLAGS;
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
                          (version.compareTo(VERSION.COMPRESSED) >= 0
                           ? Integer.BYTES
                           : 0); // flag if version >= compressed
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
            public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
            {
              Channels.writeFully(channel, ByteBuffer.wrap(new byte[]{version.asByte()}));
              if (version.compareTo(VERSION.COMPRESSED) >= 0) {
                channel.write(ByteBuffer.wrap(Ints.toByteArray(flags)));
              }
              if (dictionaryWriter != null) {
                dictionaryWriter.writeTo(channel, smoosher);
              }
              if (valueWriter != null) {
                valueWriter.writeTo(channel, smoosher);
              }
              if (bitmapIndexWriter != null) {
                bitmapIndexWriter.writeTo(channel, smoosher);
              }
              if (spatialIndexWriter != null) {
                spatialIndexWriter.writeTo(channel, smoosher);
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
      public void read(ByteBuffer buffer, ColumnBuilder builder, ColumnConfig columnConfig)
      {
        final VERSION rVersion = VERSION.fromByte(buffer.get());
        final int rFlags;

        if (rVersion.compareTo(VERSION.COMPRESSED) >= 0) {
          rFlags = buffer.getInt();
        } else {
          rFlags = rVersion.equals(VERSION.UNCOMPRESSED_MULTI_VALUE)
                   ? Feature.MULTI_VALUE.getMask()
                   : NO_FLAGS;
        }

        final boolean hasMultipleValues = Feature.MULTI_VALUE.isSet(rFlags) || Feature.MULTI_VALUE_V3.isSet(rFlags);

        builder.setType(ValueType.STRING);

        final int dictionaryStartPosition = buffer.position();
        final byte dictionaryVersion = buffer.get();

        if (dictionaryVersion == EncodedStringDictionaryWriter.VERSION) {
          final byte encodingId = buffer.get();
          if (encodingId == StringEncodingStrategy.FRONT_CODED_ID) {
            readFrontCodedColumn(buffer, builder, rVersion, rFlags, hasMultipleValues);
          } else if (encodingId == StringEncodingStrategy.UTF8_ID) {
            // this cannot happen naturally right now since generic indexed is written in the 'legacy' format, but
            // this provides backwards compatibility should we switch at some point in the future to always
            // writing dictionaryVersion
            readGenericIndexedColumn(buffer, builder, columnConfig, rVersion, rFlags, hasMultipleValues);
          } else {
            throw new ISE("impossible, unknown encoding strategy id: %s", encodingId);
          }
        } else {
          // legacy format that only supports plain utf8 enoding stored in GenericIndexed and the byte we are reading
          // as dictionaryVersion is actually also the GenericIndexed version, so we reset start position so the
          // GenericIndexed version can be correctly read
          buffer.position(dictionaryStartPosition);
          readGenericIndexedColumn(buffer, builder, columnConfig, rVersion, rFlags, hasMultipleValues);
        }
      }

      private void readGenericIndexedColumn(
          ByteBuffer buffer,
          ColumnBuilder builder,
          ColumnConfig columnConfig,
          VERSION rVersion,
          int rFlags,
          boolean hasMultipleValues
      )
      {
        // Duplicate the first buffer since we are reading the dictionary twice.
        final GenericIndexed<String> rDictionary = GenericIndexed.read(
            buffer.duplicate(),
            GenericIndexed.STRING_STRATEGY,
            builder.getFileMapper()
        );

        final GenericIndexed<ByteBuffer> rDictionaryUtf8 = GenericIndexed.read(
            buffer,
            GenericIndexed.UTF8_STRATEGY,
            builder.getFileMapper()
        );

        final WritableSupplier<ColumnarInts> rSingleValuedColumn;
        final WritableSupplier<ColumnarMultiInts> rMultiValuedColumn;

        if (hasMultipleValues) {
          rMultiValuedColumn = readMultiValuedColumn(rVersion, buffer, rFlags);
          rSingleValuedColumn = null;
        } else {
          rSingleValuedColumn = readSingleValuedColumn(rVersion, buffer);
          rMultiValuedColumn = null;
        }

        final String firstDictionaryEntry = rDictionary.get(0);

        DictionaryEncodedColumnSupplier dictionaryEncodedColumnSupplier = new DictionaryEncodedColumnSupplier(
            rDictionary,
            rDictionaryUtf8,
            rSingleValuedColumn,
            rMultiValuedColumn,
            columnConfig.columnCacheSizeBytes()
        );

        builder.setHasMultipleValues(hasMultipleValues)
               .setHasNulls(firstDictionaryEntry == null)
               .setDictionaryEncodedColumnSupplier(dictionaryEncodedColumnSupplier);

        GenericIndexed<ImmutableBitmap> rBitmaps = null;
        ImmutableRTree rSpatialIndex = null;
        if (!Feature.NO_BITMAP_INDEX.isSet(rFlags)) {
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
              new DictionaryEncodedStringIndexSupplier(
                  bitmapSerdeFactory.getBitmapFactory(),
                  rDictionary,
                  rDictionaryUtf8,
                  rBitmaps,
                  rSpatialIndex
              ),
              rBitmaps != null,
              rSpatialIndex != null
          );
        }
      }

      private void readFrontCodedColumn(
          ByteBuffer buffer,
          ColumnBuilder builder,
          VERSION rVersion,
          int rFlags,
          boolean hasMultipleValues
      )
      {
        final Supplier<FrontCodedIndexed> rUtf8Dictionary = FrontCodedIndexed.read(
            buffer,
            byteOrder
        );

        final WritableSupplier<ColumnarInts> rSingleValuedColumn;
        final WritableSupplier<ColumnarMultiInts> rMultiValuedColumn;

        if (hasMultipleValues) {
          rMultiValuedColumn = readMultiValuedColumn(rVersion, buffer, rFlags);
          rSingleValuedColumn = null;
        } else {
          rSingleValuedColumn = readSingleValuedColumn(rVersion, buffer);
          rMultiValuedColumn = null;
        }

        final boolean hasNulls = rUtf8Dictionary.get().get(0) == null;

        StringFrontCodedDictionaryEncodedColumnSupplier dictionaryEncodedColumnSupplier =
            new StringFrontCodedDictionaryEncodedColumnSupplier(
                rUtf8Dictionary,
                rSingleValuedColumn,
                rMultiValuedColumn
            );
        builder.setHasMultipleValues(hasMultipleValues)
               .setHasNulls(hasNulls)
               .setDictionaryEncodedColumnSupplier(dictionaryEncodedColumnSupplier);

        GenericIndexed<ImmutableBitmap> rBitmaps = null;
        ImmutableRTree rSpatialIndex = null;
        if (!Feature.NO_BITMAP_INDEX.isSet(rFlags)) {
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
              new StringFrontCodedColumnIndexSupplier(
                  bitmapSerdeFactory.getBitmapFactory(),
                  rUtf8Dictionary,
                  rBitmaps,
                  rSpatialIndex
              ),
              rBitmaps != null,
              rSpatialIndex != null
          );
        }
      }

      private WritableSupplier<ColumnarInts> readSingleValuedColumn(VERSION version, ByteBuffer buffer)
      {
        switch (version) {
          case UNCOMPRESSED_SINGLE_VALUE:
          case UNCOMPRESSED_WITH_FLAGS:
            return VSizeColumnarInts.readFromByteBuffer(buffer);
          case COMPRESSED:
            return CompressedVSizeColumnarIntsSupplier.fromByteBuffer(buffer, byteOrder);
          default:
            throw new IAE("Unsupported single-value version[%s]", version);
        }
      }

      private WritableSupplier<ColumnarMultiInts> readMultiValuedColumn(VERSION version, ByteBuffer buffer, int flags)
      {
        switch (version) {
          case UNCOMPRESSED_MULTI_VALUE: {
            return VSizeColumnarMultiInts.readFromByteBuffer(buffer);
          }
          case UNCOMPRESSED_WITH_FLAGS: {
            if (Feature.MULTI_VALUE.isSet(flags)) {
              return VSizeColumnarMultiInts.readFromByteBuffer(buffer);
            } else {
              throw new IAE("Unrecognized multi-value flag[%d] for version[%s]", flags, version);
            }
          }
          case COMPRESSED: {
            if (Feature.MULTI_VALUE.isSet(flags)) {
              return CompressedVSizeColumnarMultiIntsSupplier.fromByteBuffer(buffer, byteOrder);
            } else if (Feature.MULTI_VALUE_V3.isSet(flags)) {
              return V3CompressedVSizeColumnarMultiIntsSupplier.fromByteBuffer(buffer, byteOrder);
            } else {
              throw new IAE("Unrecognized multi-value flag[%d] for version[%s]", flags, version);
            }
          }
          default:
            throw new IAE("Unsupported multi-value version[%s]", version);
        }
      }
    };
  }

  private static boolean mustWriteFlags(final int flags)
  {
    // Flags that are not implied by version codes < COMPRESSED must be written. This includes MULTI_VALUE_V3.
    return flags != NO_FLAGS && flags != Feature.MULTI_VALUE.getMask();
  }
}
