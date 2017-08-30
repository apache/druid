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

package io.druid.segment.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.spatial.ImmutableRTree;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.segment.CompressedVSizeIndexedSupplier;
import io.druid.segment.CompressedVSizeIndexedV3Supplier;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.BitmapSerde;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferSerializer;
import io.druid.segment.data.ByteBufferWriter;
import io.druid.segment.data.CompressedVSizeIntsIndexedSupplier;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.GenericIndexedWriter;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedIntsWriter;
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
    MULTI_VALUE,
    MULTI_VALUE_V3;

    public boolean isSet(int flags)
    {
      return (getMask() & flags) != 0;
    }

    public int getMask()
    {
      return (1 << ordinal());
    }
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

  @JsonCreator
  public static DictionaryEncodedColumnPartSerde createDeserializer(
      @Nullable @JsonProperty("bitmapSerdeFactory") BitmapSerdeFactory bitmapSerdeFactory,
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
      Serializer serializer
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
    private VERSION version = null;
    private int flags = NO_FLAGS;
    private GenericIndexedWriter<String> dictionaryWriter = null;
    private IndexedIntsWriter valueWriter = null;
    private BitmapSerdeFactory bitmapSerdeFactory = null;
    private GenericIndexedWriter<ImmutableBitmap> bitmapIndexWriter = null;
    private ByteBufferWriter<ImmutableRTree> spatialIndexWriter = null;
    private ByteOrder byteOrder = null;

    public SerializerBuilder withDictionary(GenericIndexedWriter<String> dictionaryWriter)
    {
      this.dictionaryWriter = dictionaryWriter;
      return this;
    }

    public SerializerBuilder withBitmapSerdeFactory(BitmapSerdeFactory bitmapSerdeFactory)
    {
      this.bitmapSerdeFactory = bitmapSerdeFactory;
      return this;
    }

    public SerializerBuilder withBitmapIndex(GenericIndexedWriter<ImmutableBitmap> bitmapIndexWriter)
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

    public SerializerBuilder withValue(IndexedIntsWriter valueWriter, boolean hasMultiValue, boolean compressed)
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
      return new DictionaryEncodedColumnPartSerde(
          byteOrder,
          bitmapSerdeFactory,
          new Serializer()
          {
            @Override
            public long numBytes()
            {
              long size = 1 + // version
                          (version.compareTo(VERSION.COMPRESSED) >= 0
                           ? Ints.BYTES
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
            public void write(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
            {
              channel.write(ByteBuffer.wrap(new byte[]{version.asByte()}));
              if (version.compareTo(VERSION.COMPRESSED) >= 0) {
                channel.write(ByteBuffer.wrap(Ints.toByteArray(flags)));
              }
              if (dictionaryWriter != null) {
                dictionaryWriter.writeToChannel(channel, smoosher);
              }
              if (valueWriter != null) {
                valueWriter.writeToChannel(channel, smoosher);
              }
              if (bitmapIndexWriter != null) {
                bitmapIndexWriter.writeToChannel(channel, smoosher);
              }
              if (spatialIndexWriter != null) {
                spatialIndexWriter.writeToChannel(channel, smoosher);
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

        final GenericIndexed<String> rDictionary = GenericIndexed.read(
            buffer,
            GenericIndexed.STRING_STRATEGY,
            builder.getFileMapper()
        );
        builder.setType(ValueType.STRING);

        final WritableSupplier<IndexedInts> rSingleValuedColumn;
        final WritableSupplier<IndexedMultivalue<IndexedInts>> rMultiValuedColumn;

        if (hasMultipleValues) {
          rMultiValuedColumn = readMultiValuedColumn(rVersion, buffer, rFlags, builder.getFileMapper());
          rSingleValuedColumn = null;
        } else {
          rSingleValuedColumn = readSingleValuedColumn(rVersion, buffer, builder.getFileMapper());
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
            buffer, bitmapSerdeFactory.getObjectStrategy(), builder.getFileMapper()
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
      }


      private WritableSupplier<IndexedInts> readSingleValuedColumn(
          VERSION version,
          ByteBuffer buffer,
          SmooshedFileMapper fileMapper
      )
      {
        switch (version) {
          case UNCOMPRESSED_SINGLE_VALUE:
            return VSizeIndexedInts.readFromByteBuffer(buffer).asWritableSupplier();
          case COMPRESSED:
            return CompressedVSizeIntsIndexedSupplier.fromByteBuffer(buffer, byteOrder, fileMapper);
          default:
            throw new IAE("Unsupported single-value version[%s]", version);
        }
      }

      private WritableSupplier<IndexedMultivalue<IndexedInts>> readMultiValuedColumn(
          VERSION version, ByteBuffer buffer, int flags, SmooshedFileMapper fileMapper
      )
      {
        switch (version) {
          case UNCOMPRESSED_MULTI_VALUE: {
            return VSizeIndexed.readFromByteBuffer(buffer).asWritableSupplier();
          }
          case COMPRESSED: {
            if (Feature.MULTI_VALUE.isSet(flags)) {
              return CompressedVSizeIndexedSupplier.fromByteBuffer(buffer, byteOrder, fileMapper);
            } else if (Feature.MULTI_VALUE_V3.isSet(flags)) {
              return CompressedVSizeIndexedV3Supplier.fromByteBuffer(buffer, byteOrder, fileMapper);
            } else {
              throw new IAE("Unrecognized multi-value flag[%d]", flags);
            }
          }
          default:
            throw new IAE("Unsupported multi-value version[%s]", version);
        }
      }
    };
  }
}
