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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.nested.NestedCommonFormatColumn;
import org.apache.druid.segment.nested.NestedCommonFormatColumnSerializer;
import org.apache.druid.segment.nested.NestedDataColumnSupplier;
import org.apache.druid.segment.nested.ScalarDoubleColumnAndIndexSupplier;
import org.apache.druid.segment.nested.ScalarLongColumnAndIndexSupplier;
import org.apache.druid.segment.nested.ScalarStringColumnAndIndexSupplier;
import org.apache.druid.segment.nested.VariantArrayColumnAndIndexSupplier;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class NestedCommonFormatColumnPartSerde implements ColumnPartSerde
{
  public static SerializerBuilder serializerBuilder()
  {
    return new SerializerBuilder();
  }

  public static ByteBuffer loadInternalFile(
      SmooshedFileMapper fileMapper,
      String fileNameBase,
      String internalFileName
  ) throws IOException
  {
    return fileMapper.mapFile(
        NestedCommonFormatColumnSerializer.getInternalFileName(fileNameBase, internalFileName)
    );
  }

  @JsonCreator
  public static NestedCommonFormatColumnPartSerde createDeserializer(
      @JsonProperty("logicalType") ColumnType logicalType,
      @JsonProperty("hasNulls") boolean hasNulls,
      @JsonProperty("byteOrder") ByteOrder byteOrder,
      @JsonProperty("bitmapSerdeFactory") BitmapSerdeFactory bitmapSerdeFactory
  )
  {
    return new NestedCommonFormatColumnPartSerde(logicalType, hasNulls, byteOrder, bitmapSerdeFactory, null);
  }

  private final ColumnType logicalType;
  private final boolean hasNulls;
  private final ByteOrder byteOrder;
  private final BitmapSerdeFactory bitmapSerdeFactory;

  @Nullable
  private final Serializer serializer;


  private NestedCommonFormatColumnPartSerde(
      ColumnType logicalType,
      boolean hasNulls,
      ByteOrder byteOrder,
      BitmapSerdeFactory bitmapSerdeFactory,
      @Nullable Serializer serializer
  )
  {
    this.logicalType = logicalType;
    this.hasNulls = hasNulls;
    this.byteOrder = byteOrder;
    this.bitmapSerdeFactory = bitmapSerdeFactory;
    this.serializer = serializer;
  }

  @JsonIgnore
  @Nullable
  @Override
  public Serializer getSerializer()
  {
    return serializer;
  }

  @Override
  public Deserializer getDeserializer()
  {
    if (logicalType.is(ValueType.STRING)) {
      return ((buffer, builder, columnConfig) -> {
        ScalarStringColumnAndIndexSupplier supplier = ScalarStringColumnAndIndexSupplier.read(
            byteOrder,
            bitmapSerdeFactory,
            buffer,
            builder,
            columnConfig
        );
        ColumnCapabilitiesImpl capabilitiesBuilder = builder.getCapabilitiesBuilder();
        capabilitiesBuilder.setDictionaryEncoded(true);
        capabilitiesBuilder.setDictionaryValuesSorted(true);
        capabilitiesBuilder.setDictionaryValuesUnique(true);
        builder.setType(logicalType);
        builder.setStandardTypeColumnSupplier(supplier);
        builder.setIndexSupplier(supplier, true, false);
        builder.setColumnFormat(new NestedCommonFormatColumn.Format(logicalType, capabilitiesBuilder.hasNulls().isTrue()));
      });
    }
    if (logicalType.is(ValueType.LONG)) {
      return ((buffer, builder, columnConfig) -> {
        ScalarLongColumnAndIndexSupplier supplier = ScalarLongColumnAndIndexSupplier.read(
            byteOrder,
            bitmapSerdeFactory,
            buffer,
            builder,
            columnConfig
        );
        ColumnCapabilitiesImpl capabilitiesBuilder = builder.getCapabilitiesBuilder();
        capabilitiesBuilder.setDictionaryEncoded(true);
        capabilitiesBuilder.setDictionaryValuesSorted(true);
        capabilitiesBuilder.setDictionaryValuesUnique(true);
        builder.setType(logicalType);
        builder.setStandardTypeColumnSupplier(supplier);
        builder.setIndexSupplier(supplier, true, false);
        builder.setColumnFormat(new NestedCommonFormatColumn.Format(logicalType, capabilitiesBuilder.hasNulls().isTrue()));
      });
    }
    if (logicalType.is(ValueType.DOUBLE)) {
      return ((buffer, builder, columnConfig) -> {
        ScalarDoubleColumnAndIndexSupplier supplier = ScalarDoubleColumnAndIndexSupplier.read(
            byteOrder,
            bitmapSerdeFactory,
            buffer,
            builder,
            columnConfig
        );
        ColumnCapabilitiesImpl capabilitiesBuilder = builder.getCapabilitiesBuilder();
        capabilitiesBuilder.setDictionaryEncoded(true);
        capabilitiesBuilder.setDictionaryValuesSorted(true);
        capabilitiesBuilder.setDictionaryValuesUnique(true);
        builder.setType(logicalType);
        builder.setStandardTypeColumnSupplier(supplier);
        builder.setIndexSupplier(supplier, true, false);
        builder.setColumnFormat(new NestedCommonFormatColumn.Format(logicalType, capabilitiesBuilder.hasNulls().isTrue()));
      });
    }
    if (logicalType.isArray()) {
      return ((buffer, builder, columnConfig) -> {
        VariantArrayColumnAndIndexSupplier supplier = VariantArrayColumnAndIndexSupplier.read(
            logicalType,
            byteOrder,
            bitmapSerdeFactory,
            buffer,
            builder,
            columnConfig
        );
        ColumnCapabilitiesImpl capabilitiesBuilder = builder.getCapabilitiesBuilder();
        capabilitiesBuilder.setDictionaryEncoded(true);
        capabilitiesBuilder.setDictionaryValuesSorted(true);
        capabilitiesBuilder.setDictionaryValuesUnique(true);
        builder.setType(logicalType);
        builder.setStandardTypeColumnSupplier(supplier);
        builder.setColumnFormat(new NestedCommonFormatColumn.Format(logicalType, capabilitiesBuilder.hasNulls().isTrue()));
      });
    }
    return (buffer, builder, columnConfig) -> {
      NestedDataColumnSupplier supplier = NestedDataColumnSupplier.read(
          hasNulls,
          buffer,
          builder,
          columnConfig,
          bitmapSerdeFactory,
          byteOrder
      );
      ColumnCapabilitiesImpl capabilitiesBuilder = builder.getCapabilitiesBuilder();
      capabilitiesBuilder.setDictionaryEncoded(true);
      capabilitiesBuilder.setDictionaryValuesSorted(true);
      capabilitiesBuilder.setDictionaryValuesUnique(true);
      ColumnType simpleType = supplier.getLogicalType();
      ColumnType logicalType = simpleType == null ? ColumnType.NESTED_DATA : simpleType;
      builder.setType(logicalType);
      builder.setStandardTypeColumnSupplier(supplier);
      builder.setColumnFormat(new NestedCommonFormatColumn.Format(logicalType, hasNulls));
    };
  }

  @JsonProperty
  public ColumnType getLogicalType()
  {
    return logicalType;
  }

  @JsonProperty
  public boolean isHasNulls()
  {
    return hasNulls;
  }

  @JsonProperty
  public ByteOrder getByteOrder()
  {
    return byteOrder;
  }

  @JsonProperty
  public BitmapSerdeFactory getBitmapSerdeFactory()
  {
    return bitmapSerdeFactory;
  }

  public static class SerializerBuilder
  {
    private ColumnType logicalType;
    private boolean hasNulls;
    private ByteOrder byteOrder = ByteOrder.nativeOrder();
    BitmapSerdeFactory bitmapSerdeFactory;
    @Nullable
    private Serializer serializer = null;

    public SerializerBuilder withLogicalType(ColumnType logicalType)
    {
      this.logicalType = logicalType;
      return this;
    }

    public SerializerBuilder withSerializer(final Serializer serializer)
    {
      this.serializer = serializer;
      return this;
    }

    public SerializerBuilder withByteOrder(final ByteOrder byteOrder)
    {
      this.byteOrder = byteOrder;
      return this;
    }

    public SerializerBuilder withBitmapSerdeFactory(BitmapSerdeFactory serdeFactory)
    {
      this.bitmapSerdeFactory = serdeFactory;
      return this;
    }

    public SerializerBuilder withHasNulls(boolean hasNulls)
    {
      this.hasNulls = hasNulls;
      return this;
    }

    public NestedCommonFormatColumnPartSerde build()
    {
      return new NestedCommonFormatColumnPartSerde(logicalType, hasNulls, byteOrder, bitmapSerdeFactory, serializer);
    }
  }
}
