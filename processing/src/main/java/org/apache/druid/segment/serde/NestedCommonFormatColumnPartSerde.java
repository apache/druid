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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.nested.NestedCommonFormatColumn;
import org.apache.druid.segment.nested.NestedCommonFormatColumnSerializer;
import org.apache.druid.segment.nested.NestedDataColumnSupplier;
import org.apache.druid.segment.nested.ScalarDoubleColumnAndIndexSupplier;
import org.apache.druid.segment.nested.ScalarLongColumnAndIndexSupplier;
import org.apache.druid.segment.nested.ScalarStringColumnAndIndexSupplier;
import org.apache.druid.segment.nested.VariantColumnAndIndexSupplier;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * {@link ColumnPartSerde} shared by all {@link NestedCommonFormatColumn}. The {@link #logicalType} defines the native
 * Druid type of the column to use for things like {@link org.apache.druid.segment.column.ColumnCapabilities} and
 * {@link #hasNulls} if any null values are present. If {@link #isVariantType} is set, the column internally is
 * composed of a mix of types, this is currently only used by {@link VariantColumnAndIndexSupplier}.
 *
 * @see ScalarDoubleColumnAndIndexSupplier
 * @see ScalarLongColumnAndIndexSupplier
 * @see ScalarStringColumnAndIndexSupplier
 * @see VariantColumnAndIndexSupplier
 * @see NestedDataColumnSupplier
 */
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
      @JsonProperty("isVariantType") boolean isVariantType,
      @JsonProperty("enforceLogicalType") boolean enforceLogicalType,
      @JsonProperty("byteOrder") ByteOrder byteOrder,
      @JsonProperty("bitmapSerdeFactory") BitmapSerdeFactory bitmapSerdeFactory
  )
  {
    return new NestedCommonFormatColumnPartSerde(
        logicalType,
        hasNulls,
        isVariantType,
        enforceLogicalType,
        byteOrder,
        bitmapSerdeFactory,
        null
    );
  }

  private final ColumnType logicalType;
  private final boolean hasNulls;
  private final boolean isVariantType;
  private final boolean enforceLogicalType;
  private final ByteOrder byteOrder;
  private final BitmapSerdeFactory bitmapSerdeFactory;

  @Nullable
  private final Serializer serializer;


  private NestedCommonFormatColumnPartSerde(
      ColumnType logicalType,
      boolean hasNulls,
      boolean isVariant,
      boolean enforceLogicalType,
      ByteOrder byteOrder,
      BitmapSerdeFactory bitmapSerdeFactory,
      @Nullable Serializer serializer
  )
  {
    this.logicalType = logicalType;
    this.hasNulls = hasNulls;
    this.isVariantType = isVariant;
    this.enforceLogicalType = enforceLogicalType;
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
    if (isVariantType || logicalType.isArray()) {
      return new VariantColumnDeserializer();
    }
    if (logicalType.is(ValueType.STRING)) {
      return new StringColumnDeserializer();
    }
    if (logicalType.is(ValueType.LONG)) {
      return new LongColumnDeserializer();
    }
    if (logicalType.is(ValueType.DOUBLE)) {
      return new DoubleColumnDeserializer();
    }
    return new NestedColumnDeserializer();
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

  @JsonProperty("isVariantType")
  public boolean isVariantType()
  {
    return isVariantType;
  }

  @JsonProperty("enforceLogicalType")
  public boolean enforceLogicalType()
  {
    return enforceLogicalType;
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

  private class StringColumnDeserializer implements Deserializer
  {
    @Override
    public void read(ByteBuffer buffer, ColumnBuilder builder, ColumnConfig columnConfig)
    {
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
      builder.setNestedCommonFormatColumnSupplier(supplier);
      builder.setIndexSupplier(supplier, true, false);
      builder.setColumnFormat(new NestedCommonFormatColumn.Format(logicalType, capabilitiesBuilder.hasNulls().isTrue(), enforceLogicalType));
    }
  }

  private class LongColumnDeserializer implements Deserializer
  {
    @Override
    public void read(ByteBuffer buffer, ColumnBuilder builder, ColumnConfig columnConfig)
    {
      ScalarLongColumnAndIndexSupplier supplier = ScalarLongColumnAndIndexSupplier.read(
          byteOrder,
          bitmapSerdeFactory,
          buffer,
          builder,
          columnConfig
      );
      ColumnCapabilitiesImpl capabilitiesBuilder = builder.getCapabilitiesBuilder();
      // technically, these columns are dictionary encoded, however they do not implement the DictionaryEncodedColumn
      // interface, so do not make the claim in the ColumnCapabilities
      builder.setType(logicalType);
      builder.setNestedCommonFormatColumnSupplier(supplier);
      builder.setIndexSupplier(supplier, true, false);
      builder.setColumnFormat(new NestedCommonFormatColumn.Format(logicalType, capabilitiesBuilder.hasNulls().isTrue(), enforceLogicalType));
    }
  }

  private class DoubleColumnDeserializer implements Deserializer
  {
    @Override
    public void read(ByteBuffer buffer, ColumnBuilder builder, ColumnConfig columnConfig)
    {
      ScalarDoubleColumnAndIndexSupplier supplier = ScalarDoubleColumnAndIndexSupplier.read(
          byteOrder,
          bitmapSerdeFactory,
          buffer,
          builder,
          columnConfig
      );
      ColumnCapabilitiesImpl capabilitiesBuilder = builder.getCapabilitiesBuilder();
      // technically, these columns are dictionary encoded, however they do not implement the DictionaryEncodedColumn
      // interface, so do not make the claim in the ColumnCapabilities
      builder.setType(logicalType);
      builder.setNestedCommonFormatColumnSupplier(supplier);
      builder.setIndexSupplier(supplier, true, false);
      builder.setColumnFormat(new NestedCommonFormatColumn.Format(logicalType, capabilitiesBuilder.hasNulls().isTrue(), enforceLogicalType));
    }
  }

  private class VariantColumnDeserializer implements Deserializer
  {
    @Override
    public void read(ByteBuffer buffer, ColumnBuilder builder, ColumnConfig columnConfig)
    {
      VariantColumnAndIndexSupplier supplier = VariantColumnAndIndexSupplier.read(
          logicalType,
          byteOrder,
          bitmapSerdeFactory,
          buffer,
          builder,
          columnConfig
      );
      ColumnCapabilitiesImpl capabilitiesBuilder = builder.getCapabilitiesBuilder();
      // if we are a mixed type, don't call ourself dictionary encoded for now so we don't end up doing the wrong thing
      // in places. technically we could probably get by with indicating that our dictionary ids are not unique/sorted
      // but just in case that still causes problems, skip it all...
      if (supplier.getVariantTypeSetByte() == null) {
        capabilitiesBuilder.setDictionaryEncoded(true);
        capabilitiesBuilder.setDictionaryValuesSorted(true);
        capabilitiesBuilder.setDictionaryValuesUnique(true);
      }
      builder.setType(logicalType);
      builder.setNestedCommonFormatColumnSupplier(supplier);
      builder.setIndexSupplier(supplier, true, false);
      builder.setColumnFormat(new NestedCommonFormatColumn.Format(
          logicalType,
          capabilitiesBuilder.hasNulls().isTrue(),
          enforceLogicalType
      ));
    }
  }

  private class NestedColumnDeserializer implements Deserializer
  {
    @Override
    public void read(ByteBuffer buffer, ColumnBuilder builder, ColumnConfig columnConfig)
    {
      NestedDataColumnSupplier supplier = NestedDataColumnSupplier.read(
          logicalType,
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
      builder.setNestedCommonFormatColumnSupplier(supplier);
      // in default value mode, SQL planning by default uses selector filters for things like 'is null', which does
      // not work correctly for complex types (or arrays). so, only hook up this index in sql compatible mode so that
      // query results are consistent when using an index or the value matcher
      // additionally, nested columns only have a null value index, so we only bother with the index supplier if there
      // are actually any null rows, otherwise we use the default 'no indexes' supplier
      if (NullHandling.sqlCompatible() && hasNulls) {
        builder.setIndexSupplier(supplier, false, false);
      }
      builder.setColumnFormat(new NestedCommonFormatColumn.Format(logicalType, hasNulls, enforceLogicalType));
    }
  }

  public static class SerializerBuilder
  {
    private ColumnType logicalType = ColumnType.NESTED_DATA;
    private boolean hasNulls;
    private boolean isVariantType;
    private boolean enforceLogicalType;

    private ByteOrder byteOrder = ByteOrder.nativeOrder();
    BitmapSerdeFactory bitmapSerdeFactory = RoaringBitmapSerdeFactory.getInstance();
    @Nullable
    private Serializer serializer = null;

    public SerializerBuilder withLogicalType(ColumnType logicalType)
    {
      this.logicalType = logicalType;
      return this;
    }

    public SerializerBuilder isVariantType(boolean isVariant)
    {
      this.isVariantType = isVariant;
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

    public SerializerBuilder withEnforceLogicalType(boolean enforceLogicalType)
    {
      this.enforceLogicalType = enforceLogicalType;
      return this;
    }

    public NestedCommonFormatColumnPartSerde build()
    {
      return new NestedCommonFormatColumnPartSerde(
          logicalType,
          hasNulls,
          isVariantType,
          enforceLogicalType,
          byteOrder,
          bitmapSerdeFactory,
          serializer
      );
    }
  }
}
