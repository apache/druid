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

package org.apache.druid.segment.nested;

import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.NestedCommonFormatColumnHandler;
import org.apache.druid.segment.NestedDataColumnSchema;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnFormat;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.TypeStrategies;
import org.apache.druid.segment.column.TypeStrategy;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ColumnSerializerUtils;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class NestedDataComplexTypeSerde extends ComplexMetricSerde
{
  public static final String TYPE_NAME = "json";

  public static final NestedDataComplexTypeSerde INSTANCE = new NestedDataComplexTypeSerde();

  @Override
  public String getTypeName()
  {
    return TYPE_NAME;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public void deserializeColumn(
      ByteBuffer buffer,
      ColumnBuilder builder,
      ColumnConfig columnConfig
  )
  {
    final NestedDataColumnSupplierV4 supplier = NestedDataColumnSupplierV4.read(
        buffer,
        builder,
        columnConfig,
        ColumnSerializerUtils.SMILE_MAPPER
    );
    final ColumnCapabilitiesImpl capabilitiesBuilder = builder.getCapabilitiesBuilder();
    capabilitiesBuilder.setDictionaryEncoded(true);
    capabilitiesBuilder.setDictionaryValuesSorted(true);
    capabilitiesBuilder.setDictionaryValuesUnique(true);
    final ColumnType simpleType = supplier.getSimpleType();
    if (simpleType != null) {
      builder.setType(simpleType);
    } else {
      builder.setComplexTypeName(TYPE_NAME);
    }
    builder.setComplexColumnSupplier(supplier);
    builder.setColumnFormat(new NestedColumnFormatV4());
  }

  @Override
  public ObjectStrategy<Object> getObjectStrategy()
  {
    return new ObjectStrategy<>()
    {
      @Override
      public int compare(Object o1, Object o2)
      {
        return TypeStrategies.NESTED.compare(o1, o2);
      }

      @Override
      public Class<? extends Object> getClazz()
      {
        return TypeStrategies.NESTED.getClazz();
      }

      @Nullable
      @Override
      public StructuredData fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        return TypeStrategies.NESTED.fromByteBuffer(buffer, numBytes);
      }

      @Nullable
      @Override
      public byte[] toBytes(@Nullable Object val)
      {
        return TypeStrategies.NESTED.toBytes(val);
      }

      @Override
      public boolean readRetainsBufferReference()
      {
        return TypeStrategies.NESTED.readRetainsBufferReference();
      }
    };
  }

  @Override
  public TypeStrategy<Object> getTypeStrategy()
  {
    return TypeStrategies.NESTED;
  }

  @Override
  public byte[] toBytes(@Nullable Object val)
  {
    return getObjectStrategy().toBytes(val);
  }

  public static class NestedColumnFormatV4 implements ColumnFormat
  {
    @Override
    public ColumnType getLogicalType()
    {
      return ColumnType.NESTED_DATA;
    }

    @Override
    public DimensionHandler getColumnHandler(String columnName)
    {
      return new NestedCommonFormatColumnHandler(columnName, null, IndexSpec.getDefault().getAutoColumnFormatSpec());
    }

    @Override
    public DimensionSchema getColumnSchema(String columnName)
    {
      return new NestedDataColumnSchema(columnName, 5);
    }

    @Override
    public ColumnFormat merge(@Nullable ColumnFormat otherFormat)
    {
      // we don't care, we are anything, there is no configurability
      return this;
    }

    @Override
    public ColumnCapabilities toColumnCapabilities()
    {
      return ColumnCapabilitiesImpl.createDefault().setType(ColumnType.NESTED_DATA).setHasNulls(true);
    }
  }
}
