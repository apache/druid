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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import it.unimi.dsi.fastutil.Hash;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.NestedDataColumnHandlerV4;
import org.apache.druid.segment.NestedDataColumnSchema;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnFormat;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ObjectStrategyComplexTypeStrategy;
import org.apache.druid.segment.column.TypeStrategy;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;

public class NestedDataComplexTypeSerde extends ComplexMetricSerde
{
  public static final String TYPE_NAME = "json";

  public static final ObjectMapper OBJECT_MAPPER;

  public static final NestedDataComplexTypeSerde INSTANCE = new NestedDataComplexTypeSerde();

  static {
    final SmileFactory smileFactory = new SmileFactory();
    smileFactory.configure(SmileGenerator.Feature.ENCODE_BINARY_AS_7BIT, false);
    smileFactory.delegateToTextual(true);
    final ObjectMapper mapper = new DefaultObjectMapper(smileFactory, null);
    mapper.getFactory().setCodec(mapper);
    mapper.registerModules(NestedDataModule.getJacksonModulesList());
    OBJECT_MAPPER = mapper;
  }

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
        OBJECT_MAPPER
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
  public ObjectStrategy getObjectStrategy()
  {
    return new ObjectStrategy<Object>()
    {
      @Override
      public int compare(Object o1, Object o2)
      {
        return Comparators.<StructuredData>naturalNullsFirst()
                          .compare(StructuredData.wrap(o1), StructuredData.wrap(o2));
      }

      @Override
      public Class<? extends Object> getClazz()
      {
        return StructuredData.class;
      }

      @Nullable
      @Override
      public Object fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        final byte[] bytes = new byte[numBytes];
        buffer.get(bytes, 0, numBytes);
        try {
          return OBJECT_MAPPER.readValue(bytes, StructuredData.class);
        }
        catch (IOException e) {
          throw new ISE(e, "Unable to deserialize value");
        }
      }

      @Nullable
      @Override
      public byte[] toBytes(@Nullable Object val)
      {
        if (val == null) {
          return new byte[0];
        }
        try {
          return OBJECT_MAPPER.writeValueAsBytes(val);
        }
        catch (JsonProcessingException e) {
          throw new ISE(e, "Unable to serialize value [%s]", val);
        }
      }
    };
  }

  @Override
  public <T extends Comparable<T>> TypeStrategy<T> getTypeStrategy()
  {
    return new ObjectStrategyComplexTypeStrategy<>(
        getObjectStrategy(),
        ColumnType.ofComplex(TYPE_NAME),
        new Hash.Strategy<Object>()
        {
          @Override
          public int hashCode(Object o)
          {
            return StructuredData.wrap(o).equalityHash();
          }

          @Override
          public boolean equals(Object a, Object b)
          {
            return StructuredData.wrap(a).compareTo(StructuredData.wrap(b)) == 0;
          }
        }
    );
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
      return new NestedDataColumnHandlerV4(columnName);
    }

    @Override
    public DimensionSchema getColumnSchema(String columnName)
    {
      return new NestedDataColumnSchema(columnName, 4);
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
