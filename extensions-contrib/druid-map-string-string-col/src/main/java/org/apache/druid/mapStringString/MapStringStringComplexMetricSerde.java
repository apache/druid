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

package org.apache.druid.mapStringString;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.nio.ByteBuffer;

public class MapStringStringComplexMetricSerde extends ComplexMetricSerde
{
  public static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  public static final MapStringStringComplexMetricSerde INSTANCE = new MapStringStringComplexMetricSerde();

  @Override
  public String getTypeName()
  {
    return MapStringStringDruidModule.TYPE_NAME;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<?> extractedClass()
      {
        return Object.class;
      }

      @Override
      public Object extractValue(InputRow inputRow, String columnName)
      {
        return inputRow.getRaw(columnName);
      }
    };
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    throw new UnsupportedOperationException("Not Supported");
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder, ColumnConfig columnConfig)
  {
    MapStringStringColumnSupplier supplier = new MapStringStringColumnSupplier(buffer, builder, columnConfig, JSON_MAPPER);
    builder.setComplexTypeName(MapStringStringDruidModule.TYPE_NAME);
    builder.setComplexColumnSupplier(supplier);
  }

  @Override
  public GenericColumnSerializer getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String column)
  {
    throw new UnsupportedOperationException("Not needed for dimension column. It is created via DimensionMergerV9.");
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    throw new UnsupportedOperationException("Not supported");
  }
}
