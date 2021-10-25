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

package org.apache.druid.query.aggregation.longunique;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexColumnPartSupplier;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.LargeColumnSupportedComplexColumnSerializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;


public class LongRoaringBitmapComplexMetricSerde extends ComplexMetricSerde
{

  private static final Logger log = new Logger(LongRoaringBitmapComplexMetricSerde.class);

  @Override
  public String getTypeName()
  {
    return "Roaring64NavigableMap";
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<Roaring64NavigableMap> extractedClass()
      {
        return Roaring64NavigableMap.class;
      }

      @Nullable
      @Override
      public Roaring64NavigableMap extractValue(InputRow inputRow, String metricName)
      {
        // log.info("get rawValue from " + metricName);
        final Object rawValue = inputRow.getRaw(metricName);
        if (rawValue == null) {
          // log.info("rawValue is null, then return new Roaring64NavigableMap()");
          return new Roaring64NavigableMap();
        } else if (rawValue instanceof Roaring64NavigableMap) {
          // log.info("rawValue instance of Roaring64NavigableMap");
          return (Roaring64NavigableMap) rawValue;
        } else {
          Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
          List<String> dimValues = inputRow.getDimension(metricName);
          if (dimValues == null) {
            return bitmap;
          }
          long[] array = dimValues.stream().mapToLong(Long::parseLong).toArray();
          bitmap.add(array);
          return bitmap;
        }
      }
    };
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    GenericIndexed<Roaring64NavigableMap> ge = GenericIndexed.read(
            buffer,
            Roaring64NavigableMapObjectStrategy.STRATEGY,
            builder.getFileMapper()
    );
    builder.setComplexColumnSupplier(new ComplexColumnPartSupplier(getTypeName(), ge));
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return Roaring64NavigableMapObjectStrategy.STRATEGY;
  }


  @Override
  public GenericColumnSerializer getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String column)
  {
    return LargeColumnSupportedComplexColumnSerializer.create(segmentWriteOutMedium, column, this.getObjectStrategy());
  }
}
