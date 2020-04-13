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

package org.apache.druid.query.aggregation.variance;

import com.google.common.collect.Ordering;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexColumnPartSupplier;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.LargeColumnSupportedComplexColumnSerializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.nio.ByteBuffer;
import java.util.List;

/**
 */
public class VarianceSerde extends ComplexMetricSerde
{
  private static final Ordering<VarianceAggregatorCollector> COMPARATOR =
      Ordering.from(VarianceAggregatorCollector.COMPARATOR).nullsFirst();

  @Override
  public String getTypeName()
  {
    return "variance";
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<VarianceAggregatorCollector> extractedClass()
      {
        return VarianceAggregatorCollector.class;
      }

      @Override
      public VarianceAggregatorCollector extractValue(InputRow inputRow, String metricName)
      {
        Object rawValue = inputRow.getRaw(metricName);

        if (rawValue instanceof VarianceAggregatorCollector) {
          return (VarianceAggregatorCollector) rawValue;
        }
        VarianceAggregatorCollector collector = new VarianceAggregatorCollector();

        List<String> dimValues = inputRow.getDimension(metricName);
        if (dimValues != null && dimValues.size() > 0) {
          for (String dimValue : dimValues) {
            float value = Float.parseFloat(dimValue);
            collector.add(value);
          }
        }
        return collector;
      }
    };
  }

  @Override
  public void deserializeColumn(ByteBuffer byteBuffer, ColumnBuilder columnBuilder)
  {
    final GenericIndexed column = GenericIndexed.read(byteBuffer, getObjectStrategy(), columnBuilder.getFileMapper());
    columnBuilder.setComplexColumnSupplier(new ComplexColumnPartSupplier(getTypeName(), column));
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return new ObjectStrategy<VarianceAggregatorCollector>()
    {
      @Override
      public Class<VarianceAggregatorCollector> getClazz()
      {
        return VarianceAggregatorCollector.class;
      }

      @Override
      public VarianceAggregatorCollector fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        buffer.limit(buffer.position() + numBytes);
        return VarianceAggregatorCollector.from(buffer);
      }

      @Override
      public byte[] toBytes(VarianceAggregatorCollector collector)
      {
        return collector == null ? new byte[]{} : collector.toByteArray();
      }

      @Override
      public int compare(VarianceAggregatorCollector o1, VarianceAggregatorCollector o2)
      {
        return COMPARATOR.compare(o1, o2);
      }
    };
  }

  @Override
  public GenericColumnSerializer getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String column)
  {
    return LargeColumnSupportedComplexColumnSerializer.create(segmentWriteOutMedium, column, this.getObjectStrategy());
  }

}
