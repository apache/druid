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

package io.druid.query.aggregation.histogram;

import com.google.common.collect.Ordering;
import io.druid.data.input.InputRow;
import io.druid.data.input.Rows;
import io.druid.segment.GenericColumnSerializer;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.LargeColumnSupportedComplexColumnSerializer;
import io.druid.segment.writeout.SegmentWriteOutMedium;

import java.nio.ByteBuffer;
import java.util.Collection;

public class ApproximateHistogramFoldingSerde extends ComplexMetricSerde
{
  private static Ordering<ApproximateHistogram> comparator = new Ordering<ApproximateHistogram>()
  {
    @Override
    public int compare(
        ApproximateHistogram arg1, ApproximateHistogram arg2
    )
    {
      return ApproximateHistogramAggregator.COMPARATOR.compare(arg1, arg2);
    }
  }.nullsFirst();

  @Override
  public String getTypeName()
  {
    return "approximateHistogram";
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<ApproximateHistogram> extractedClass()
      {
        return ApproximateHistogram.class;
      }

      @Override
      public ApproximateHistogram extractValue(InputRow inputRow, String metricName)
      {
        Object rawValue = inputRow.getRaw(metricName);

        if (rawValue == null) {
          return new ApproximateHistogram(0);
        } else if (rawValue instanceof ApproximateHistogram) {
          return (ApproximateHistogram) rawValue;
        } else {
          ApproximateHistogram h = new ApproximateHistogram();

          if (rawValue instanceof Collection) {
            for (final Object next : ((Collection) rawValue)) {
              if (next != null) {
                h.offer(Rows.objectToNumber(metricName, next).floatValue());
              }
            }
          } else {
            h.offer(Rows.objectToNumber(metricName, rawValue).floatValue());
          }

          return h;
        }
      }
    };
  }

  @Override
  public void deserializeColumn(
      ByteBuffer byteBuffer, ColumnBuilder columnBuilder
  )
  {
    final GenericIndexed column = GenericIndexed.read(byteBuffer, getObjectStrategy(), columnBuilder.getFileMapper());
    columnBuilder.setComplexColumn(new ComplexColumnPartSupplier(getTypeName(), column));
  }

  @Override
  public GenericColumnSerializer getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String column)
  {
    return LargeColumnSupportedComplexColumnSerializer.create(segmentWriteOutMedium, column, this.getObjectStrategy());
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return new ObjectStrategy<ApproximateHistogram>()
    {
      @Override
      public Class<? extends ApproximateHistogram> getClazz()
      {
        return ApproximateHistogram.class;
      }

      @Override
      public ApproximateHistogram fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        buffer.limit(buffer.position() + numBytes);
        return ApproximateHistogram.fromBytes(buffer);
      }

      @Override
      public byte[] toBytes(ApproximateHistogram h)
      {
        if (h == null) {
          return new byte[]{};
        }
        return h.toBytes();
      }

      @Override
      public int compare(ApproximateHistogram o1, ApproximateHistogram o2)
      {
        return comparator.compare(o1, o2);
      }
    };
  }
}
