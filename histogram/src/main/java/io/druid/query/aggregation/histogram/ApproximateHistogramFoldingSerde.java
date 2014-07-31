/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.aggregation.histogram;

import com.google.common.collect.Ordering;
import io.druid.data.input.InputRow;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ColumnPartSerde;
import io.druid.segment.serde.ComplexColumnPartSerde;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

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

        if (rawValue instanceof ApproximateHistogram) {
          return (ApproximateHistogram) rawValue;
        } else {
          List<String> dimValues = inputRow.getDimension(metricName);
          if (dimValues != null && dimValues.size() > 0) {
            Iterator<String> values = dimValues.iterator();

            ApproximateHistogram h = new ApproximateHistogram();

            while (values.hasNext()) {
              float value = Float.parseFloat(values.next());
              h.offer(value);
            }
            return h;
          } else {
            return new ApproximateHistogram(0);
          }
        }
      }
    };
  }

  @Override
  public ColumnPartSerde deserializeColumn(
      ByteBuffer byteBuffer, ColumnBuilder columnBuilder
  )
  {
    final GenericIndexed column = GenericIndexed.read(byteBuffer, getObjectStrategy());

    columnBuilder.setComplexColumn(new ComplexColumnPartSupplier(getTypeName(), column));

    return new ComplexColumnPartSerde(column, getTypeName());
  }

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
        final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
        readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
        return ApproximateHistogram.fromBytes(readOnlyBuffer);
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
