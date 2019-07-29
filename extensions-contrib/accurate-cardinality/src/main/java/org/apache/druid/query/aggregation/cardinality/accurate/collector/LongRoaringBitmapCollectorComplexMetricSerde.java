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

package org.apache.druid.query.aggregation.cardinality.accurate.collector;

import com.google.common.collect.Ordering;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.query.aggregation.cardinality.accurate.AccurateCardinalityModule;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexColumnPartSupplier;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;

public class LongRoaringBitmapCollectorComplexMetricSerde extends ComplexMetricSerde
{
  private LongBitmapCollectorFactory longBitmapCollectorFactory;

  public LongRoaringBitmapCollectorComplexMetricSerde(LongBitmapCollectorFactory longBitmapCollectorFactory)
  {
    this.longBitmapCollectorFactory = longBitmapCollectorFactory;
  }

  private static Ordering<LongBitmapCollector> comparator = new Ordering<LongBitmapCollector>()
  {
    @Override
    public int compare(
        LongBitmapCollector arg1,
        LongBitmapCollector arg2
    )
    {
      return arg1.toByteBuffer().compareTo(arg2.toByteBuffer());
    }
  }.nullsFirst();

  @Override
  public String getTypeName()
  {
    return AccurateCardinalityModule.BITMAP_COLLECTOR;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<LongRoaringBitmapCollector> extractedClass()
      {
        return LongRoaringBitmapCollector.class;
      }

      @Override
      public LongRoaringBitmapCollector extractValue(InputRow inputRow, String metricName)
      {
        final Object object = inputRow.getRaw(metricName);
        if (object instanceof LongRoaringBitmapCollector) {
          return (LongRoaringBitmapCollector) object;
        }
        LongRoaringBitmapCollector collector = (LongRoaringBitmapCollector) longBitmapCollectorFactory.makeEmptyCollector();
        collector.add(Long.valueOf(object.toString()));
        return collector;
      }
    };
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    final GenericIndexed column = GenericIndexed.read(buffer, getObjectStrategy(), builder.getFileMapper());
    builder.setComplexColumnSupplier(new ComplexColumnPartSupplier(getTypeName(), column));
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return new ObjectStrategy<LongRoaringBitmapCollector>()
    {

      @Override
      public int compare(
          LongRoaringBitmapCollector o1,
          LongRoaringBitmapCollector o2
      )
      {
        return comparator.compare(o1, o2);
      }

      @Override
      public Class<? extends LongRoaringBitmapCollector> getClazz()
      {
        return LongRoaringBitmapCollector.class;
      }

      @Override
      public LongRoaringBitmapCollector fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();

        readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
        byte[] bytes = new byte[readOnlyBuffer.remaining()];
        readOnlyBuffer.get(bytes, 0, numBytes);

        return LongRoaringBitmapCollector.deserialize(bytes);
      }

      @Override
      public byte[] toBytes(LongRoaringBitmapCollector collector)
      {
        if (collector == null) {
          return new byte[]{};
        }
        ByteBuffer val = collector.toByteBuffer();
        byte[] retVal = new byte[val.remaining()];
        val.asReadOnlyBuffer().get(retVal);
        return retVal;
      }
    };
  }
}
