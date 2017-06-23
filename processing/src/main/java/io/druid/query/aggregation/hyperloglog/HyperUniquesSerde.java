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

package io.druid.query.aggregation.hyperloglog;

import com.google.common.collect.Ordering;
import io.druid.data.input.InputRow;
import io.druid.hll.HyperLogLogCollector;
import io.druid.hll.HyperLogLogHash;
import io.druid.segment.GenericColumnSerializer;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.IOPeon;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.LargeColumnSupportedComplexColumnSerializer;

import java.nio.ByteBuffer;
import java.util.List;

public class HyperUniquesSerde extends ComplexMetricSerde
{
  private static Ordering<HyperLogLogCollector> comparator = new Ordering<HyperLogLogCollector>()
  {
    @Override
    public int compare(
        HyperLogLogCollector arg1, HyperLogLogCollector arg2
    )
    {
      return arg1.toByteBuffer().compareTo(arg2.toByteBuffer());
    }
  }.nullsFirst();

  private final HyperLogLogHash hyperLogLogHash;

  public HyperUniquesSerde(HyperLogLogHash hyperLogLogHash)
  {
    this.hyperLogLogHash = hyperLogLogHash;
  }

  @Override
  public String getTypeName()
  {
    return "hyperUnique";
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<HyperLogLogCollector> extractedClass()
      {
        return HyperLogLogCollector.class;
      }

      @Override
      public HyperLogLogCollector extractValue(InputRow inputRow, String metricName)
      {
        Object rawValue = inputRow.getRaw(metricName);

        if (rawValue instanceof HyperLogLogCollector) {
          return (HyperLogLogCollector) rawValue;
        } else {
          HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

          List<String> dimValues = inputRow.getDimension(metricName);
          if (dimValues == null) {
            return collector;
          }

          for (String dimensionValue : dimValues) {
            collector.add(hyperLogLogHash.hash(dimensionValue));
          }
          return collector;
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
  public ObjectStrategy getObjectStrategy()
  {
    return new ObjectStrategy<HyperLogLogCollector>()
    {
      @Override
      public Class<? extends HyperLogLogCollector> getClazz()
      {
        return HyperLogLogCollector.class;
      }

      @Override
      public HyperLogLogCollector fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        // make a copy of buffer, because the given buffer is not duplicated in HyperLogLogCollector.makeCollector() and
        // stored in a field.
        final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
        readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
        return HyperLogLogCollector.makeCollector(readOnlyBuffer);
      }

      @Override
      public byte[] toBytes(HyperLogLogCollector collector)
      {
        if (collector == null) {
          return new byte[]{};
        }
        ByteBuffer val = collector.toByteBuffer();
        byte[] retVal = new byte[val.remaining()];
        val.asReadOnlyBuffer().get(retVal);
        return retVal;
      }

      @Override
      public int compare(HyperLogLogCollector o1, HyperLogLogCollector o2)
      {
        return comparator.compare(o1, o2);
      }
    };
  }

  @Override
  public GenericColumnSerializer getSerializer(IOPeon peon, String column)
  {
    return LargeColumnSupportedComplexColumnSerializer.create(peon, column, this.getObjectStrategy());
  }

}
