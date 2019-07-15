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

package org.apache.druid.query.aggregation.hyperloglog;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.hll.HyperLogLogHash;
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
import java.util.Comparator;
import java.util.List;

public class HyperUniquesSerde extends ComplexMetricSerde
{
  private static Comparator<HyperLogLogCollector> comparator =
      Comparator.nullsFirst(Comparator.comparing(HyperLogLogCollector::toByteBuffer));

  private final HyperLogLogHash hyperLogLogHash;

  public HyperUniquesSerde()
  {
    this(HyperLogLogHash.getDefault());
  }

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
  public ComplexMetricExtractor<HyperLogLogCollector> getExtractor()
  {
    return new ComplexMetricExtractor<HyperLogLogCollector>()
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
  public void deserializeColumn(ByteBuffer byteBuffer, ColumnBuilder columnBuilder)
  {
    final GenericIndexed column = GenericIndexed.read(byteBuffer, getObjectStrategy(), columnBuilder.getFileMapper());
    columnBuilder.setComplexColumnSupplier(new ComplexColumnPartSupplier(getTypeName(), column));
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return new ObjectStrategy<HyperLogLogCollector>()
    {
      @Override
      public Class<HyperLogLogCollector> getClazz()
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
  public GenericColumnSerializer getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String column)
  {
    return LargeColumnSupportedComplexColumnSerializer.create(segmentWriteOutMedium, column, this.getObjectStrategy());
  }

}
