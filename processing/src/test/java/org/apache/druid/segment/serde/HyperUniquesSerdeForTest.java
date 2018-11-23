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

package org.apache.druid.segment.serde;

import com.google.common.hash.HashFunction;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

public class HyperUniquesSerdeForTest extends ComplexMetricSerde
{
  private static Comparator<HyperLogLogCollector> comparator =
      Comparator.nullsFirst(Comparator.comparing(HyperLogLogCollector::toByteBuffer));

  private final HashFunction hashFn;

  public HyperUniquesSerdeForTest(
      HashFunction hashFn
  )
  {
    this.hashFn = hashFn;
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
            collector.add(
                hashFn.hashBytes(StringUtils.toUtf8(dimensionValue)).asBytes());
          }
          return collector;
        }
      }
    };
  }

  @Override
  public void deserializeColumn(ByteBuffer byteBuffer, ColumnBuilder columnBuilder)
  {
    final GenericIndexed column;
    if (columnBuilder.getFileMapper() == null) {
      column = GenericIndexed.read(byteBuffer, getObjectStrategy());
    } else {
      column = GenericIndexed.read(byteBuffer, getObjectStrategy(), columnBuilder.getFileMapper());
    }

    columnBuilder.setComplexColumnSupplier(new ComplexColumnPartSupplier(getTypeName(), column));
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
        final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
        readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
        return HyperLogLogCollector.makeCollector(readOnlyBuffer);
      }

      @Override
      public byte[] toBytes(HyperLogLogCollector collector)
      {
        if (collector == null) {
          return ByteArrays.EMPTY_ARRAY;
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
  public GenericColumnSerializer getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String metric)
  {
    return LargeColumnSupportedComplexColumnSerializer.createWithColumnSize(
        segmentWriteOutMedium,
        metric,
        this.getObjectStrategy(),
        Integer.MAX_VALUE
    );
  }
}
