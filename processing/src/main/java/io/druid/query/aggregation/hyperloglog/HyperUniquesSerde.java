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

package io.druid.query.aggregation.hyperloglog;

import com.google.common.base.Charsets;
import com.google.common.collect.Ordering;
import com.google.common.hash.HashFunction;
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
import java.util.List;

/**
 */
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

  private final HashFunction hashFn;

  public HyperUniquesSerde(
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
          return (HyperLogLogCollector) inputRow.getRaw(metricName);
        } else {
          HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

          List<String> dimValues = inputRow.getDimension(metricName);
          if (dimValues == null) {
            return collector;
          }

          for (String dimensionValue : dimValues) {
            collector.add(
                hashFn.hashBytes(dimensionValue.getBytes(Charsets.UTF_8)).asBytes()
            );
          }
          return collector;
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
}
