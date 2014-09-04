/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package io.druid.query.aggregation.cardinality;

import com.google.common.base.Charsets;
import com.google.common.primitives.Longs;
import io.druid.data.input.InputRow;
import io.druid.query.aggregation.cardinality.hll.HyperLogLogPlus;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ColumnPartSerde;
import io.druid.segment.serde.ComplexColumnPartSerde;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class HllPlusComplexMetricSerde extends ComplexMetricSerde
{
  @Override
  public String getTypeName()
  {
    return "hll+";
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<?> extractedClass()
      {
        return HyperLogLogPlus.class;
      }

      @Override
      public Object extractValue(InputRow inputRow, String metricName)
      {
        Object rawVal = inputRow.getRaw(metricName);
        if (rawVal instanceof HyperLogLogPlus) {
          return rawVal;
        }

        HyperLogLogPlus hll = new HyperLogLogPlus(11);
        if (rawVal instanceof List) {
          for (Object o : (List) rawVal) {
            hll.offer(o.toString().getBytes(Charsets.UTF_8));
          }
        }
        else {
          hll.offer(rawVal);
        }
        return hll;
      }
    };
  }

  @Override
  public ColumnPartSerde deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    GenericIndexed column = GenericIndexed.read(buffer, getObjectStrategy());
    builder.setType(ValueType.COMPLEX);
    builder.setComplexColumn(new ComplexColumnPartSupplier("hyperloglog", column));
    return new ComplexColumnPartSerde(column, "hyperloglog");
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return new HllPlusObjectStrategy();
  }

  public static class HllPlusObjectStrategy implements ObjectStrategy<HyperLogLogPlus>
  {
    @Override
    public Class<? extends HyperLogLogPlus> getClazz()
    {
      return HyperLogLogPlus.class;
    }

    @Override
    public HyperLogLogPlus fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      buffer.limit(buffer.position() + numBytes);
      return new HyperLogLogPlus(buffer.order(ByteOrder.nativeOrder()));
    }

    @Override
    public byte[] toBytes(HyperLogLogPlus val)
    {
      byte[] retVal = new byte[val.sizeof()];
      val.getBuffer().duplicate().order(ByteOrder.nativeOrder()).get(retVal);
      return retVal;
    }

    @Override
    public int compare(HyperLogLogPlus o1, HyperLogLogPlus o2)
    {
      return Longs.compare(o1.cardinality(), o2.cardinality());
    }
  }
}

