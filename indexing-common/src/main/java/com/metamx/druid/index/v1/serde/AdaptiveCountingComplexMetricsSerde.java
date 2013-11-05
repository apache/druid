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

package com.metamx.druid.index.v1.serde;

import com.clearspring.analytics.stream.cardinality.AdaptiveCounting;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.google.common.collect.Ordering;
import com.metamx.druid.index.column.ColumnBuilder;
import com.metamx.druid.index.serde.ColumnPartSerde;
import com.metamx.druid.index.serde.ComplexColumnPartSerde;
import com.metamx.druid.index.serde.ComplexColumnPartSupplier;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.kv.GenericIndexed;
import com.metamx.druid.kv.ObjectStrategy;

import java.io.IOException;
import java.nio.ByteBuffer;

public class AdaptiveCountingComplexMetricsSerde extends ComplexMetricSerde
{
  private final ObjectStrategy<ICardinality> cardinalityStrategy = new ObjectStrategy<ICardinality>()
  {
    @Override
    public Class<? extends ICardinality> getClazz()
    {
      return ICardinality.class;
    }

    @Override
    public ICardinality fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      byte[] bytes = new byte[numBytes];
      buffer.get(bytes);
      return new AdaptiveCounting(bytes);
    }

    @Override
    public byte[] toBytes(ICardinality val)
    {
      try {
        final byte[] bytes = val.getBytes();
        return bytes;
      }
      catch (IOException e) {
        return null;
      }
    }

    @Override
    public int compare(ICardinality o1, ICardinality o2)
    {
      return Ordering.natural().nullsFirst().compare(o1.cardinality(), o2.cardinality());
    }
  };

  private final ComplexMetricExtractor cardinalityMetricExtractor = new ComplexMetricExtractor()
  {
    @Override
    public Class<?> extractedClass()
    {
      return ICardinality.class;
    }

    @Override
    public Object extractValue(InputRow inputRow, String metricName)
    {
      final Object value = inputRow.getRaw(metricName);
      if (value instanceof ICardinality) {
        return value;
      } else {
        ICardinality card = AdaptiveCounting.Builder.obyCount(Integer.MAX_VALUE).build();
        card.offer(value);
        return card;
      }
    }
  };

  @Override
  public String getTypeName()
  {
    return "adaptiveCounting";
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return cardinalityMetricExtractor;
  }

  @Override
  public ColumnPartSerde deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    GenericIndexed column = GenericIndexed.read(buffer, cardinalityStrategy);
    builder.setComplexColumn(new ComplexColumnPartSupplier(getTypeName(), column));
    return new ComplexColumnPartSerde(column, getTypeName());
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return cardinalityStrategy;
  }
}
