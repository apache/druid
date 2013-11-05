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

package com.metamx.druid.aggregation;

import com.clearspring.analytics.stream.cardinality.AdaptiveCounting;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.metamx.common.logger.Logger;
import com.metamx.druid.processing.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 */
public class AdaptiveCountingAggregatorFactory implements AggregatorFactory
{
  private static final Logger log = new Logger(AdaptiveCountingAggregatorFactory.class);
  private static final byte CACHE_TYPE_ID = 0x8;

  private final String fieldName;
  private final String name;

  @JsonCreator
  public AdaptiveCountingAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName
  )
  {
    Preconditions.checkArgument(
        fieldName != null && fieldName.length() > 0, "Must have a valid, non-null aggregator name"
    );
    Preconditions.checkArgument(fieldName != null && fieldName.length() > 0, "Must have a valid, non-null fieldName");

    this.name = name;
    this.fieldName = fieldName;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new AdaptiveCountingAggregator(name, metricFactory.makeComplexMetricSelector(fieldName));
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new AdaptiveCountingBufferAggregator(metricFactory.makeComplexMetricSelector(fieldName));
  }

  @Override
  public Comparator getComparator()
  {
    return AdaptiveCountingAggregator.COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return AdaptiveCountingAggregator.combineValues(lhs, rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new AdaptiveCountingAggregatorFactory(name, name);
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof byte[]) {
      return new AdaptiveCounting((byte[]) object);
    } else if (object instanceof ByteBuffer) {
      ByteBuffer buf = (ByteBuffer) object;
      int size = buf.remaining();
      byte[] bytes = new byte[size];
      buf.get(bytes);
      return new AdaptiveCounting(bytes);
    }
    return object;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return ((ICardinality) object).cardinality();
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = fieldName.getBytes();
    return ByteBuffer.allocate(1 + fieldNameBytes.length).put(CACHE_TYPE_ID).put(fieldNameBytes).array();
  }

  @Override
  public String getTypeName()
  {
    return "adaptiveCounting";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return 65536;
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return AdaptiveCounting.Builder.obyCount(Integer.MAX_VALUE).build();
  }

  @Override
  public String toString()
  {
    return "AdaptiveCountingAggregatorFactory{" +
           "fieldName='" + fieldName + '\'' +
           ", name='" + name + '\'' +
           '}';
  }
}
