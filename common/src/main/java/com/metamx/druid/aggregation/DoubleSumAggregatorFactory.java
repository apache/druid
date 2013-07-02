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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import com.metamx.druid.processing.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 */
public class DoubleSumAggregatorFactory implements AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0x2;

  private final String fieldName;
  private final String name;

  @JsonCreator
  public DoubleSumAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.name = name;
    this.fieldName = fieldName;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new DoubleSumAggregator(
        name,
        metricFactory.makeFloatMetricSelector(fieldName)
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new DoubleSumBufferAggregator(metricFactory.makeFloatMetricSelector(fieldName));
  }

  @Override
  public Comparator getComparator()
  {
    return DoubleSumAggregator.COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return DoubleSumAggregator.combineValues(lhs, rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DoubleSumAggregatorFactory(name, name);
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return object;
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
    return "float";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Doubles.BYTES;
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return 0;
  }

  @Override
  public String toString()
  {
    return "DoubleSumAggregatorFactory{" +
           "fieldName='" + fieldName + '\'' +
           ", name='" + name + '\'' +
           '}';
  }
}
