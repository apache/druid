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

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * The sample aggregator provides a single value for a given column per bucket created.
 *
 * Use case:
 * One of your columns has such a high cardinality, that it's impossible to store it. However, there is still
 * value in having "sample" data in druid.
 *
 * @author Hagen Rother, hagen@rother.cc
 */
public class SampleAggregatorFactory implements AggregatorFactory {
  private static final byte[] CACHE_KEY = new byte[]{0x53};

  private final String name;
  private final String fieldName;

  @JsonCreator
  public SampleAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldName") final String fieldName
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.name = name;
    this.fieldName = fieldName;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory) {
    return new SampleAggregator(
        name,
        metricFactory.makeObjectColumnSelector(fieldName)
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory) {
    return new SampleBufferAggregator(
      metricFactory.makeObjectColumnSelector(fieldName)
    );
 }

  @Override
  public Comparator getComparator() {
    return SampleAggregator.COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs) {
    if (lhs != null) {
      return lhs;
    }
    return rhs;
  }

  @Override
  public AggregatorFactory getCombiningFactory() {
    return new SampleAggregatorFactory(name, fieldName);
  }

  @Override
  public Object deserialize(Object object) {
    return object;
  }

  @Override
  public Object finalizeComputation(Object object) {
    return object;
  }

  @JsonProperty
  public String getFieldName() {
    return fieldName;
  }

  @Override
  @JsonProperty
  public String getName() {
    return name;
  }

  @Override
  public List<String> requiredFields() {
    return Arrays.asList(fieldName);
  }

  @Override
  public byte[] getCacheKey() {
    byte[] fieldNameBytes = fieldName.getBytes();
    return ByteBuffer.allocate(1 + fieldNameBytes.length)
        .put(CACHE_KEY)
        .put(fieldNameBytes).array();
  }

  @Override
  public String getTypeName() {
    return "sample";
  }

  @Override
  public int getMaxIntermediateSize() {
    return 32768; // max signed short
  }

  @Override
  public Object getAggregatorStartValue() {
    return null;
  }
}