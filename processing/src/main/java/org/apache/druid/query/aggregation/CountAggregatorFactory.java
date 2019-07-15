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

package org.apache.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 */
public class CountAggregatorFactory extends AggregatorFactory
{
  private final String name;

  @JsonCreator
  public CountAggregatorFactory(
      @JsonProperty("name") String name
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");

    this.name = name;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new CountAggregator();
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new CountBufferAggregator();
  }

  @Override
  public VectorAggregator factorizeVector(final VectorColumnSelectorFactory selectorFactory)
  {
    return new CountVectorAggregator();
  }

  @Override
  public Comparator getComparator()
  {
    return CountAggregator.COMPARATOR;
  }

  @Override
  public boolean canVectorize()
  {
    return true;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return CountAggregator.combineValues(lhs, rhs);
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new LongSumAggregateCombiner();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new LongSumAggregatorFactory(name, name);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(new CountAggregatorFactory(name));
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    return object;
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
    return ImmutableList.of();
  }

  @Override
  public byte[] getCacheKey()
  {
    return new byte[]{AggregatorUtil.COUNT_CACHE_TYPE_ID};
  }

  @Override
  public String getTypeName()
  {
    return "long";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Long.BYTES;
  }

  @Override
  public String toString()
  {
    return "CountAggregatorFactory{" +
           "name='" + name + '\'' +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CountAggregatorFactory that = (CountAggregatorFactory) o;

    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return name != null ? name.hashCode() : 0;
  }
}
