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

import com.fasterxml.jackson.annotation.*;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.*;
import org.apache.druid.query.aggregation.any.DoubleAnyAggregator;
import org.apache.druid.query.aggregation.any.LongAnyAggregator;
import org.apache.druid.query.aggregation.first.DoubleFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.FirstLastUtils;
import org.apache.druid.query.aggregation.first.LongFirstAggregatorFactory;
import org.apache.druid.query.aggregation.last.DoubleLastAggregator;
import org.apache.druid.query.aggregation.last.DoubleLastBufferAggregator;
import org.apache.druid.query.aggregation.last.DoubleLastVectorAggregator;
import org.apache.druid.query.aggregation.last.GenericLastAggregateCombiner;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.*;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.Types;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.*;

@JsonTypeName("singleValue")
public class SingleValueAggregatorFactory extends AggregatorFactory
{

  private static final Aggregator NIL_AGGREGATOR = new LongAnyAggregator(
          NilColumnValueSelector.instance()
  )
  {
    @Override
    public void aggregate()
    {
      // no-op
    }
  };
  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final String name;

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final String fieldName;

  @JsonCreator
  public SingleValueAggregatorFactory(
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
  public Aggregator factorize(ColumnSelectorFactory metricFactory) {
    final BaseLongColumnValueSelector valueSelector = metricFactory.makeColumnValueSelector(fieldName);
    if (valueSelector instanceof NilColumnValueSelector) {
      return NIL_AGGREGATOR;
    } else {
      return new SingleValueAggregator(
              valueSelector
      );
    }
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory) {
    final BaseLongColumnValueSelector valueSelector = metricFactory.makeColumnValueSelector(fieldName);
    return new SingleValueBufferAggregator(valueSelector);
  }

  @Override
  public Comparator getComparator()
  {
    return LongFirstAggregatorFactory.VALUE_COMPARATOR;
  }

  @Override
  @Nullable
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    return ((Number) lhs).longValue() + ((Number) rhs).longValue();
  }

  @Override
  public AggregatorFactory getCombiningFactory() {
    return new SingleValueAggregatorFactory(name, fieldName);
  }

  @Override
  public Object deserialize(Object object)
  {
    return object == null ? null : (Long) object;
  }

  @Override
  @Nullable
  public Object finalizeComputation(@Nullable Object object)
  {
    return object == null ? null : (Long) object;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public List<String> requiredFields() {
    return Collections.singletonList(fieldName);
  }

  @Override
  public int getMaxIntermediateSize() {
    return Long.BYTES;
  }

  @Override
  public byte[] getCacheKey() {
    return new byte[]{AggregatorUtil.SINGLE_VALUE_CACHE_TYPE_ID};
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return ColumnType.LONG;
  }

  @Override
  public ColumnType getResultType()
  {
    return ColumnType.LONG;
  }
}
