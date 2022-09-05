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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 */
public class DoubleMaxAggregatorFactory extends SimpleDoubleAggregatorFactory
{
  private final Supplier<byte[]> cacheKey;

  @JsonCreator
  public DoubleMaxAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("expression") @Nullable String expression,
      @JacksonInject ExprMacroTable macroTable
  )
  {
    super(macroTable, name, fieldName, expression);
    this.cacheKey = AggregatorUtil.getSimpleAggregatorCacheKeySupplier(
        AggregatorUtil.DOUBLE_MAX_CACHE_TYPE_ID,
        fieldName,
        fieldExpression
    );
  }

  public DoubleMaxAggregatorFactory(String name, String fieldName)
  {
    this(name, fieldName, null, ExprMacroTable.nil());
  }

  @Override
  protected double nullValue()
  {
    return Double.NEGATIVE_INFINITY;
  }

  @Override
  protected Aggregator buildAggregator(BaseDoubleColumnValueSelector selector)
  {
    return new DoubleMaxAggregator(selector);
  }

  @Override
  protected BufferAggregator buildBufferAggregator(BaseDoubleColumnValueSelector selector)
  {
    return new DoubleMaxBufferAggregator(selector);
  }

  @Override
  protected VectorAggregator factorizeVector(
      VectorColumnSelectorFactory columnSelectorFactory,
      VectorValueSelector selector
  )
  {
    return new DoubleMaxVectorAggregator(selector);
  }

  @Override
  @Nullable
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    if (rhs == null) {
      return lhs;
    }
    if (lhs == null) {
      return rhs;
    }
    return DoubleMaxAggregator.combineValues(lhs, rhs);
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new DoubleMaxAggregateCombiner();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DoubleMaxAggregatorFactory(name, name, null, macroTable);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(new DoubleMaxAggregatorFactory(fieldName, fieldName, expression, macroTable));
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new DoubleMaxAggregatorFactory(newName, getFieldName(), getExpression(), macroTable);
  }

  @Override
  public byte[] getCacheKey()
  {
    return cacheKey.get();
  }

  @Override
  public String toString()
  {
    return "DoubleMaxAggregatorFactory{" +
           "fieldName='" + fieldName + '\'' +
           ", expression='" + expression + '\'' +
           ", name='" + name + '\'' +
           '}';
  }
}
