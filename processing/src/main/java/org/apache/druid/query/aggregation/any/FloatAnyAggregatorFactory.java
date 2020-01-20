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

package org.apache.druid.query.aggregation.any;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.SimpleFloatAggregatorFactory;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.BaseFloatColumnValueSelector;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public class FloatAnyAggregatorFactory extends SimpleFloatAggregatorFactory
{
  @JsonCreator
  public FloatAnyAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("expression") @Nullable String expression,
      @JacksonInject ExprMacroTable macroTable
  )
  {
    super(macroTable, name, fieldName, expression);
  }

  public FloatAnyAggregatorFactory(String name, String fieldName)
  {
    this(name, fieldName, null, ExprMacroTable.nil());
  }

  @Override
  protected float nullValue()
  {
    return Float.NaN;
  }

  @Override
  protected Aggregator buildAggregator(BaseFloatColumnValueSelector selector)
  {
    return new FloatAnyAggregator(selector);
  }

  @Override
  protected BufferAggregator buildBufferAggregator(BaseFloatColumnValueSelector selector)
  {
    return new FloatAnyBufferAggregator(selector);
  }

  @Override
  @Nullable
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    if (lhs != null) {
      return lhs;
    } else {
      return rhs;
    }
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new FloatAnyAggregatorFactory(name, name, null, macroTable);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(new FloatAnyAggregatorFactory(fieldName, fieldName, expression, macroTable));
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.FLOAT_ANY_CACHE_TYPE_ID)
        .appendString(fieldName)
        .appendString(expression)
        .build();
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Float.BYTES + Byte.BYTES;
  }

  @Override
  public String toString()
  {
    return "FloatAnyAggregatorFactory{" +
           "fieldName='" + fieldName + '\'' +
           ", expression='" + expression + '\'' +
           ", name='" + name + '\'' +
           '}';
  }
}
