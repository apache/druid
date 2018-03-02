/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.StringUtils;
import io.druid.math.expr.ExprMacroTable;
import io.druid.segment.BaseDoubleColumnValueSelector;
import io.druid.segment.BaseNullableColumnValueSelector;
import io.druid.segment.ColumnSelectorFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 */
public class DoubleMinAggregatorFactory extends SimpleDoubleAggregatorFactory
{
  @JsonCreator
  public DoubleMinAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("expression") String expression,
      @JacksonInject ExprMacroTable macroTable
  )
  {
    super(macroTable, fieldName, name, expression);
  }

  public DoubleMinAggregatorFactory(String name, String fieldName)
  {
    this(name, fieldName, null, ExprMacroTable.nil());
  }

  @Override
  public Pair<Aggregator, BaseNullableColumnValueSelector> factorize2(ColumnSelectorFactory metricFactory)
  {
    BaseDoubleColumnValueSelector doubleColumnSelector = getDoubleColumnSelector(
        metricFactory,
        Double.POSITIVE_INFINITY
    );
    return Pair.of(
        new DoubleMinAggregator(doubleColumnSelector),
        doubleColumnSelector
    );
  }

  @Override
  public Pair<BufferAggregator, BaseNullableColumnValueSelector> factorizeBuffered2(ColumnSelectorFactory metricFactory)
  {
    BaseDoubleColumnValueSelector doubleColumnSelector = getDoubleColumnSelector(
        metricFactory,
        Double.POSITIVE_INFINITY
    );
    return Pair.of(
        new DoubleMinBufferAggregator(doubleColumnSelector),
        doubleColumnSelector
    );
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
    return DoubleMinAggregator.combineValues(lhs, rhs);
  }

  @Override
  public AggregateCombiner makeAggregateCombiner2()
  {
    return new DoubleMinAggregateCombiner();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DoubleMinAggregatorFactory(name, name, null, macroTable);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Arrays.<AggregatorFactory>asList(new DoubleMinAggregatorFactory(
        fieldName,
        fieldName,
        expression,
        macroTable
    ));
  }


  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8WithNullToEmpty(fieldName);
    byte[] expressionBytes = StringUtils.toUtf8WithNullToEmpty(expression);

    return ByteBuffer.allocate(2 + fieldNameBytes.length + expressionBytes.length)
                     .put(AggregatorUtil.DOUBLE_MIN_CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .put(AggregatorUtil.STRING_SEPARATOR)
                     .put(expressionBytes)
                     .array();
  }

  @Override
  public String toString()
  {
    return "DoubleMinAggregatorFactory{" +
           "fieldName='" + fieldName + '\'' +
           ", expression='" + expression + '\'' +
           ", name='" + name + '\'' +
           '}';
  }
}
