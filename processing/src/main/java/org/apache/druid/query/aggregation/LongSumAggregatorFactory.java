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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

/**
 */
public class LongSumAggregatorFactory extends SimpleLongAggregatorFactory
{
  @JsonCreator
  public LongSumAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("expression") @Nullable String expression,
      @JacksonInject ExprMacroTable macroTable
  )
  {
    super(macroTable, name, fieldName, expression);
  }

  public LongSumAggregatorFactory(String name, String fieldName)
  {
    this(name, fieldName, null, ExprMacroTable.nil());
  }

  @Override
  protected long nullValue()
  {
    return 0L;
  }

  @Override
  protected Aggregator buildAggregator(BaseLongColumnValueSelector selector)
  {
    return new LongSumAggregator(selector);
  }

  @Override
  protected BufferAggregator buildBufferAggregator(BaseLongColumnValueSelector selector)
  {
    return new LongSumBufferAggregator(selector);
  }

  @Override
  protected VectorValueSelector vectorSelector(VectorColumnSelectorFactory columnSelectorFactory)
  {
    return columnSelectorFactory.makeValueSelector(fieldName);
  }

  @Override
  protected VectorAggregator factorizeVector(
      VectorColumnSelectorFactory columnSelectorFactory,
      VectorValueSelector selector
  )
  {
    return new LongSumVectorAggregator(selector);
  }

  @Override
  public boolean canVectorize(ColumnInspector columnInspector)
  {
    if (fieldName != null) {
      final ColumnCapabilities capabilities = columnInspector.getColumnCapabilities(fieldName);
      return expression == null && (capabilities == null || capabilities.getType().isNumeric());
    }
    return expression == null;
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
    return LongSumAggregator.combineValues(lhs, rhs);
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new LongSumAggregateCombiner();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new LongSumAggregatorFactory(name, name, null, macroTable);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(new LongSumAggregatorFactory(fieldName, fieldName, expression, macroTable));
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8WithNullToEmpty(fieldName);
    byte[] expressionBytes = StringUtils.toUtf8WithNullToEmpty(expression);

    return ByteBuffer.allocate(2 + fieldNameBytes.length + expressionBytes.length)
                     .put(AggregatorUtil.LONG_SUM_CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .put(AggregatorUtil.STRING_SEPARATOR)
                     .put(expressionBytes)
                     .array();
  }

  @Override
  public String toString()
  {
    return "LongSumAggregatorFactory{" +
           "fieldName='" + fieldName + '\'' +
           ", expression='" + expression + '\'' +
           ", name='" + name + '\'' +
           '}';
  }
}
