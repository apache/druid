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


import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.util.Comparator;

/**
 * This is an abstract class inherited by various {@link AggregatorFactory} implementations that consume long input
 * and produce long output on aggregation.
 * It extends "NullableAggregatorFactory<ColumnValueSelector>" instead of "NullableAggregatorFactory<BaseLongColumnValueSelector>"
 * to additionally support aggregation on single/multi value string column types.
 */
public abstract class SimpleLongAggregatorFactory extends SimpleNumericAggregatorFactory<BaseLongColumnValueSelector>
{
  public SimpleLongAggregatorFactory(
      ExprMacroTable macroTable,
      String name,
      @Nullable final String fieldName,
      @Nullable String expression
  )
  {
    super(macroTable, name, fieldName, expression);
  }

  @Override
  protected ColumnValueSelector selector(ColumnSelectorFactory metricFactory)
  {
    return AggregatorUtil.makeColumnValueSelectorWithLongDefault(
        metricFactory,
        fieldName,
        fieldExpression.get(),
        nullValue()
    );
  }

  @Override
  protected Aggregator buildStringColumnAggregatorWrapper(BaseObjectColumnValueSelector selector)
  {
    return new StringColumnLongAggregatorWrapper(
        selector,
        SimpleLongAggregatorFactory.this::buildAggregator,
        nullValue()
    );
  }

  @Override
  protected BufferAggregator buildStringColumnBufferAggregatorWrapper(BaseObjectColumnValueSelector selector)
  {
    return new StringColumnLongBufferAggregatorWrapper(
        selector,
        SimpleLongAggregatorFactory.this::buildBufferAggregator,
        nullValue()
    );
  }

  @Override
  public ValueType getType()
  {
    return ValueType.LONG;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Long.BYTES;
  }

  @Override
  public Comparator getComparator()
  {
    return LongSumAggregator.COMPARATOR;
  }

  protected abstract long nullValue();
}
