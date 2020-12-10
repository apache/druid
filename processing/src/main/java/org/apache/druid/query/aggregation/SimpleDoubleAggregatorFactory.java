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
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.util.Comparator;

/**
 * This is an abstract class inherited by various {@link AggregatorFactory} implementations that consume double input
 * and produce double output on aggregation.
 * It extends "NullableAggregatorFactory<ColumnValueSelector>" instead of "NullableAggregatorFactory<BaseDoubleColumnValueSelector>"
 * to additionally support aggregation on single/multi value string column types.
 */
public abstract class SimpleDoubleAggregatorFactory extends SimpleNumericAggregatorFactory<BaseDoubleColumnValueSelector>
{
  protected final boolean storeDoubleAsFloat;

  public SimpleDoubleAggregatorFactory(
      ExprMacroTable macroTable,
      String name,
      @Nullable final String fieldName,
      @Nullable String expression
  )
  {
    super(macroTable, name, fieldName, expression);
    this.storeDoubleAsFloat = ColumnHolder.storeDoubleAsFloat();
  }

  protected abstract double nullValue();

  @Override
  protected ColumnValueSelector selector(ColumnSelectorFactory metricFactory)
  {
    return AggregatorUtil.makeColumnValueSelectorWithDoubleDefault(
        metricFactory,
        fieldName,
        fieldExpression.get(),
        nullValue()
    );
  }

  @Override
  protected Aggregator buildStringColumnAggregatorWrapper(BaseObjectColumnValueSelector selector)
  {
    return new StringColumnDoubleAggregatorWrapper(
        selector,
        SimpleDoubleAggregatorFactory.this::buildAggregator,
        nullValue()
    );
  }

  @Override
  protected BufferAggregator buildStringColumnBufferAggregatorWrapper(BaseObjectColumnValueSelector selector)
  {
    return new StringColumnDoubleBufferAggregatorWrapper(
        selector,
        SimpleDoubleAggregatorFactory.this::buildBufferAggregator,
        nullValue()
    );
  }

  @Override
  public Object deserialize(Object object)
  {
    // handle "NaN" / "Infinity" values serialized as strings in JSON
    if (object instanceof String) {
      return Double.parseDouble((String) object);
    }
    return object;
  }

  @Override
  public ValueType getType()
  {
    if (storeDoubleAsFloat) {
      return ValueType.FLOAT;
    }
    return ValueType.DOUBLE;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Double.BYTES;
  }

  @Override
  public Comparator getComparator()
  {
    return DoubleSumAggregator.COMPARATOR;
  }
}
