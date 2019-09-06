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


import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public abstract class SimpleFloatAggregatorFactory extends NullableAggregatorFactory<ColumnValueSelector>
{
  protected final String name;
  @Nullable
  protected final String fieldName;
  @Nullable
  protected final String expression;
  protected final ExprMacroTable macroTable;
  protected final Supplier<Expr> fieldExpression;

  public SimpleFloatAggregatorFactory(
      ExprMacroTable macroTable,
      String name,
      @Nullable final String fieldName,
      @Nullable String expression
  )
  {
    this.macroTable = macroTable;
    this.name = name;
    this.fieldName = fieldName;
    this.expression = expression;
    this.fieldExpression = Suppliers.memoize(() -> expression == null ? null : Parser.parse(expression, macroTable));
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkArgument(
        fieldName == null ^ expression == null,
        "Must have a valid, non-null fieldName or expression"
    );
  }

  @Override
  protected Aggregator factorize(ColumnSelectorFactory metricFactory, ColumnValueSelector selector)
  {
    if (shouldUseStringColumnAggregatorWrapper(metricFactory)) {
      return new StringColumnFloatAggregatorWrapper(
          selector,
          SimpleFloatAggregatorFactory.this::buildAggregator,
          nullValue()
      );
    } else {
      return buildAggregator(selector);
    }
  }

  @Override
  protected BufferAggregator factorizeBuffered(
      ColumnSelectorFactory metricFactory,
      ColumnValueSelector selector
  )
  {
    if (shouldUseStringColumnAggregatorWrapper(metricFactory)) {
      return new StringColumnFloatBufferAggregatorWrapper(
          selector,
          SimpleFloatAggregatorFactory.this::buildBufferAggregator,
          nullValue()
      );
    } else {
      return buildBufferAggregator(selector);
    }
  }

  @Override
  protected ColumnValueSelector selector(ColumnSelectorFactory metricFactory)
  {
    return AggregatorUtil.makeColumnValueSelectorWithFloatDefault(
        metricFactory,
        fieldName,
        fieldExpression.get(),
        nullValue()
    );
  }

  @Override
  public Object deserialize(Object object)
  {
    // handle "NaN" / "Infinity" values serialized as strings in JSON
    if (object instanceof String) {
      return Float.parseFloat((String) object);
    }
    return object;
  }

  @Override
  public String getTypeName()
  {
    return "float";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Float.BYTES;
  }

  @Override
  public Comparator getComparator()
  {
    return FloatSumAggregator.COMPARATOR;
  }

  @Override
  @Nullable
  public Object finalizeComputation(@Nullable Object object)
  {
    return object;
  }

  @Override
  public List<String> requiredFields()
  {
    return fieldName != null
           ? Collections.singletonList(fieldName)
           : fieldExpression.get().analyzeInputs().getRequiredBindingsList();
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && this.getClass() == other.getClass()) {
      return getCombiningFactory();
    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(fieldName, expression, name);
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

    SimpleFloatAggregatorFactory that = (SimpleFloatAggregatorFactory) o;

    if (!Objects.equals(fieldName, that.fieldName)) {
      return false;
    }
    if (!Objects.equals(expression, that.expression)) {
      return false;
    }
    if (!Objects.equals(name, that.name)) {
      return false;
    }
    return true;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Nullable
  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Nullable
  @JsonProperty
  public String getExpression()
  {
    return expression;
  }

  private boolean shouldUseStringColumnAggregatorWrapper(ColumnSelectorFactory columnSelectorFactory)
  {
    if (fieldName != null) {
      ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(fieldName);
      return capabilities != null && capabilities.getType() == ValueType.STRING;
    }
    return false;
  }

  protected abstract float nullValue();

  protected abstract Aggregator buildAggregator(BaseFloatColumnValueSelector selector);

  protected abstract BufferAggregator buildBufferAggregator(BaseFloatColumnValueSelector selector);
}
