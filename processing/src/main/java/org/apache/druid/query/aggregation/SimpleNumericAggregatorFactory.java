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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.segment.BaseNullableColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Base {@link AggregatorFactory} for all {@link NullableNumericAggregatorFactory} implementations which produce a
 * primitive numeric output and are based on either a single column or single expression (which might have inputs
 * from multiple column)
 */
public abstract class SimpleNumericAggregatorFactory<TValueSelector extends BaseNullableColumnValueSelector>
    extends NullableNumericAggregatorFactory<ColumnValueSelector>
{
  protected final String name;
  @Nullable
  protected final String fieldName;
  @Nullable
  protected final String expression;
  protected final ExprMacroTable macroTable;
  protected final Supplier<Expr> fieldExpression;

  public SimpleNumericAggregatorFactory(
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
    this.fieldExpression = Parser.lazyParse(expression, macroTable);
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkArgument(
        fieldName == null ^ expression == null,
        "Must have a valid, non-null fieldName or expression"
    );
  }

  protected abstract Aggregator buildAggregator(TValueSelector selector);

  protected abstract Aggregator buildStringColumnAggregatorWrapper(BaseObjectColumnValueSelector selector);

  protected abstract BufferAggregator buildBufferAggregator(TValueSelector selector);

  protected abstract BufferAggregator buildStringColumnBufferAggregatorWrapper(BaseObjectColumnValueSelector selector);

  @Override
  protected Aggregator factorize(ColumnSelectorFactory metricFactory, ColumnValueSelector selector)
  {
    if (shouldUseStringColumnAggregatorWrapper(metricFactory)) {
      return buildStringColumnAggregatorWrapper(selector);
    } else {
      return buildAggregator((TValueSelector) selector);
    }
  }

  @Override
  protected BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory, ColumnValueSelector selector)
  {
    if (shouldUseStringColumnAggregatorWrapper(metricFactory)) {
      return buildStringColumnBufferAggregatorWrapper(selector);
    } else {
      return buildBufferAggregator((TValueSelector) selector);
    }
  }

  @Override
  protected VectorValueSelector vectorSelector(VectorColumnSelectorFactory columnSelectorFactory)
  {
    return AggregatorUtil.makeVectorValueSelector(columnSelectorFactory, fieldName, expression, fieldExpression);
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
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
  public boolean canVectorize(ColumnInspector columnInspector)
  {
    return AggregatorUtil.canVectorize(columnInspector, fieldName, expression, fieldExpression);
  }

  protected boolean shouldUseStringColumnAggregatorWrapper(ColumnSelectorFactory columnSelectorFactory)
  {
    if (fieldName != null) {
      ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(fieldName);
      return capabilities != null && capabilities.getType() == ValueType.STRING;
    }
    return false;
  }

  @Override
  protected boolean hasNulls(ColumnInspector inspector)
  {
    if (fieldName != null) {
      ColumnCapabilities capabilities = inspector.getColumnCapabilities(fieldName);
      if (capabilities != null) {
        return NullHandling.sqlCompatible() && capabilities.hasNulls().isMaybeTrue();
      }
    }
    // expressions are a bit more complicated, even if none of their inputs are null, the expression
    // might still produce a null, so we will need more elaborate handling of this in the future.
    // missing capabilities also falls through to here, and either means 'unknown', which might have nulls,
    // or definitely non-existent, in which case it is all nulls (or default values, depending on the mode)
    return NullHandling.sqlCompatible();
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

    SimpleNumericAggregatorFactory<?> that = (SimpleNumericAggregatorFactory<?>) o;

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
}
