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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public abstract class SimpleStringAggregatorFactory
    extends NullableNumericAggregatorFactory<ColumnValueSelector<String>>
{
  public static final int DEFAULT_MAX_STRING_SIZE = 1024;

  protected final String name;
  @Nullable
  protected final String fieldName;
  @Nullable
  protected final String expression;
  protected final ExprMacroTable macroTable;
  protected final Supplier<Expr> fieldExpression;
  protected final int maxStringBytes;

  public SimpleStringAggregatorFactory(
      final ExprMacroTable macroTable,
      final String name,
      @Nullable final String fieldName,
      @Nullable final String expression,
      @Nullable final Integer maxStringBytes
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkArgument(
        fieldName == null ^ expression == null,
        "Must have a valid, non-null fieldName or expression"
    );

    if (maxStringBytes == null) {
      this.maxStringBytes = DEFAULT_MAX_STRING_SIZE;
    } else {
      if (maxStringBytes < 0) {
        throw new IAE("maxStringBytes must be greater than 0");
      }

      this.maxStringBytes = maxStringBytes;
    }

    this.macroTable = macroTable;
    this.name = name;
    this.fieldName = fieldName;
    this.expression = expression;
    this.fieldExpression = Parser.lazyParse(expression, macroTable);
  }

  @Override
  protected Aggregator factorize(ColumnSelectorFactory metricFactory, ColumnValueSelector<String> selector)
  {
    return buildAggregator(selector);
  }

  @Override
  protected BufferAggregator factorizeBuffered(
      ColumnSelectorFactory metricFactory,
      ColumnValueSelector<String> selector
  )
  {
    return buildBufferAggregator(selector);
  }

  @Override
  protected ColumnValueSelector<String> selector(ColumnSelectorFactory metricFactory)
  {
    return AggregatorUtil.makeColumnValueSelectorWithStringDefault(
        metricFactory,
        fieldName,
        fieldExpression.get()
    );
  }

  @Override
  protected VectorValueSelector vectorSelector(VectorColumnSelectorFactory columnSelectorFactory)
  {
    return AggregatorUtil.makeVectorValueSelector(columnSelectorFactory, fieldName, expression, fieldExpression);
  }

  @Override
  protected boolean useGetObject(ColumnSelectorFactory columnSelectorFactory)
  {
    return AggregatorUtil.shouldUseObjectColumnAggregatorWrapper(fieldName, columnSelectorFactory);
  }

  @Override
  public Object deserialize(Object object)
  {
    return object.toString(); // Assuming the object can be converted to a String
  }


  @Override
  public ColumnType getIntermediateType()
  {
    return ColumnType.STRING;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Integer.BYTES + maxStringBytes;
  }

  @Override
  public ColumnType getResultType()
  {
    return ColumnType.STRING;
  }

  @Override
  public int guessAggregatorHeapFootprint(long rows)
  {
    return getMaxIntermediateSize();
  }

  @Nullable
  @Override
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
  public int hashCode()
  {
    return Objects.hash(fieldName, expression, name, maxStringBytes);
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

    SimpleStringAggregatorFactory that = (SimpleStringAggregatorFactory) o;

    if (!Objects.equals(fieldName, that.fieldName)) {
      return false;
    }
    if (!Objects.equals(expression, that.expression)) {
      return false;
    }
    if (!Objects.equals(name, that.name)) {
      return false;
    }

    return maxStringBytes == that.maxStringBytes;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getFieldName()
  {
    return fieldName;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getExpression()
  {
    return expression;
  }

  @Override
  public boolean canVectorize(ColumnInspector columnInspector)
  {
    return AggregatorUtil.canVectorize(columnInspector, fieldName, expression, fieldExpression);
  }

  protected abstract Aggregator buildAggregator(BaseObjectColumnValueSelector<String> selector);

  protected abstract BufferAggregator buildBufferAggregator(BaseObjectColumnValueSelector<String> selector);
}
