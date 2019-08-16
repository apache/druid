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

package org.apache.druid.segment.virtual;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ValueType;

import java.util.List;
import java.util.Objects;

public class ExpressionVirtualColumn implements VirtualColumn
{
  private final String name;
  private final String expression;
  private final ValueType outputType;
  private final Supplier<Expr> parsedExpression;

  @JsonCreator
  public ExpressionVirtualColumn(
      @JsonProperty("name") String name,
      @JsonProperty("expression") String expression,
      @JsonProperty("outputType") ValueType outputType,
      @JacksonInject ExprMacroTable macroTable
  )
  {
    this.name = Preconditions.checkNotNull(name, "name");
    this.expression = Preconditions.checkNotNull(expression, "expression");
    this.outputType = outputType != null ? outputType : ValueType.FLOAT;
    this.parsedExpression = Suppliers.memoize(() -> Parser.parse(expression, macroTable));
  }

  @JsonProperty("name")
  @Override
  public String getOutputName()
  {
    return name;
  }

  @JsonProperty
  public String getExpression()
  {
    return expression;
  }

  @JsonProperty
  public ValueType getOutputType()
  {
    return outputType;
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      final DimensionSpec dimensionSpec,
      final ColumnSelectorFactory columnSelectorFactory
  )
  {
    return dimensionSpec.decorate(
        ExpressionSelectors.makeDimensionSelector(
            columnSelectorFactory,
            parsedExpression.get(),
            dimensionSpec.getExtractionFn()
        )
    );
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(String columnName, ColumnSelectorFactory factory)
  {
    return ExpressionSelectors.makeColumnValueSelector(factory, parsedExpression.get());
  }

  @Override
  public ColumnCapabilities capabilities(String columnName)
  {
    // Note: Ideally we would only "setHasMultipleValues(true)" if the expression in question could potentially return
    // multiple values. However, we don't currently have a good way of determining this, so to be safe we always
    // set the flag.
    return new ColumnCapabilitiesImpl().setType(outputType).setHasMultipleValues(true);
  }

  @Override
  public List<String> requiredColumns()
  {
    return parsedExpression.get().analyzeInputs().getRequiredBindingsList();
  }

  @Override
  public boolean usesDotNotation()
  {
    return false;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(VirtualColumnCacheHelper.CACHE_TYPE_ID_EXPRESSION)
        .appendString(name)
        .appendString(expression)
        .appendString(outputType.toString())
        .build();
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ExpressionVirtualColumn that = (ExpressionVirtualColumn) o;
    return Objects.equals(name, that.name) &&
           Objects.equals(expression, that.expression) &&
           outputType == that.outputType;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, expression, outputType);
  }

  @Override
  public String toString()
  {
    return "ExpressionVirtualColumn{" +
           "name='" + name + '\'' +
           ", expression='" + expression + '\'' +
           ", outputType=" + outputType +
           '}';
  }
}
