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

package io.druid.segment.virtual;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprMacroTable;
import io.druid.math.expr.Parser;
import io.druid.query.cache.CacheKeyBuilder;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.VirtualColumn;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ValueType;

import java.util.List;
import java.util.Objects;

public class ExpressionVirtualColumn implements VirtualColumn
{
  private final String name;
  private final String expression;
  private final ValueType outputType;
  private final Expr parsedExpression;

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
    this.parsedExpression = Parser.parse(expression, macroTable);
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
  public ObjectColumnSelector makeObjectColumnSelector(
      final String columnName,
      final ColumnSelectorFactory columnSelectorFactory
  )
  {
    final ExpressionObjectSelector baseSelector = ExpressionSelectors.makeObjectColumnSelector(
        columnSelectorFactory,
        parsedExpression
    );
    return new ObjectColumnSelector()
    {
      @Override
      public Class classOfObject()
      {
        return Object.class;
      }

      @Override
      public Object get()
      {
        return baseSelector.get().value();
      }
    };
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
            parsedExpression,
            dimensionSpec.getExtractionFn()
        )
    );
  }

  @Override
  public FloatColumnSelector makeFloatColumnSelector(
      final String columnName,
      final ColumnSelectorFactory columnSelectorFactory
  )
  {
    return ExpressionSelectors.makeFloatColumnSelector(columnSelectorFactory, parsedExpression, 0.0f);
  }

  @Override
  public LongColumnSelector makeLongColumnSelector(
      final String columnName,
      final ColumnSelectorFactory columnSelectorFactory
  )
  {
    return ExpressionSelectors.makeLongColumnSelector(columnSelectorFactory, parsedExpression, 0L);
  }

  @Override
  public ColumnCapabilities capabilities(String columnName)
  {
    return new ColumnCapabilitiesImpl().setType(outputType);
  }

  @Override
  public List<String> requiredColumns()
  {
    return Parser.findRequiredBindings(parsedExpression);
  }

  @Override
  public boolean usesDotNotation()
  {
    return false;
  }

  @Override
  public DoubleColumnSelector makeDoubleColumnSelector(
      String columnName, ColumnSelectorFactory factory
  )
  {
    return ExpressionSelectors.makeDoubleColumnSelector(factory, parsedExpression, 0.0d);
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
