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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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
      @JsonProperty("outputType") @Nullable ValueType outputType,
      @JacksonInject ExprMacroTable macroTable
  )
  {
    this.name = Preconditions.checkNotNull(name, "name");
    this.expression = Preconditions.checkNotNull(expression, "expression");
    this.outputType = outputType != null ? outputType : ValueType.FLOAT;
    this.parsedExpression = Suppliers.memoize(() -> Parser.parse(expression, macroTable));
  }

  /**
   * Constructor for creating an ExpressionVirtualColumn from a pre-parsed expression.
   */
  public ExpressionVirtualColumn(
      String name,
      Expr parsedExpression,
      ValueType outputType
  )
  {
    this.name = Preconditions.checkNotNull(name, "name");
    // Unfortunately this string representation can't be reparsed into the same expression, might be useful
    // if the expression system supported that
    this.expression = parsedExpression.toString();
    this.outputType = outputType != null ? outputType : ValueType.FLOAT;
    this.parsedExpression = Suppliers.ofInstance(parsedExpression);
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

  @JsonIgnore
  @VisibleForTesting
  public Supplier<Expr> getParsedExpression()
  {
    return parsedExpression;
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
  public boolean canVectorize(ColumnInspector inspector)
  {
    Expr expr = parsedExpression.get();
    Expr.BindingAnalysis analysis = expr.analyzeInputs();
    // we don't currently support multi-value inputs or outputs for vectorized expressions
    return !analysis.hasInputArrays() &&
           !analysis.isOutputArray() &&
           analysis.getRequiredBindingsList().stream().noneMatch(column -> {
             ColumnCapabilities capabilities = inspector.getColumnCapabilities(column);
             return capabilities == null || capabilities.hasMultipleValues().isMaybeTrue();
           }) &&
           parsedExpression.get().canVectorize(inspector);
  }

  @Override
  public VectorValueSelector makeVectorValueSelector(String columnName, VectorColumnSelectorFactory factory)
  {
    return ExpressionSelectors.makeVectorValueSelector(factory, parsedExpression.get());
  }

  @Override
  public VectorObjectSelector makeVectorObjectSelector(String columnName, VectorColumnSelectorFactory factory)
  {
    return ExpressionSelectors.makeVectorObjectSelector(factory, parsedExpression.get());
  }

  @Override
  public ColumnCapabilities capabilities(String columnName)
  {
    // Note: Ideally we would fill out additional information instead of leaving capabilities as 'unknown', e.g. examine
    // if the expression in question could potentially return multiple values and anything else. However, we don't
    // currently have a good way of determining this, so fill this out more once we do
    return new ColumnCapabilitiesImpl().setType(outputType);
  }

  @Override
  public ColumnCapabilities capabilities(ColumnInspector inspector, String columnName)
  {
    final ExprType outputType = parsedExpression.get().getOutputType(inspector);

    if (outputType != null) {
      final ValueType valueType = ExprType.toValueType(outputType);
      if (valueType.isNumeric()) {
        // numbers are easy
        return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ExprType.toValueType(outputType));
      }
      if (valueType.isArray()) {
        // always a multi-value string since wider engine does not yet support array types
        return ColumnCapabilitiesImpl.createSimpleArrayColumnCapabilities(ValueType.STRING);
      }

      final Expr.BindingAnalysis analysis = parsedExpression.get().analyzeInputs();
      if (analysis.isOutputArray()) {
        // always a multi-value string since wider engine does not yet support array types
        return ColumnCapabilitiesImpl.createSimpleArrayColumnCapabilities(ValueType.STRING);
      }

      // dupe of some logic in ExpressionSelectors to detect if any possiblility of this producing multi value outputs
      final Pair<Set<String>, Set<String>> arrayUsage = ExpressionSelectors.findArrayInputBindings(inspector, analysis);
      final Set<String> actualArrays = arrayUsage.lhs;
      final Set<String> unknownIfArrays = arrayUsage.rhs;
      final long needsAppliedCount =
          analysis.getRequiredBindings()
                  .stream()
                  .filter(c -> actualArrays.contains(c) && !analysis.getArrayBindings().contains(c))
                  .count();

      // unapplied multi-valued inputs result in multi-valued outputs, and unknowns are unknown
      if (unknownIfArrays.size() > 0 || needsAppliedCount > 0) {
        // always a multi-value string since wider engine does not yet support array types
        return ColumnCapabilitiesImpl.createSimpleArrayColumnCapabilities(ValueType.STRING);
      }
      // if we got here, lets call it single value string output
      return new ColumnCapabilitiesImpl().setType(ValueType.STRING)
                                         .setHasMultipleValues(false)
                                         .setDictionaryEncoded(false);
    }
    return capabilities(columnName);
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
