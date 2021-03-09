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
import org.apache.druid.com.google.common.annotations.VisibleForTesting;
import org.apache.druid.com.google.common.base.Preconditions;
import org.apache.druid.com.google.common.base.Supplier;
import org.apache.druid.com.google.common.base.Suppliers;
import org.apache.druid.java.util.common.logger.Logger;
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
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public class ExpressionVirtualColumn implements VirtualColumn
{
  private static final Logger log = new Logger(ExpressionVirtualColumn.class);

  private final String name;
  private final String expression;
  @Nullable
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
    this.outputType = outputType;
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
    this.outputType = outputType;
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

  @Nullable
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
    final ExpressionPlan plan = ExpressionPlanner.plan(inspector, parsedExpression.get());
    return plan.is(ExpressionPlan.Trait.VECTORIZABLE);
  }

  @Override
  public SingleValueDimensionVectorSelector makeSingleValueVectorDimensionSelector(
      DimensionSpec dimensionSpec,
      VectorColumnSelectorFactory factory
  )
  {
    return ExpressionVectorSelectors.makeSingleValueDimensionVectorSelector(factory, parsedExpression.get());
  }

  @Override
  public VectorValueSelector makeVectorValueSelector(String columnName, VectorColumnSelectorFactory factory)
  {
    return ExpressionVectorSelectors.makeVectorValueSelector(factory, parsedExpression.get());
  }

  @Override
  public VectorObjectSelector makeVectorObjectSelector(String columnName, VectorColumnSelectorFactory factory)
  {
    return ExpressionVectorSelectors.makeVectorObjectSelector(factory, parsedExpression.get());
  }

  @Override
  public ColumnCapabilities capabilities(String columnName)
  {
    // If possible, this should only be used as a fallback method for when capabilities are truly 'unknown', because we
    // are unable to compute the output type of the expression, either due to incomplete type information of the
    // inputs or because of unimplemented methods on expression implementations themselves, or, because a
    // ColumnInspector is not available

    // array types must not currently escape from the expression system
    if (outputType != null && outputType.isArray()) {
      return new ColumnCapabilitiesImpl().setType(ValueType.STRING).setHasMultipleValues(true);
    }

    return new ColumnCapabilitiesImpl().setType(outputType == null ? ValueType.FLOAT : outputType);
  }

  @Override
  public ColumnCapabilities capabilities(ColumnInspector inspector, String columnName)
  {
    final ExpressionPlan plan = ExpressionPlanner.plan(inspector, parsedExpression.get());

    if (plan.getOutputType() != null) {

      final ExprType inferredOutputType = plan.getOutputType();
      if (outputType != null && ExprType.fromValueType(outputType) != inferredOutputType) {
        log.warn(
            "Projected output type %s of expression %s does not match provided type %s",
            plan.getOutputType(),
            expression,
            outputType
        );
      }
      final ValueType valueType = ExprType.toValueType(inferredOutputType);

      if (valueType.isNumeric()) {
        // if float was explicitly specified preserve it, because it will currently never be the computed output type
        if (ValueType.FLOAT == outputType) {
          return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ValueType.FLOAT);
        }
        return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(valueType);
      }

      // null constants can sometimes trip up the type inference to report STRING, so check if explicitly supplied
      // output type is numeric and stick with that if so
      if (outputType != null && outputType.isNumeric()) {
        return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(outputType);
      }

      // array types shouldn't escape the expression system currently, so coerce anything past this point into some
      // style of string

      // we don't have to check for unknown input here because output type is unable to be inferred if we don't know
      // the complete set of input types
      if (plan.any(ExpressionPlan.Trait.NON_SCALAR_OUTPUT, ExpressionPlan.Trait.NEEDS_APPLIED)) {
        // always a multi-value string since wider engine does not yet support array types
        return new ColumnCapabilitiesImpl().setType(ValueType.STRING).setHasMultipleValues(true);
      }

      // constant strings are supported as dimension selectors, set them as dictionary encoded and unique
      if (plan.isConstant()) {
        return new ColumnCapabilitiesImpl().setType(ValueType.STRING)
                                           .setDictionaryEncoded(true)
                                           .setDictionaryValuesUnique(true)
                                           .setDictionaryValuesSorted(true)
                                           .setHasMultipleValues(false);
      }

      // if we got here, lets call it single value string output, non-dictionary encoded
      return new ColumnCapabilitiesImpl().setType(ValueType.STRING)
                                         .setHasMultipleValues(false)
                                         .setDictionaryEncoded(false);
    }
    // fallback to
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
    CacheKeyBuilder builder = new CacheKeyBuilder(VirtualColumnCacheHelper.CACHE_TYPE_ID_EXPRESSION)
        .appendString(name)
        .appendString(expression);

    if (outputType != null) {
      builder.appendString(outputType.toString());
    }
    return builder.build();
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
