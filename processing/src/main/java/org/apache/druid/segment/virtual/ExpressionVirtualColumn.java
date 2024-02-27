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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
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
  private final ColumnType outputType;
  private final Supplier<Expr> parsedExpression;
  private final Supplier<byte[]> cacheKey;

  /**
   * Constructor for deserialization.
   */
  @JsonCreator
  public ExpressionVirtualColumn(
      @JsonProperty("name") String name,
      @JsonProperty("expression") String expression,
      @JsonProperty("outputType") @Nullable ColumnType outputType,
      @JacksonInject ExprMacroTable macroTable
  )
  {
    this(name, expression, outputType, Parser.lazyParse(expression, macroTable));
  }

  /**
   * Constructor for creating an ExpressionVirtualColumn from a pre-parsed-and-analyzed expression, where the original
   * expression string is known.
   */
  public ExpressionVirtualColumn(
      final String name,
      final String expression,
      final Expr parsedExpression,
      @Nullable final ColumnType outputType
  )
  {
    this(name, expression, outputType, () -> parsedExpression);
  }

  /**
   * Constructor for creating an ExpressionVirtualColumn from a pre-parsed expression, where the original
   * expression string is not known.
   *
   * This constructor leads to an instance where {@link #getExpression()} is the toString representation of the
   * parsed expression, which is not necessarily a valid expression. Do not try to reparse it as an expression, as
   * this will not work.
   *
   * If you know the original expression, use
   * {@link ExpressionVirtualColumn#ExpressionVirtualColumn(String, String, Expr, ColumnType)} instead.
   */
  public ExpressionVirtualColumn(
      final String name,
      final Expr parsedExpression,
      @Nullable final ColumnType outputType
  )
  {
    this(name, parsedExpression.toString(), outputType, () -> parsedExpression);
  }

  /**
   * Private constructor used by the public ones.
   */
  private ExpressionVirtualColumn(
      final String name,
      final String expression,
      @Nullable final ColumnType outputType,
      final Supplier<Expr> parsedExpression
  )
  {
    this.name = Preconditions.checkNotNull(name, "name");
    this.expression = Preconditions.checkNotNull(expression, "expression");
    this.outputType = outputType;
    this.parsedExpression = parsedExpression;
    this.cacheKey = makeCacheKeySupplier();
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
  public ColumnType getOutputType()
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
    if (isDirectAccess(columnSelectorFactory)) {
      return columnSelectorFactory.makeDimensionSelector(
          dimensionSpec.withDimension(parsedExpression.get().getBindingIfIdentifier())
      );
    }

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
    if (isDirectAccess(factory)) {
      return factory.makeColumnValueSelector(parsedExpression.get().getBindingIfIdentifier());
    }

    final ColumnCapabilities capabilities = capabilities(factory, name);
    // we make a special column value selector for values that are expected to be STRING to conform to behavior of
    // other single and multi-value STRING selectors, whose getObject is expected to produce a single STRING value
    // or List of STRING values.
    if (capabilities.is(ValueType.STRING)) {
      return ExpressionSelectors.makeStringColumnValueSelector(factory, parsedExpression.get());
    }
    return ExpressionSelectors.makeColumnValueSelector(factory, parsedExpression.get());
  }

  @Override
  public boolean canVectorize(ColumnInspector inspector)
  {
    if (isDirectAccess(inspector)) {
      // Can vectorize if the underlying adapter can vectorize.
      return true;
    }

    final ExpressionPlan plan = ExpressionPlanner.plan(inspector, parsedExpression.get());
    return plan.is(ExpressionPlan.Trait.VECTORIZABLE);
  }

  @Override
  public SingleValueDimensionVectorSelector makeSingleValueVectorDimensionSelector(
      DimensionSpec dimensionSpec,
      VectorColumnSelectorFactory factory
  )
  {
    if (isDirectAccess(factory)) {
      return factory.makeSingleValueDimensionSelector(
          dimensionSpec.withDimension(parsedExpression.get().getBindingIfIdentifier())
      );
    }

    return ExpressionVectorSelectors.makeSingleValueDimensionVectorSelector(factory, parsedExpression.get());
  }

  @Override
  public VectorValueSelector makeVectorValueSelector(String columnName, VectorColumnSelectorFactory factory)
  {
    if (isDirectAccess(factory)) {
      return factory.makeValueSelector(parsedExpression.get().getBindingIfIdentifier());
    }

    return ExpressionVectorSelectors.makeVectorValueSelector(factory, parsedExpression.get());
  }

  @Override
  public VectorObjectSelector makeVectorObjectSelector(String columnName, VectorColumnSelectorFactory factory)
  {
    if (isDirectAccess(factory)) {
      return factory.makeObjectSelector(parsedExpression.get().getBindingIfIdentifier());
    }

    return ExpressionVectorSelectors.makeVectorObjectSelector(factory, parsedExpression.get());
  }

  @Nullable
  @Override
  public ColumnIndexSupplier getIndexSupplier(
      String columnName,
      ColumnIndexSelector columnIndexSelector
  )
  {
    return getParsedExpression().get().asColumnIndexSupplier(columnIndexSelector, outputType);
  }

  @Override
  public ColumnCapabilities capabilities(String columnName)
  {
    // If possible, this should only be used as a fallback method for when capabilities are truly 'unknown', because we
    // are unable to compute the output type of the expression, either due to incomplete type information of the
    // inputs or because of unimplemented methods on expression implementations themselves, or, because a
    // ColumnInspector is not available

    if (ExpressionProcessing.processArraysAsMultiValueStrings() && outputType != null && outputType.isArray()) {
      return new ColumnCapabilitiesImpl().setType(ColumnType.STRING).setHasMultipleValues(true);
    }

    return new ColumnCapabilitiesImpl().setType(outputType == null ? ColumnType.FLOAT : outputType);
  }

  @Nullable
  @Override
  public ColumnCapabilities capabilities(ColumnInspector inspector, String columnName)
  {
    if (isDirectAccess(inspector)) {
      return inspector.getColumnCapabilities(parsedExpression.get().getBindingIfIdentifier());
    }

    final ExpressionPlan plan = ExpressionPlanner.plan(inspector, parsedExpression.get());
    final ColumnCapabilities inferred = plan.inferColumnCapabilities(outputType);
    // if we can infer the column capabilities from the expression plan, then use that
    if (inferred != null) {
      // explicit outputType is used as a hint, how did it compare to the planners inferred output type?
      if (outputType != null && inferred.getType() != outputType.getType()) {
        // if both sides are numeric, let it slide and log at debug level
        // but mismatches involving strings and arrays might be worth knowing about so warn
        if (!inferred.isNumeric() && !outputType.isNumeric()) {
          log.warn(
              "Projected output type %s of expression %s does not match provided type %s",
              inferred.asTypeString(),
              expression,
              outputType
          );
        } else {
          log.debug(
              "Projected output type %s of expression %s does not match provided type %s",
              inferred.asTypeString(),
              expression,
              outputType
          );
        }
      }
      return inferred;
    }

    // fallback to default capabilities
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
    return cacheKey.get();
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
           Objects.equals(outputType, that.outputType);
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

  /**
   * Whether this expression is an identifier that directly accesses an underlying column. In this case we skip
   * the expression system entirely, and directly return backing columns.
   */
  private boolean isDirectAccess(final ColumnInspector inspector)
  {
    if (parsedExpression.get().isIdentifier()) {
      final ColumnCapabilities baseCapabilities =
          inspector.getColumnCapabilities(parsedExpression.get().getBindingIfIdentifier());

      if (outputType == null) {
        // No desired output type. Anything from the source is fine.
        return true;
      } else if (baseCapabilities != null && outputType.equals(baseCapabilities.toColumnType())) {
        // Desired output type matches the type from the source.
        return true;
      }
    }

    return false;
  }

  private Supplier<byte[]> makeCacheKeySupplier()
  {
    return Suppliers.memoize(() -> {
      CacheKeyBuilder builder = new CacheKeyBuilder(VirtualColumnCacheHelper.CACHE_TYPE_ID_EXPRESSION)
          .appendString(name)
          .appendCacheable(parsedExpression.get());

      if (outputType != null) {
        builder.appendString(outputType.toString());
      }
      return builder.build();
    });
  }
}
